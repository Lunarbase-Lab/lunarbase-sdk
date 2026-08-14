//! Network client construction and best-effort checkpoint scheduling.

use crate::{checkpoint::RedisCheckpointStore, config::Config, metrics::Metrics};
use lunarbase_client::indexer::client::ConnectedQuoteClient;
use lunarbase_client::indexer::errors::IndexerError;
use lunarbase_client::model::{Checkpoint, Network};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle, time::interval};

#[derive(Debug, Error)]
/// Service startup failure.
pub enum RuntimeError {
    /// The embeddable client failed deployment validation, bootstrap, or recovery.
    #[error(transparent)]
    Client(#[from] IndexerError),
    /// The binary was compiled without the source selected by configuration.
    #[error("network support is not compiled: {0:?}")]
    UnsupportedNetwork(Network),
}

/// Composes the common client with the source selected by deployment identity.
pub async fn connect_client(
    config: &Config,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    match config.client.deployment.network {
        Network::Evm => connect_evm(config, checkpoint).await,
        Network::Base => connect_base(config, checkpoint).await,
        Network::Monad => connect_monad(config, checkpoint).await,
        Network::Arbitrum => connect_arbitrum(config, checkpoint).await,
    }
}

#[cfg(feature = "evm")]
async fn connect_evm(
    config: &Config,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    let rpc = lunarbase_source_evm::rpc::client::RpcHttpClient::new(config.http_rpc_url.clone())
        .map_err(lunarbase_client::model::SourceError::from)
        .map_err(IndexerError::from)?;
    let source = Arc::new(lunarbase_source_evm::ws::EvmRpcSource::new(
        rpc,
        config.realtime_url.clone(),
        Network::Evm,
        config.client.deployment.chain_id,
        "latest",
    ));
    Ok(ConnectedQuoteClient::connect(config.client.clone(), source, checkpoint).await?)
}

#[cfg(not(feature = "evm"))]
async fn connect_evm(
    _config: &Config,
    _checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::UnsupportedNetwork(Network::Evm))
}

#[cfg(feature = "base")]
async fn connect_base(
    config: &Config,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    let rpc = lunarbase_source_evm::rpc::client::RpcHttpClient::new(config.http_rpc_url.clone())
        .map_err(lunarbase_client::model::SourceError::from)
        .map_err(IndexerError::from)?;
    let source = Arc::new(lunarbase_source_evm::ws::EvmRpcSource::base_flashblocks(
        rpc,
        config.realtime_url.clone(),
        config.client.deployment.chain_id,
    ));
    Ok(ConnectedQuoteClient::connect(config.client.clone(), source, checkpoint).await?)
}

#[cfg(not(feature = "base"))]
async fn connect_base(
    _config: &Config,
    _checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::UnsupportedNetwork(Network::Base))
}

#[cfg(feature = "monad")]
async fn connect_monad(
    config: &Config,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    #[cfg(all(feature = "monad-native", target_os = "linux"))]
    {
        let source = Arc::new(
            lunarbase_source_monad_native::MonadEventRingSource::new(
                lunarbase_source_monad_native::MonadEventRingConfig {
                    event_ring_path: config.realtime_url.clone().into(),
                    core: config.client.deployment.core,
                    chain_id: config.client.deployment.chain_id,
                    queue_bound: config.client.buffer_capacity,
                    poll_interval: Duration::from_micros(100),
                },
                config.http_rpc_url.clone(),
            )
            .map_err(IndexerError::from)?,
        );
        Ok(ConnectedQuoteClient::connect(config.client.clone(), source, checkpoint).await?)
    }
    #[cfg(not(all(feature = "monad-native", target_os = "linux")))]
    {
        let source = Arc::new(
            lunarbase_source_monad::parser::MonadParserSource::new(
                lunarbase_source_monad::parser::MonadParserConfig {
                    ws_url: config.realtime_url.clone(),
                    core: config.client.deployment.core,
                    chain_id: config.client.deployment.chain_id,
                    ..Default::default()
                },
                config.http_rpc_url.clone(),
            )
            .map_err(IndexerError::from)?,
        );
        Ok(ConnectedQuoteClient::connect(config.client.clone(), source, checkpoint).await?)
    }
}

#[cfg(not(feature = "monad"))]
async fn connect_monad(
    _config: &Config,
    _checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::UnsupportedNetwork(Network::Monad))
}

#[cfg(feature = "arbitrum")]
async fn connect_arbitrum(
    config: &Config,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    let source = Arc::new(
        lunarbase_source_arbitrum::source::ArbitrumNitroSource::from_urls(
            config.http_rpc_url.clone(),
            config.realtime_url.clone(),
            config.client.deployment.chain_id,
        )
        .map_err(IndexerError::from)?,
    );
    Ok(ConnectedQuoteClient::connect(config.client.clone(), source, checkpoint).await?)
}

#[cfg(not(feature = "arbitrum"))]
async fn connect_arbitrum(
    _config: &Config,
    _checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::UnsupportedNetwork(Network::Arbitrum))
}

/// Loads Redis state without making Redis a startup dependency.
pub async fn load_checkpoint(
    store: Option<&RedisCheckpointStore>,
    metrics: &Metrics,
) -> Option<Checkpoint> {
    let store = store?;
    match store.load().await {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            metrics.checkpoint_failure();
            tracing::warn!(error = %error, "Redis checkpoint ignored");
            None
        }
    }
}

/// Starts periodic full-checkpoint replacement.
pub fn spawn_checkpoint_loop(
    client: Arc<ConnectedQuoteClient>,
    store: Option<RedisCheckpointStore>,
    every: Duration,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(store) = store else {
            return;
        };
        let mut ticks = interval(every);
        ticks.tick().await;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                _ = ticks.tick() => {
                    flush_checkpoint(&client, &store, &metrics).await;
                }
            }
        }
    })
}

/// Writes the current checkpoint, treating every failure as restart-only loss.
pub async fn flush_checkpoint(
    client: &ConnectedQuoteClient,
    store: &RedisCheckpointStore,
    metrics: &Metrics,
) {
    let checkpoint = match client.checkpoint() {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => return,
        Err(error) => {
            metrics.checkpoint_failure();
            tracing::warn!(error = %error, "checkpoint snapshot failed");
            return;
        }
    };
    match store.store(&checkpoint).await {
        Ok(()) => metrics.checkpoint_success(),
        Err(error) => {
            metrics.checkpoint_failure();
            tracing::warn!(error = %error, "Redis checkpoint write failed");
        }
    }
}
