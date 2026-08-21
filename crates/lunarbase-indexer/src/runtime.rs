//! Network client construction and best-effort checkpoint scheduling.

use crate::{
    checkpoint::{RedisCheckpointStore, StoreOutcome},
    config::{Config, DeliveryMode},
    metrics::Metrics,
};
use lunarbase_client::indexer::client::ConnectedQuoteClient;
use lunarbase_client::indexer::errors::IndexerError;
use lunarbase_client::model::{Checkpoint, Network};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::interval,
};

#[derive(Clone)]
/// Serializes periodic and final writes for one checkpoint key.
pub struct CheckpointCoordinator {
    store: Option<RedisCheckpointStore>,
    singleflight: Arc<Mutex<()>>,
}

impl CheckpointCoordinator {
    /// Creates a coordinator around an optional best-effort Redis store.
    pub fn new(store: Option<RedisCheckpointStore>) -> Self {
        Self {
            store,
            singleflight: Arc::new(Mutex::new(())),
        }
    }

    /// Returns whether persistence is configured for this process.
    pub fn enabled(&self) -> bool {
        self.store.is_some()
    }

    /// Snapshots and writes current ready state under the single-flight gate.
    pub async fn flush_client(&self, client: &ConnectedQuoteClient, metrics: &Metrics) {
        let _guard = self.singleflight.lock().await;
        let checkpoint = match client.checkpoint() {
            Ok(Some(checkpoint)) => checkpoint,
            Ok(None) => return,
            Err(error) => {
                metrics.checkpoint_failure();
                tracing::warn!(error = %error, "checkpoint snapshot failed");
                return;
            }
        };
        self.store_checkpoint(&checkpoint, metrics).await;
    }

    /// Writes state captured after reducer drain under the same gate.
    pub async fn flush_final(&self, checkpoint: &Checkpoint, metrics: &Metrics) {
        let _guard = self.singleflight.lock().await;
        self.store_checkpoint(checkpoint, metrics).await;
    }

    async fn store_checkpoint(&self, checkpoint: &Checkpoint, metrics: &Metrics) {
        let Some(store) = &self.store else {
            return;
        };
        match store.store(checkpoint).await {
            Ok(StoreOutcome::Stored | StoreOutcome::Unchanged) => metrics.checkpoint_success(),
            Ok(StoreOutcome::Stale) => {
                metrics.checkpoint_stale();
                tracing::warn!("stale Redis checkpoint rejected by monotonic CAS");
            }
            Err(error) => {
                metrics.checkpoint_failure();
                tracing::warn!(error = %error, "Redis checkpoint write failed");
            }
        }
    }
}

#[derive(Debug, Error)]
/// Service startup failure.
pub enum RuntimeError {
    /// The embeddable client failed deployment validation, bootstrap, or recovery.
    #[error(transparent)]
    Client(#[from] IndexerError),
    /// The binary was compiled without the source selected by configuration.
    #[cfg(any(
        not(feature = "evm"),
        not(feature = "base"),
        not(feature = "monad"),
        not(feature = "arbitrum")
    ))]
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
    let source = Arc::new(lunarbase_source_evm::ws::EvmRpcSource::with_delivery_mode(
        rpc,
        config.realtime_url.clone(),
        Network::Evm,
        config.client.deployment.chain_id,
        evm_delivery_mode(config.delivery_mode),
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
    let source = match config.delivery_mode {
        DeliveryMode::Realtime => lunarbase_source_evm::ws::EvmRpcSource::base_flashblocks(
            rpc,
            config.realtime_url.clone(),
            config.client.deployment.chain_id,
        ),
        mode => lunarbase_source_evm::ws::EvmRpcSource::with_delivery_mode(
            rpc,
            config.realtime_url.clone(),
            Network::Base,
            config.client.deployment.chain_id,
            evm_delivery_mode(mode),
        ),
    };
    let source = Arc::new(source);
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
                    queue_byte_bound: config.client.buffer_byte_capacity,
                    poll_interval: Duration::from_micros(100),
                    delivery_mode: monad_delivery_mode(config.delivery_mode),
                    emit_removed_logs: false,
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
                    delivery_mode: monad_delivery_mode(config.delivery_mode),
                    ..lunarbase_source_monad::parser::MonadParserConfig::durable_v2()
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
        lunarbase_source_arbitrum::source::ArbitrumNitroSource::from_urls_with_delivery_mode(
            config.http_rpc_url.clone(),
            config.realtime_url.clone(),
            config.client.deployment.chain_id,
            evm_delivery_mode(config.delivery_mode),
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

#[cfg(any(feature = "evm", feature = "arbitrum"))]
fn evm_delivery_mode(mode: DeliveryMode) -> lunarbase_source_evm::ws::EvmDeliveryMode {
    use lunarbase_source_evm::ws::EvmDeliveryMode;
    match mode {
        DeliveryMode::Realtime => EvmDeliveryMode::Realtime,
        DeliveryMode::BlockOrdered => EvmDeliveryMode::BlockOrdered,
        DeliveryMode::Finalized => EvmDeliveryMode::Finalized,
    }
}

#[cfg(feature = "monad")]
fn monad_delivery_mode(mode: DeliveryMode) -> lunarbase_source_monad::execution::MonadDeliveryMode {
    use lunarbase_source_monad::execution::MonadDeliveryMode;
    match mode {
        DeliveryMode::Realtime => MonadDeliveryMode::Realtime,
        DeliveryMode::BlockOrdered => MonadDeliveryMode::BlockOrdered,
        DeliveryMode::Finalized => MonadDeliveryMode::Finalized,
    }
}

#[cfg(all(test, feature = "evm"))]
mod delivery_mode_tests {
    use super::{DeliveryMode, evm_delivery_mode};
    use lunarbase_source_evm::ws::EvmDeliveryMode;

    #[test]
    fn maps_quote_delivery_modes_to_evm_source_modes() {
        assert_eq!(
            evm_delivery_mode(DeliveryMode::Realtime),
            EvmDeliveryMode::Realtime
        );
        assert_eq!(
            evm_delivery_mode(DeliveryMode::BlockOrdered),
            EvmDeliveryMode::BlockOrdered
        );
        assert_eq!(
            evm_delivery_mode(DeliveryMode::Finalized),
            EvmDeliveryMode::Finalized
        );
    }
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
    coordinator: CheckpointCoordinator,
    every: Duration,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if !coordinator.enabled() {
            return;
        }
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
                    coordinator.flush_client(&client, &metrics).await;
                }
            }
        }
    })
}
