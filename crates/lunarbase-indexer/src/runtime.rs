//! Network client construction and best-effort checkpoint scheduling.

use crate::{checkpoint::RedisCheckpointStore, config::Config, metrics::Metrics};
use lunarbase_client_core::{Checkpoint, ConnectedQuoteClient, IndexerError, Network};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle, time::interval};

#[derive(Debug, Error)]
/// Service startup failure.
pub enum RuntimeError {
    #[error(transparent)]
    Client(#[from] IndexerError),
    #[cfg(not(all(feature = "base", feature = "monad", feature = "arbitrum")))]
    #[error("network support is not compiled: {0:?}")]
    UnsupportedNetwork(Network),
}

/// Connects the high-level network package selected by deployment identity.
pub async fn connect_client(
    config: &Config,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    match config.client.deployment.network {
        Network::Base => connect_base(config, checkpoint).await,
        Network::Monad => connect_monad(config, checkpoint).await,
        Network::Arbitrum => connect_arbitrum(config, checkpoint).await,
    }
}

#[cfg(feature = "base")]
async fn connect_base(
    config: &Config,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Ok(lunarbase_client_base::connect_base(config.client.clone(), checkpoint).await?)
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
        return Ok(lunarbase_client_monad::connect_monad_event_ring(
            config.client.clone(),
            checkpoint,
        )
        .await?);
    }
    #[cfg(not(all(feature = "monad-native", target_os = "linux")))]
    {
        Ok(lunarbase_client_monad::connect_monad_parser(config.client.clone(), checkpoint).await?)
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
    Ok(lunarbase_client_arbitrum::connect_arbitrum(config.client.clone(), checkpoint).await?)
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
