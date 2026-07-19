//! Monad execution-events client.
//!
//! Parser WebSocket and native shared-memory readers live in this network
//! package; the common reducer has no Monad-specific dependency.

pub mod execution;
pub mod parser;
pub mod prelude;
pub mod protocol;

#[cfg(all(feature = "native-event-ring", target_os = "linux"))]
pub mod native;

use lunarbase_client_core::indexer::client::ConnectedQuoteClient;
use lunarbase_client_core::indexer::client_types::ClientConnectConfig;
use lunarbase_client_core::indexer::errors::IndexerError;
use lunarbase_client_core::model::Checkpoint;
use std::sync::Arc;

use crate::parser::{MonadParserConfig, MonadParserSource};

#[cfg(all(feature = "native-event-ring", target_os = "linux"))]
use crate::native::{MonadEventRingConfig, MonadEventRingSource};

/// Connects the portable parser/RPC Monad implementation.
pub async fn connect_monad_parser(
    config: ClientConnectConfig,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, IndexerError> {
    let source = Arc::new(MonadParserSource::new(
        MonadParserConfig {
            ws_url: config.deployment.realtime_source.clone(),
            core: config.deployment.core,
            chain_id: config.deployment.chain_id,
            ..Default::default()
        },
        config.deployment.http_rpc_url.clone(),
    )?);
    ConnectedQuoteClient::connect(config, source, checkpoint).await
}

/// Connects the colocated native Monad event-ring implementation.
#[cfg(all(feature = "native-event-ring", target_os = "linux"))]
pub async fn connect_monad_event_ring(
    config: ClientConnectConfig,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, IndexerError> {
    let source = Arc::new(MonadEventRingSource::new(
        MonadEventRingConfig {
            event_ring_path: config.deployment.realtime_source.clone().into(),
            core: config.deployment.core,
            chain_id: config.deployment.chain_id,
            queue_bound: config.buffer_capacity,
            poll_interval: std::time::Duration::from_micros(100),
        },
        config.deployment.http_rpc_url.clone(),
    )?);
    ConnectedQuoteClient::connect(config, source, checkpoint).await
}
