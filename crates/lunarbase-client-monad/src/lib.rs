//! Monad execution-events client.
//!
//! Parser WebSocket and native shared-memory readers live in this network
//! package; the common reducer has no Monad-specific dependency.

mod execution;
mod parser;
mod protocol;

#[cfg(all(feature = "native-event-ring", target_os = "linux"))]
mod native;

pub use execution::*;
pub use parser::*;
pub use protocol::*;

#[cfg(all(feature = "native-event-ring", target_os = "linux"))]
pub use native::*;

use lunarbase_client_core::{Checkpoint, ClientConnectConfig, ConnectedQuoteClient, IndexerError};
use std::sync::Arc;

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
