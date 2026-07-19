//! Experimental Arbitrum Nitro client.

pub mod prelude;
pub mod transport;

use lunarbase_client_core::indexer::client::ConnectedQuoteClient;
use lunarbase_client_core::indexer::client_types::ClientConnectConfig;
use lunarbase_client_core::indexer::errors::IndexerError;
use lunarbase_client_core::model::{Checkpoint, SourceError};
use lunarbase_client_core::transport::rpc::client::RpcHttpClient;
use std::sync::Arc;

use crate::transport::ArbitrumNitroSource;

/// Connects a ready-to-use Arbitrum client.
///
/// The package remains experimental until execution `block.number` semantics
/// are validated against a live Nitro node.
pub async fn connect_arbitrum(
    config: ClientConnectConfig,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, IndexerError> {
    let rpc =
        RpcHttpClient::new(config.deployment.http_rpc_url.clone()).map_err(SourceError::from)?;
    let source = Arc::new(ArbitrumNitroSource::new(
        rpc,
        config.deployment.realtime_source.clone(),
        config.deployment.chain_id,
    ));
    ConnectedQuoteClient::connect(config, source, checkpoint).await
}
