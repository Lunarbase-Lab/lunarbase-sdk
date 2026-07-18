//! Experimental Arbitrum Nitro client.

mod transport;

pub use transport::ArbitrumNitroSource;

use lunarbase_client_core::{
    Checkpoint, ClientConnectConfig, ConnectedQuoteClient, IndexerError, RpcHttpClient, SourceError,
};
use std::sync::Arc;

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
