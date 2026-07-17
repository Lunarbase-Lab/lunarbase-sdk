//! Base Flashblocks client using official `pendingLogs + newHeads`.

mod transport;

pub use transport::{BaseFlashblocksConfig, BaseFlashblocksSource};

use lunarbase_client_core::{
    Checkpoint, ClientConnectConfig, ConnectedQuoteClient, IndexerError, RpcHttpClient,
};
use std::sync::Arc;

/// Connects a ready-to-use Base client from the common runtime config.
pub async fn connect_base(
    config: ClientConnectConfig,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, IndexerError> {
    let source = Arc::new(BaseFlashblocksSource::new(
        RpcHttpClient::new(config.deployment.http_rpc_url.clone()),
        config.deployment.realtime_source.clone(),
        config.deployment.chain_id,
    ));
    ConnectedQuoteClient::connect(config, source, checkpoint).await
}
