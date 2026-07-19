//! Base Flashblocks client using official `pendingLogs + newHeads`.

pub mod prelude;
pub mod transport;

use lunarbase_client_core::indexer::client::ConnectedQuoteClient;
use lunarbase_client_core::indexer::client_types::ClientConnectConfig;
use lunarbase_client_core::indexer::errors::IndexerError;
use lunarbase_client_core::model::{Checkpoint, SourceError};
use lunarbase_client_core::transport::rpc::client::RpcHttpClient;
use std::sync::Arc;

use crate::transport::BaseFlashblocksSource;

/// Connects a ready-to-use Base client from the common runtime config.
pub async fn connect_base(
    config: ClientConnectConfig,
    checkpoint: Option<Checkpoint>,
) -> Result<ConnectedQuoteClient, IndexerError> {
    let rpc =
        RpcHttpClient::new(config.deployment.http_rpc_url.clone()).map_err(SourceError::from)?;
    let source = Arc::new(BaseFlashblocksSource::new(
        rpc,
        config.deployment.realtime_source.clone(),
        config.deployment.chain_id,
    ));
    ConnectedQuoteClient::connect(config, source, checkpoint).await
}
