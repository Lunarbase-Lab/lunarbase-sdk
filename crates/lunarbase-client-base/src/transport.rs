//! Base Flashblocks transport composed from official Ethereum subscriptions.

use lunarbase_client_core::bootstrap::BootstrapSnapshot;
use lunarbase_client_core::model::{
    BackfillRequest, ChainCursor, Checkpoint, ContractFilter, ContractLog, DeploymentConfig,
    Network, SourceError,
};
use lunarbase_client_core::source::{ChainDataSource, SourceStream};
use lunarbase_client_core::transport::rpc::client::RpcHttpClient;
use lunarbase_client_core::transport::ws::{WsRpcBackend, WsRpcConfig};

#[derive(Clone, Debug, Eq, PartialEq)]
/// Resource bounds for the Base preconfirmation stream.
pub struct BaseFlashblocksConfig {
    /// Maximum accepted parser frame size before the source fails closed.
    pub max_frame_bytes: usize,
    /// Maximum provisional updates buffered while awaiting a head watermark.
    pub reorder_capacity: usize,
}

impl Default for BaseFlashblocksConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: 256 * 1024,
            reorder_capacity: 4096,
        }
    }
}

#[derive(Clone)]
/// Base source backed by `pendingLogs` and Flashblocks `newHeads`.
///
/// Base documents that `newHeads` is emitted for every Flashblock, so the
/// client intentionally avoids decoding the much larger `newFlashblocks`
/// payload.
pub struct BaseFlashblocksSource {
    /// Common bounded WebSocket implementation configured for Base subscriptions.
    inner: WsRpcBackend,
}

impl BaseFlashblocksSource {
    /// Creates a source with default bounded transport settings.
    pub fn new(rpc: RpcHttpClient, ws_url: impl Into<String>, chain_id: u64) -> Self {
        Self::with_config(rpc, ws_url, chain_id, BaseFlashblocksConfig::default())
    }

    /// Creates a source with explicit frame/reorder bounds.
    pub fn with_config(
        rpc: RpcHttpClient,
        ws_url: impl Into<String>,
        chain_id: u64,
        config: BaseFlashblocksConfig,
    ) -> Self {
        Self {
            inner: WsRpcBackend::with_config(
                rpc,
                ws_url,
                Network::Base,
                chain_id,
                "finalized",
                WsRpcConfig {
                    max_frame_bytes: config.max_frame_bytes,
                    reorder_capacity: config.reorder_capacity,
                    logs_subscription: "pendingLogs".into(),
                    progressive_heads: true,
                },
            ),
        }
    }

    /// Returns the underlying generic WS source for diagnostics.
    pub fn inner(&self) -> &WsRpcBackend {
        &self.inner
    }
}

impl ChainDataSource for BaseFlashblocksSource {
    fn network(&self) -> Network {
        Network::Base
    }

    async fn snapshot(
        &self,
        deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        self.inner.snapshot(deployment).await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.inner.backfill(request).await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        self.inner.subscribe(filter).await
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        self.inner.canonical_head().await
    }

    async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        self.inner.validate_checkpoint(checkpoint).await
    }
}
