use lunarbase_client_core::bootstrap::BootstrapSnapshot;
use lunarbase_client_core::model::{
    BackfillRequest, ChainCursor, Checkpoint, ContractFilter, ContractLog, DeploymentConfig,
    Network, SourceError,
};
use lunarbase_client_core::source::{ChainDataSource, SourceStream};
use lunarbase_client_core::transport::rpc::client::RpcHttpClient;
use lunarbase_client_core::transport::ws::{WsRpcBackend, WsRpcConfig};

#[derive(Clone)]
/// Generic Nitro `logs + newHeads` source with explicit execution context.
pub struct ArbitrumNitroSource {
    inner: WsRpcBackend,
}

impl ArbitrumNitroSource {
    /// Creates a bounded source using canonical finalized snapshots.
    pub fn new(rpc: RpcHttpClient, ws_url: impl Into<String>, chain_id: u64) -> Self {
        Self {
            inner: WsRpcBackend::with_config(
                rpc,
                ws_url,
                Network::Arbitrum,
                chain_id,
                "finalized",
                WsRpcConfig::default(),
            ),
        }
    }
}

impl ChainDataSource for ArbitrumNitroSource {
    fn network(&self) -> Network {
        Network::Arbitrum
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
