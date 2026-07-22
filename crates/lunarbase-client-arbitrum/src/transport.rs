//! Arbitrum Nitro transport built on standard logs and execution-aware heads.

use futures_util::{StreamExt, TryStreamExt, stream};
use lunarbase_client_core::bootstrap::BootstrapSnapshot;
use lunarbase_client_core::model::{
    BackfillRequest, ChainCursor, Checkpoint, Commitment, ContractFilter, ContractLog,
    DeploymentConfig, Network, SourceError,
};
use lunarbase_client_core::source::{ChainDataSource, SourceStream};
use lunarbase_client_core::transport::rpc::client::RpcHttpClient;
use lunarbase_client_core::transport::ws::{WsRpcBackend, WsRpcConfig};
use std::collections::{BTreeSet, HashMap};

const EXECUTION_CONTEXT_CONCURRENCY: usize = 16;

#[derive(Clone)]
/// Generic Nitro `logs + newHeads` source with explicit execution context.
pub struct ArbitrumNitroSource {
    /// Common HTTP/WebSocket source configured for the Arbitrum network family.
    inner: WsRpcBackend,
    /// Canonical RPC used to resolve Nitro execution context for backfilled logs.
    rpc: RpcHttpClient,
    /// EIP-155 chain id attached to resolved block cursors.
    chain_id: u64,
}

impl ArbitrumNitroSource {
    /// Creates a bounded source using canonical finalized snapshots.
    pub fn new(rpc: RpcHttpClient, ws_url: impl Into<String>, chain_id: u64) -> Self {
        Self {
            inner: WsRpcBackend::with_config(
                rpc.clone(),
                ws_url,
                Network::Arbitrum,
                chain_id,
                "latest",
                WsRpcConfig::default(),
            ),
            rpc,
            chain_id,
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
        let mut logs = self.inner.backfill(request).await?;
        let blocks = logs
            .iter()
            .map(|log| log.cursor.block_number)
            .collect::<BTreeSet<_>>();
        let contexts = stream::iter(blocks)
            .map(|block_number| async move {
                let tag = format!("0x{block_number:x}");
                let cursor = self
                    .rpc
                    .block_cursor(&tag, self.chain_id, Commitment::Canonical)
                    .await
                    .map_err(SourceError::from)?;
                Ok::<_, SourceError>((block_number, cursor.execution_block_number))
            })
            .buffer_unordered(EXECUTION_CONTEXT_CONCURRENCY)
            .try_collect::<HashMap<_, _>>()
            .await?;
        for log in &mut logs {
            log.cursor.execution_block_number = contexts
                .get(&log.cursor.block_number)
                .copied()
                .ok_or_else(|| {
                    SourceError::Unavailable(format!(
                        "missing execution context for Arbitrum block {}",
                        log.cursor.block_number
                    ))
                })?;
        }
        Ok(logs)
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
