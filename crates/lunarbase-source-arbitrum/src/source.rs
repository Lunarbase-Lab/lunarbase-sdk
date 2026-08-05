//! Arbitrum Nitro transport built on standard logs and execution-aware heads.

use futures_util::{StreamExt, TryStreamExt, stream};
use lunarbase_client::bootstrap::BootstrapSnapshot;
use lunarbase_client::model::{
    BackfillRequest, ChainCursor, Checkpoint, Commitment, ContractFilter, ContractLog,
    DeploymentConfig, Network, SourceError,
};
use lunarbase_client::source::{ChainDataSource, SourceStream};
use lunarbase_source_evm::rpc::client::RpcHttpClient;
use lunarbase_source_evm::ws::{EvmRpcSource, WsRpcConfig};
use std::collections::{BTreeMap, HashMap};

const EXECUTION_CONTEXT_CONCURRENCY: usize = 16;

#[derive(Clone)]
/// Generic Nitro `logs + newHeads` source with explicit execution context.
pub struct ArbitrumNitroSource {
    /// Common HTTP/WebSocket source configured for the Arbitrum network family.
    inner: EvmRpcSource,
    /// Canonical RPC used to resolve Nitro execution context.
    rpc: RpcHttpClient,
    /// EIP-155 chain id attached to resolved block cursors.
    chain_id: u64,
}

impl ArbitrumNitroSource {
    /// Creates a source directly from canonical HTTP and realtime WebSocket URLs.
    pub fn from_urls(
        rpc_url: impl Into<String>,
        ws_url: impl Into<String>,
        chain_id: u64,
    ) -> Result<Self, SourceError> {
        let rpc = RpcHttpClient::new(rpc_url).map_err(SourceError::from)?;
        Ok(Self::new(rpc, ws_url, chain_id))
    }

    /// Creates a bounded source using coherent latest-state snapshots.
    pub fn new(rpc: RpcHttpClient, ws_url: impl Into<String>, chain_id: u64) -> Self {
        Self {
            inner: EvmRpcSource::with_config(
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

    async fn nitro_execution_context(
        &self,
        block_number: u64,
        expected_block_hash: &str,
        commitment: Commitment,
    ) -> Result<(u64, u64), SourceError> {
        let tag = format!("0x{block_number:x}");
        let cursor = self
            .rpc
            .block_cursor_with_execution_context(&tag, self.chain_id, commitment)
            .await
            .map_err(SourceError::from)?;
        if cursor.block_number != block_number {
            return Err(unavailable(format!(
                "Nitro context block mismatch: expected {block_number}, got {}",
                cursor.block_number
            )));
        }
        let returned_hash = cursor
            .block_hash
            .ok_or_else(|| unavailable("Nitro context block has no hash"))?;
        if !format!("{returned_hash:#x}").eq_ignore_ascii_case(expected_block_hash) {
            return Err(unavailable(format!(
                "Nitro context hash mismatch for block {block_number}"
            )));
        }
        Ok((block_number, cursor.execution_block_number))
    }

    async fn with_nitro_execution_context(
        &self,
        mut cursor: ChainCursor,
    ) -> Result<ChainCursor, SourceError> {
        let block_hash = cursor.block_hash.ok_or_else(|| {
            unavailable(format!("Nitro block {} has no hash", cursor.block_number))
        })?;
        cursor.execution_block_number = self
            .nitro_execution_context(
                cursor.block_number,
                &format!("{block_hash:#x}"),
                cursor.commitment,
            )
            .await?
            .1;
        Ok(cursor)
    }
}

fn unavailable(message: impl Into<String>) -> SourceError {
    SourceError::Unavailable(message.into())
}

impl ChainDataSource for ArbitrumNitroSource {
    fn network(&self) -> Network {
        Network::Arbitrum
    }

    async fn snapshot(
        &self,
        deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        let mut snapshot = self.inner.snapshot(deployment).await?;
        snapshot.cursor = self.with_nitro_execution_context(snapshot.cursor).await?;
        Ok(snapshot)
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        let mut logs = self.inner.backfill(request).await?;
        if logs.is_empty() {
            return Ok(logs);
        }
        let mut blocks = BTreeMap::new();
        for log in &logs {
            let block_number = log.cursor.block_number;
            let block_hash = log.cursor.block_hash.ok_or_else(|| {
                unavailable(format!(
                    "Arbitrum backfill log at block {block_number} has no block hash"
                ))
            })?;
            let block_hash = format!("{block_hash:#x}");
            if let Some(previous) = blocks.insert(block_number, block_hash.clone())
                && previous != block_hash
            {
                return Err(unavailable(format!(
                    "Arbitrum backfill block {block_number} has conflicting hashes"
                )));
            }
        }
        let contexts = stream::iter(blocks)
            .map(|(block_number, expected_block_hash)| async move {
                self.nitro_execution_context(
                    block_number,
                    &expected_block_hash,
                    Commitment::Canonical,
                )
                .await
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
        let cursor = self.inner.canonical_head().await?;
        self.with_nitro_execution_context(cursor).await
    }

    async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        self.inner.validate_checkpoint(checkpoint).await
    }
}
