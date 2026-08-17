//! Arbitrum Nitro transport built on standard logs and execution-aware heads.

use alloy_primitives::B256;
use futures_util::{StreamExt, TryStreamExt, stream};
use lunarbase_client::bootstrap::BootstrapSnapshot;
use lunarbase_client::model::{
    BackfillRequest, ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractFilter, ContractLog,
    DeploymentConfig, Network, SourceError,
};
use lunarbase_client::source::{ChainDataSource, SourceStream};
use lunarbase_source_evm::rpc::client::RpcHttpClient;
use lunarbase_source_evm::ws::{EvmDeliveryMode, EvmRpcSource};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
};

const EXECUTION_CONTEXT_CONCURRENCY: usize = 16;
const EXECUTION_CONTEXT_CACHE_CAPACITY: usize = 4_096;

#[derive(Default)]
struct ExecutionContextCache {
    values: HashMap<(u64, B256), u64>,
    order: VecDeque<(u64, B256)>,
}

impl ExecutionContextCache {
    fn get(&self, key: &(u64, B256)) -> Option<u64> {
        self.values.get(key).copied()
    }

    fn insert(&mut self, key: (u64, B256), execution_block: u64) -> Result<(), SourceError> {
        if let Some(previous) = self.values.get(&key) {
            return if *previous == execution_block {
                Ok(())
            } else {
                Err(SourceError::Unavailable(
                    "conflicting cached Arbitrum execution context".into(),
                ))
            };
        }
        self.values.insert(key, execution_block);
        self.order.push_back(key);
        if self.order.len() > EXECUTION_CONTEXT_CACHE_CAPACITY
            && let Some(expired) = self.order.pop_front()
        {
            self.values.remove(&expired);
        }
        Ok(())
    }
}

#[derive(Clone)]
/// Generic Nitro `logs + newHeads` source with explicit execution context.
pub struct ArbitrumNitroSource {
    /// Common HTTP/WebSocket source configured for the Arbitrum network family.
    inner: EvmRpcSource,
    /// Canonical RPC used to resolve Nitro execution context.
    rpc: RpcHttpClient,
    /// EIP-155 chain id attached to resolved block cursors.
    chain_id: u64,
    /// Live delivery policy used to avoid enrichment work in the ordered hot path.
    delivery_mode: EvmDeliveryMode,
    /// Bounded exact-branch cache shared by snapshots, backfills, and live delivery.
    contexts: Arc<Mutex<ExecutionContextCache>>,
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

    /// Creates a source with an explicit delivery/confidence policy.
    pub fn from_urls_with_delivery_mode(
        rpc_url: impl Into<String>,
        ws_url: impl Into<String>,
        chain_id: u64,
        delivery_mode: EvmDeliveryMode,
    ) -> Result<Self, SourceError> {
        let rpc = RpcHttpClient::new(rpc_url).map_err(SourceError::from)?;
        Ok(Self::new_with_delivery_mode(
            rpc,
            ws_url,
            chain_id,
            delivery_mode,
        ))
    }

    /// Creates a bounded source using coherent latest-state snapshots.
    pub fn new(rpc: RpcHttpClient, ws_url: impl Into<String>, chain_id: u64) -> Self {
        Self::new_with_delivery_mode(rpc, ws_url, chain_id, EvmDeliveryMode::BlockOrdered)
    }

    /// Creates a bounded source with an explicit delivery/confidence policy.
    pub fn new_with_delivery_mode(
        rpc: RpcHttpClient,
        ws_url: impl Into<String>,
        chain_id: u64,
        delivery_mode: EvmDeliveryMode,
    ) -> Self {
        Self {
            inner: EvmRpcSource::with_delivery_mode(
                rpc.clone(),
                ws_url,
                Network::Arbitrum,
                chain_id,
                delivery_mode,
            ),
            rpc,
            chain_id,
            delivery_mode,
            contexts: Arc::new(Mutex::new(ExecutionContextCache::default())),
        }
    }

    /// Overrides the bounded block span used by finalized live catch-up.
    pub fn with_backfill_page_blocks(mut self, blocks: u64) -> Self {
        self.inner = self.inner.with_backfill_page_blocks(blocks);
        self
    }

    async fn nitro_execution_context(
        &self,
        block_number: u64,
        expected_block_hash: B256,
        commitment: Commitment,
    ) -> Result<(u64, u64), SourceError> {
        let key = (block_number, expected_block_hash);
        if let Some(execution_block) = self.contexts().get(&key) {
            return Ok((block_number, execution_block));
        }
        let cursor = if commitment == Commitment::Realtime {
            self.rpc
                .block_cursor_by_hash_with_execution_context(
                    expected_block_hash,
                    self.chain_id,
                    commitment,
                )
                .await
        } else {
            self.rpc
                .block_cursor_with_execution_context(
                    &format!("0x{block_number:x}"),
                    self.chain_id,
                    commitment,
                )
                .await
        }
        .map_err(SourceError::from)?;
        if cursor.block_number != block_number {
            return Err(unavailable(format!(
                "Nitro context block mismatch: expected {block_number}, got {}",
                cursor.block_number
            )));
        }
        if cursor.block_hash != Some(expected_block_hash) {
            return Err(unavailable(format!(
                "Nitro context hash mismatch for block {block_number}"
            )));
        }
        self.contexts().insert(key, cursor.execution_block_number)?;
        Ok((block_number, cursor.execution_block_number))
    }

    async fn with_nitro_execution_context(
        &self,
        mut cursor: ChainCursor,
    ) -> Result<ChainCursor, SourceError> {
        let block_hash = cursor.block_hash.ok_or_else(|| {
            unavailable(format!("Nitro block {} has no hash", cursor.block_number))
        })?;
        if cursor.execution_block_number != cursor.block_number {
            self.contexts().insert(
                (cursor.block_number, block_hash),
                cursor.execution_block_number,
            )?;
            return Ok(cursor);
        }
        cursor.execution_block_number = self
            .nitro_execution_context(cursor.block_number, block_hash, cursor.commitment)
            .await?
            .1;
        Ok(cursor)
    }

    fn contexts(&self) -> std::sync::MutexGuard<'_, ExecutionContextCache> {
        self.contexts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    async fn enrich_update(&self, update: ChainUpdate) -> Result<ChainUpdate, SourceError> {
        match update {
            ChainUpdate::Head(mut head) => {
                head.cursor = self.with_nitro_execution_context(head.cursor).await?;
                Ok(ChainUpdate::Head(head))
            }
            ChainUpdate::Log(mut log) => {
                log.cursor = self.with_nitro_execution_context(log.cursor).await?;
                Ok(ChainUpdate::Log(log))
            }
            update @ (ChainUpdate::Reorg { .. } | ChainUpdate::Gap { .. }) => Ok(update),
        }
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
            if let Some(previous) = blocks.insert(block_number, block_hash)
                && previous != block_hash
            {
                return Err(unavailable(format!(
                    "Arbitrum backfill block {block_number} has conflicting hashes"
                )));
            }
        }
        let commitment = logs[0].cursor.commitment;
        let contexts = stream::iter(blocks)
            .map(|(block_number, expected_block_hash)| async move {
                self.nitro_execution_context(block_number, expected_block_hash, commitment)
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
        let stream = self.inner.subscribe(filter).await?;
        if self.delivery_mode == EvmDeliveryMode::BlockOrdered {
            return Ok(stream);
        }
        let source = self.clone();
        Ok(Box::pin(stream.then(move |update| {
            let source = source.clone();
            async move { source.enrich_update(update?).await }
        })))
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        let cursor = self.inner.canonical_head().await?;
        self.with_nitro_execution_context(cursor).await
    }

    async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        self.inner.validate_checkpoint(checkpoint).await
    }
}
