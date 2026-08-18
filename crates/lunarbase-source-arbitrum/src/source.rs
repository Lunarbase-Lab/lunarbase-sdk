//! Arbitrum Nitro transport built on standard logs and execution-aware heads.

use alloy_primitives::B256;
use futures_util::{StreamExt, TryStreamExt, stream};
use lunarbase_client::bootstrap::BootstrapSnapshot;
use lunarbase_client::model::{
    BackfillRequest, ChainCorrection, ChainCursor, ChainUpdate, Checkpoint, Commitment,
    ContractFilter, ContractLog, DeploymentConfig, Network, SourceError,
};
use lunarbase_client::source::{ChainDataSource, SourceStream};
use lunarbase_source_evm::fork::{ForkError, ForkResolver};
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

    /// Creates a rare-path fork resolver with Nitro execution context.
    pub fn fork_resolver(&self, max_depth: usize) -> Result<ForkResolver, ForkError> {
        self.inner.fork_resolver(max_depth)
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
            ChainUpdate::Correction(correction) => self.enrich_correction(correction).await,
            update @ (ChainUpdate::Reorg { .. } | ChainUpdate::Gap { .. }) => Ok(update),
        }
    }

    async fn enrich_correction(
        &self,
        mut correction: Box<ChainCorrection>,
    ) -> Result<ChainUpdate, SourceError> {
        let mut unique_blocks = BTreeMap::new();
        for block in std::iter::once(&correction.common_ancestor)
            .chain(std::iter::once(&correction.old_tip))
            .chain(std::iter::once(&correction.new_tip))
            .chain(correction.old_branch.iter())
            .chain(correction.new_branch.iter())
        {
            let hash = block.cursor.block_hash.ok_or_else(|| {
                unavailable(format!(
                    "Arbitrum correction block {} has no hash",
                    block.cursor.block_number
                ))
            })?;
            let key = (block.cursor.block_number, hash);
            if block.cursor.execution_block_number != block.cursor.block_number {
                self.contexts()
                    .insert(key, block.cursor.execution_block_number)?;
            }
            unique_blocks
                .entry(key)
                .and_modify(|commitment| {
                    if block.cursor.commitment == Commitment::Realtime {
                        *commitment = Commitment::Realtime;
                    }
                })
                .or_insert(block.cursor.commitment);
        }
        let contexts = stream::iter(unique_blocks)
            .map(|((block_number, block_hash), commitment)| async move {
                let (_, execution_block) = self
                    .nitro_execution_context(block_number, block_hash, commitment)
                    .await?;
                Ok::<_, SourceError>(((block_number, block_hash), execution_block))
            })
            .buffer_unordered(EXECUTION_CONTEXT_CONCURRENCY)
            .try_collect::<HashMap<_, _>>()
            .await?;
        let stamp = |cursor: &mut ChainCursor| -> Result<(), SourceError> {
            let block_hash = cursor.block_hash.ok_or_else(|| {
                unavailable(format!(
                    "Arbitrum correction cursor {} has no hash",
                    cursor.block_number
                ))
            })?;
            cursor.execution_block_number = contexts
                .get(&(cursor.block_number, block_hash))
                .copied()
                .ok_or_else(|| {
                    unavailable(format!(
                        "Arbitrum correction cursor {} is outside its branch",
                        cursor.block_number
                    ))
                })?;
            Ok(())
        };
        stamp(&mut correction.common_ancestor.cursor)?;
        stamp(&mut correction.old_tip.cursor)?;
        stamp(&mut correction.new_tip.cursor)?;
        for block in correction
            .old_branch
            .iter_mut()
            .chain(correction.new_branch.iter_mut())
        {
            stamp(&mut block.cursor)?;
        }
        for log in &mut correction.replacement_logs {
            stamp(&mut log.cursor)?;
        }
        Ok(ChainUpdate::Correction(correction))
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
        let source = self.clone();
        Ok(Box::pin(stream.then(move |update| {
            let source = source.clone();
            async move {
                let update = update?;
                if source.delivery_mode == EvmDeliveryMode::BlockOrdered
                    && !matches!(&update, ChainUpdate::Correction(_))
                {
                    Ok(update)
                } else {
                    source.enrich_update(update).await
                }
            }
        })))
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        let cursor = self.inner.canonical_head().await?;
        self.with_nitro_execution_context(cursor).await
    }

    async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        if !self.inner.validate_checkpoint(checkpoint).await? {
            return Ok(false);
        }
        let Some(block_hash) = checkpoint.cursor.block_hash else {
            return Ok(false);
        };
        let (_, execution_block) = self
            .nitro_execution_context(
                checkpoint.cursor.block_number,
                block_hash,
                checkpoint.cursor.commitment,
            )
            .await?;
        Ok(execution_block == checkpoint.cursor.execution_block_number)
    }
}

#[cfg(test)]
#[path = "source_correction_tests.rs"]
mod correction_rpc_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes};
    use lunarbase_client::model::BlockRef;

    #[tokio::test]
    async fn correction_enriches_every_nitro_execution_cursor() {
        let rpc = RpcHttpClient::new("http://127.0.0.1:1").unwrap();
        let source = ArbitrumNitroSource::new(rpc, "ws://127.0.0.1:1", 42_161);
        let ancestor_hash = B256::new([0x81; 32]);
        let old_hash = B256::new([0x82; 32]);
        let new_hash = B256::new([0x83; 32]);
        {
            let mut contexts = source.contexts();
            contexts.insert((10, ancestor_hash), 1_000).unwrap();
            contexts.insert((11, old_hash), 1_001).unwrap();
            contexts.insert((11, new_hash), 2_001).unwrap();
        }

        let ancestor = block(10, ancestor_hash, None, Commitment::Finalized);
        let old = block(11, old_hash, Some(ancestor_hash), Commitment::Realtime);
        let new = block(11, new_hash, Some(ancestor_hash), Commitment::Realtime);
        let update = ChainUpdate::Correction(Box::new(ChainCorrection {
            common_ancestor: ancestor,
            old_tip: old.clone(),
            new_tip: new.clone(),
            old_branch: vec![old],
            new_branch: vec![new],
            replacement_logs: vec![ContractLog {
                address: Address::new([1; 20]),
                transaction_hash: Some(B256::new([0x84; 32])),
                topics: Vec::new(),
                data: Bytes::new(),
                removed: false,
                cursor: event_cursor(11, new_hash),
            }],
        }));

        let ChainUpdate::Correction(correction) = source.enrich_update(update).await.unwrap()
        else {
            panic!("correction update expected");
        };
        assert_eq!(
            correction.common_ancestor.cursor.execution_block_number,
            1_000
        );
        assert_eq!(correction.old_tip.cursor.execution_block_number, 1_001);
        assert_eq!(
            correction.old_branch[0].cursor.execution_block_number,
            1_001
        );
        assert_eq!(correction.new_tip.cursor.execution_block_number, 2_001);
        assert_eq!(
            correction.new_branch[0].cursor.execution_block_number,
            2_001
        );
        assert_eq!(
            correction.replacement_logs[0].cursor.execution_block_number,
            2_001
        );
    }

    fn block(
        number: u64,
        hash: B256,
        parent_hash: Option<B256>,
        commitment: Commitment,
    ) -> BlockRef {
        BlockRef::new(
            ChainCursor::block(42_161, number, Some(hash), commitment),
            parent_hash,
        )
    }

    fn event_cursor(number: u64, hash: B256) -> ChainCursor {
        let mut cursor = ChainCursor::block(42_161, number, Some(hash), Commitment::Realtime);
        cursor.transaction_index = Some(0);
        cursor.log_index = Some(0);
        cursor
    }
}
