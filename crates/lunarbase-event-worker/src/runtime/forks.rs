//! Fork-aware recovery state kept out of the normal log append fast path.

use super::{
    RuntimeError, Transition, persist, validate_cursor, validate_recovery_log, wait_for_shutdown,
};
use crate::{
    config::Config,
    event::ReorgCorrection,
    metrics::Metrics,
    redis_store::{CorrectionLimits, JournalWindow, RedisEventStore, StoreError},
};
use lunarbase_client::{
    model::{BackfillRequest, BlockRef, Commitment, ContractFilter, ContractLog},
    source::ChainDataSource,
};
use lunarbase_source_evm::fork::{
    CanonicalWindow, ForkError, ForkResolution, ForkResolver, ForkWindowLimits,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::watch;

pub(super) struct ForkRuntime {
    resolver: ForkResolver,
    window: CanonicalWindow,
    finalized: Option<BlockRef>,
    loaded: bool,
}

impl ForkRuntime {
    pub(super) fn new(resolver: ForkResolver, config: &Config) -> Result<Self, ForkError> {
        Ok(Self {
            resolver,
            window: CanonicalWindow::new(ForkWindowLimits {
                max_blocks: config.fork_window_blocks,
                max_bytes: config.fork_window_bytes,
            })?,
            finalized: None,
            loaded: false,
        })
    }

    pub(super) async fn reconcile<S: ChainDataSource>(
        &mut self,
        source: &S,
        target: Option<BlockRef>,
        config: &Config,
        filter: &ContractFilter,
        store: &RedisEventStore,
        metrics: &Metrics,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Transition, RuntimeError> {
        if config.minimum_commitment == Commitment::Finalized {
            return if target.is_some() {
                Err(ForkError::FinalizedConflict.into())
            } else {
                Ok(Transition::Continue)
            };
        }
        self.ensure_loaded(config, store).await?;
        let finalized = self.resolver.finalized_tip().await?;
        let desired = match target {
            Some(target) => {
                validate_cursor(&target.cursor, config.chain_id)?;
                let hash = target.cursor.block_hash.ok_or_else(|| {
                    ForkError::InvalidIdentity("replacement tip hash is absent".into())
                })?;
                self.resolver
                    .block_ref_by_hash(hash, target.cursor.commitment)
                    .await?
            }
            None => self.resolver.canonical_tip().await?,
        };
        validate_cursor(&desired.cursor, config.chain_id)?;
        validate_cursor(&finalized.cursor, config.chain_id)?;

        if self.window.is_empty() {
            self.seed(desired, finalized, config, store, metrics, shutdown)
                .await?;
            return Ok(if *shutdown.borrow() {
                Transition::Shutdown
            } else {
                Transition::Continue
            });
        }

        let tip = self.window.tip().expect("non-empty checked above");
        if same_block(tip, &desired) {
            if tip != &desired {
                self.window.replace_progressive_tip(desired)?;
            }
            self.advance_finalized(finalized, config, store, metrics, shutdown)
                .await?;
            return Ok(if *shutdown.borrow() {
                Transition::Shutdown
            } else {
                Transition::Continue
            });
        }

        let resolution = self.resolver.resolve(&self.window, desired).await?;
        let finalize_after = self
            .prepare_finalized(&resolution, &finalized, config, store, metrics, shutdown)
            .await?;
        if *shutdown.borrow() {
            return Ok(Transition::Shutdown);
        }
        if resolution.old_branch.is_empty() {
            for block in &resolution.new_branch {
                if persist::head(block.clone(), config, store, metrics, shutdown).await?
                    == Transition::Shutdown
                {
                    return Ok(Transition::Shutdown);
                }
            }
            self.window.apply_resolution(&resolution)?;
        } else {
            let logs = replacement_logs(source, &resolution, config, filter).await?;
            let durable_finalized = self.finalized.clone().ok_or_else(|| {
                ForkError::InvalidIdentity(
                    "durable finalized boundary is absent before correction".into(),
                )
            })?;
            let correction = Arc::new(
                ReorgCorrection::new(&resolution, durable_finalized, logs, config.core)
                    .map_err(StoreError::from)?,
            );
            let reorg_id = correction.reorg_id.clone();
            let outcome = tokio::select! {
                biased;
                () = wait_for_shutdown(shutdown) => return Ok(Transition::Shutdown),
                result = store.correct(
                    correction,
                    CorrectionLimits {
                        max_events: config.correction_event_bound,
                        max_bytes: config.correction_byte_bound,
                    },
                ) => result?,
            };
            if outcome.reverted.saturating_add(outcome.applied) > config.correction_event_bound {
                return Err(RuntimeError::Fork(ForkError::BlockBudget));
            }
            metrics.reorg_corrected(outcome.reverted, outcome.applied, !outcome.appended);
            tracing::info!(
                reorg_id,
                reverted = outcome.reverted,
                applied = outcome.applied,
                duplicate = !outcome.appended,
                stream_id = outcome.stream_id,
                "durable fork correction committed"
            );
            self.window.apply_resolution(&resolution)?;
        }
        if finalize_after {
            self.advance_finalized(finalized, config, store, metrics, shutdown)
                .await?;
        }
        Ok(if *shutdown.borrow() {
            Transition::Shutdown
        } else {
            Transition::Continue
        })
    }

    pub(super) async fn observe_head(
        &mut self,
        head: BlockRef,
        config: &Config,
        store: &RedisEventStore,
        metrics: &Metrics,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Transition, RuntimeError> {
        validate_cursor(&head.cursor, config.chain_id)?;
        if config.minimum_commitment == Commitment::Finalized {
            return persist::head(head, config, store, metrics, shutdown).await;
        }
        if !self.loaded || self.window.is_empty() {
            return Ok(Transition::Recover(Some(head)));
        }
        if head.cursor.commitment == Commitment::Finalized {
            self.advance_finalized(head, config, store, metrics, shutdown)
                .await?;
            return Ok(Transition::Continue);
        }
        let tip = self.window.tip().expect("non-empty checked above");
        if same_block(tip, &head) {
            let changed = tip != &head;
            if persist::head(head.clone(), config, store, metrics, shutdown).await?
                == Transition::Shutdown
            {
                return Ok(Transition::Shutdown);
            }
            if changed {
                self.window.replace_progressive_tip(head)?;
            }
            return Ok(Transition::Continue);
        }
        if head.cursor.block_number == tip.cursor.block_number.saturating_add(1)
            && head.parent_hash == tip.cursor.block_hash
        {
            if persist::head(head.clone(), config, store, metrics, shutdown).await?
                == Transition::Shutdown
            {
                return Ok(Transition::Shutdown);
            }
            self.window.push_head(head)?;
            return Ok(Transition::Continue);
        }
        Ok(Transition::Recover(Some(head)))
    }

    async fn ensure_loaded(
        &mut self,
        config: &Config,
        store: &RedisEventStore,
    ) -> Result<(), RuntimeError> {
        if self.loaded {
            return Ok(());
        }
        let JournalWindow { blocks, finalized } = store
            .load_window(
                config.chain_id,
                config.fork_window_blocks,
                config.fork_window_bytes,
            )
            .await?;
        for block in blocks {
            self.window.push_head(block)?;
        }
        if let Some(block) = finalized {
            if !self
                .window
                .blocks()
                .any(|retained| same_block(retained, &block))
            {
                return Err(ForkError::BlockBudget.into());
            }
            self.window.advance_finalized(block.clone())?;
            self.finalized = Some(block);
        }
        self.loaded = true;
        Ok(())
    }

    async fn seed(
        &mut self,
        desired: BlockRef,
        finalized: BlockRef,
        config: &Config,
        store: &RedisEventStore,
        metrics: &Metrics,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), RuntimeError> {
        if finalized.cursor.block_number > desired.cursor.block_number {
            return Err(ForkError::FinalizedConflict.into());
        }
        let finalized_hash = finalized
            .cursor
            .block_hash
            .ok_or_else(|| ForkError::InvalidIdentity("finalized hash is absent".into()))?;
        let mut descending = vec![desired];
        while descending
            .last()
            .expect("desired tip seeded")
            .cursor
            .block_number
            > finalized.cursor.block_number
        {
            if descending.len() >= config.fork_window_blocks
                || descending.len() >= config.fork_max_depth
            {
                return Err(ForkError::BlockBudget.into());
            }
            let child = descending.last().expect("desired tip seeded");
            let parent_hash = child
                .parent_hash
                .ok_or_else(|| ForkError::InvalidIdentity("parent hash is absent".into()))?;
            let parent = self
                .resolver
                .block_ref_by_hash(parent_hash, child.cursor.commitment)
                .await?;
            descending.push(parent);
        }
        if descending.last().and_then(|block| block.cursor.block_hash) != Some(finalized_hash) {
            return Err(ForkError::FinalizedConflict.into());
        }
        descending.reverse();
        for mut block in descending {
            if same_block(&block, &finalized) {
                block = finalized.clone();
            }
            if persist::head(block.clone(), config, store, metrics, shutdown).await?
                == Transition::Shutdown
            {
                return Ok(());
            }
            self.window.push_head(block)?;
        }
        self.window.advance_finalized(finalized.clone())?;
        self.finalized = Some(finalized);
        Ok(())
    }

    async fn prepare_finalized(
        &mut self,
        resolution: &ForkResolution,
        finalized: &BlockRef,
        config: &Config,
        store: &RedisEventStore,
        metrics: &Metrics,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<bool, RuntimeError> {
        if resolution
            .old_branch
            .iter()
            .any(|block| same_block(block, finalized))
        {
            return Err(ForkError::FinalizedConflict.into());
        }
        if self
            .window
            .blocks()
            .any(|block| same_block(block, finalized))
        {
            self.advance_finalized(finalized.clone(), config, store, metrics, shutdown)
                .await?;
            return Ok(false);
        }
        if resolution
            .new_branch
            .iter()
            .any(|block| same_block(block, finalized))
        {
            return Ok(true);
        }
        Err(ForkError::FinalizedConflict.into())
    }

    async fn advance_finalized(
        &mut self,
        finalized: BlockRef,
        config: &Config,
        store: &RedisEventStore,
        metrics: &Metrics,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), RuntimeError> {
        if self
            .finalized
            .as_ref()
            .is_some_and(|previous| same_block(previous, &finalized))
        {
            return Ok(());
        }
        if !self
            .window
            .blocks()
            .any(|block| same_block(block, &finalized))
        {
            return Err(ForkError::FinalizedConflict.into());
        }
        if let Some(previous) = &self.finalized
            && (finalized.cursor.block_number < previous.cursor.block_number
                || (finalized.cursor.block_number == previous.cursor.block_number
                    && !same_block(previous, &finalized)))
        {
            return Err(ForkError::FinalizedConflict.into());
        }
        if persist::head(finalized.clone(), config, store, metrics, shutdown).await?
            == Transition::Shutdown
        {
            return Ok(());
        }
        self.window.advance_finalized(finalized.clone())?;
        self.finalized = Some(finalized);
        Ok(())
    }
}

async fn replacement_logs<S: ChainDataSource>(
    source: &S,
    resolution: &ForkResolution,
    config: &Config,
    filter: &ContractFilter,
) -> Result<Vec<ContractLog>, RuntimeError> {
    let allowed = resolution
        .new_branch
        .iter()
        .map(|block| {
            block
                .cursor
                .block_hash
                .map(|hash| (block.cursor.block_number, hash))
                .ok_or_else(|| {
                    ForkError::InvalidIdentity("replacement block hash is absent".into())
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let from_block = resolution
        .common_ancestor
        .cursor
        .block_number
        .saturating_add(1);
    let to_block = resolution.new_tip.cursor.block_number;
    let mut logs = Vec::new();
    let mut page_start = from_block;
    while page_start <= to_block {
        let page_end = page_start
            .saturating_add(config.backfill_page_blocks.saturating_sub(1))
            .min(to_block);
        let page = source
            .backfill(BackfillRequest {
                from_block: page_start,
                to_block: page_end,
                filter: filter.clone(),
            })
            .await?;
        for log in page {
            validate_recovery_log(&log, config, page_start, page_end)?;
            if allowed.get(&log.cursor.block_number) != log.cursor.block_hash.as_ref() {
                return Err(ForkError::InvalidIdentity(
                    "replacement backfill disagrees with resolved branch".into(),
                )
                .into());
            }
            logs.push(log);
        }
        if page_end == to_block {
            break;
        }
        page_start = page_end.saturating_add(1);
    }
    logs.sort_by_key(|log| log.cursor.event_order());
    if logs
        .windows(2)
        .any(|pair| pair[0].cursor.event_order() >= pair[1].cursor.event_order())
    {
        return Err(
            ForkError::InvalidIdentity("replacement logs are not strictly ordered".into()).into(),
        );
    }
    Ok(logs)
}

fn same_block(left: &BlockRef, right: &BlockRef) -> bool {
    left.cursor.chain_id == right.cursor.chain_id
        && left.cursor.block_number == right.cursor.block_number
        && left.cursor.execution_block_number == right.cursor.execution_block_number
        && left.cursor.block_hash == right.cursor.block_hash
        && left.parent_hash == right.parent_hash
}
