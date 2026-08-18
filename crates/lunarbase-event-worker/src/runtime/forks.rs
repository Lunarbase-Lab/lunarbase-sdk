//! Fork-aware recovery state kept out of the normal log append fast path.

#[path = "forks/replacement.rs"]
mod replacement;
#[path = "forks/upstream.rs"]
mod upstream;
use replacement::replacement_logs;

use super::{RuntimeError, Transition, persist, validate_cursor, wait_for_shutdown};
use crate::{
    config::Config,
    event::ReorgCorrection,
    metrics::Metrics,
    redis_store::{CorrectionLimits, JournalWindow, RedisEventStore, StoreError},
};
use lunarbase_client::{
    model::{BlockRef, Commitment, ContractFilter},
    source::ChainDataSource,
};
use lunarbase_source_evm::fork::{
    CanonicalWindow, ForkError, ForkResolution, ForkResolver, ForkWindowLimits,
};
use std::{mem, sync::Arc};
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
        let next_window = resolved_window_for_recovery(&self.window, &resolution, metrics)?;
        if resolution.old_branch.is_empty() {
            self.loaded = false;
            for block in &resolution.new_branch {
                if persist::head(block.clone(), config, store, metrics, shutdown).await?
                    == Transition::Shutdown
                {
                    return Ok(Transition::Shutdown);
                }
            }
            self.install_window(next_window);
            self.loaded = true;
        } else {
            let durable_finalized = self.finalized.clone().ok_or_else(|| {
                ForkError::InvalidIdentity(
                    "durable finalized boundary is absent before correction".into(),
                )
            })?;
            let empty = ReorgCorrection::new(
                &resolution,
                durable_finalized.clone(),
                Vec::new(),
                config.core,
            )
            .map_err(StoreError::from)
            .map_err(|error| correction_recovery(metrics, error))?;
            let base_retained_bytes = empty.retained_bytes();
            drop(empty);
            let logs = replacement_logs(source, &resolution, config, filter, base_retained_bytes)
                .await
                .map_err(|error| classify_correction_error(metrics, error))?;
            let correction = Arc::new(
                ReorgCorrection::new(&resolution, durable_finalized, logs, config.core)
                    .map_err(StoreError::from)
                    .map_err(|error| correction_recovery(metrics, error))?,
            );
            if correction.retained_bytes() > config.correction_byte_bound {
                return Err(correction_recovery(
                    metrics,
                    StoreError::CorrectionBudget(
                        "materialized correction exceeds its byte budget".into(),
                    ),
                ));
            }
            let reorg_id = correction.reorg_id.clone();
            self.loaded = false;
            let result = tokio::select! {
                biased;
                () = wait_for_shutdown(shutdown) => return Ok(Transition::Shutdown),
                result = store.correct(
                    correction,
                    CorrectionLimits {
                        max_events: config.correction_event_bound,
                        max_bytes: config.correction_byte_bound,
                    },
                ) => result,
            };
            let outcome = match result {
                Ok(outcome) => outcome,
                Err(error) if upstream::recoverable_store_error(&error) => {
                    return Err(correction_recovery(metrics, error));
                }
                Err(error) => return Err(error.into()),
            };
            debug_assert!(
                outcome.reverted.saturating_add(outcome.applied)
                    <= config.correction_event_bound.saturating_sub(2),
                "Redis store validates correction event count before its atomic commit"
            );
            self.install_window(next_window);
            self.loaded = true;
            metrics.reorg_corrected(outcome.reverted, outcome.applied, !outcome.appended);
            tracing::info!(
                reorg_id,
                reverted = outcome.reverted,
                applied = outcome.applied,
                duplicate = !outcome.appended,
                stream_id = outcome.stream_id,
                "durable fork correction committed"
            );
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
            let prepared = changed
                .then(|| self.window.prepare_progressive_tip(head.clone()))
                .transpose()?;
            if persist::head(head.clone(), config, store, metrics, shutdown).await?
                == Transition::Shutdown
            {
                return Ok(Transition::Shutdown);
            }
            if let Some(prepared) = prepared {
                self.window.commit_progressive_tip(prepared);
            }
            return Ok(Transition::Continue);
        }
        if head.cursor.block_number == tip.cursor.block_number.saturating_add(1)
            && head.parent_hash == tip.cursor.block_hash
        {
            let prepared = self.window.prepare_head(head.clone())?;
            if persist::head(head.clone(), config, store, metrics, shutdown).await?
                == Transition::Shutdown
            {
                return Ok(Transition::Shutdown);
            }
            self.window.commit_head(prepared);
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
        let mut candidate = CanonicalWindow::new(ForkWindowLimits {
            max_blocks: config.fork_window_blocks,
            max_bytes: config.fork_window_bytes,
        })?;
        for block in blocks {
            candidate.push_head(block)?;
        }
        if let Some(block) = &finalized {
            if !candidate
                .blocks()
                .any(|retained| same_block(retained, block))
            {
                return Err(ForkError::BlockBudget.into());
            }
            candidate.advance_finalized(block.clone())?;
        }
        self.install_window(candidate);
        self.finalized = finalized;
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
        for block in &mut descending {
            if same_block(block, &finalized) {
                *block = finalized.clone();
            }
        }
        let mut candidate = CanonicalWindow::new(ForkWindowLimits {
            max_blocks: config.fork_window_blocks,
            max_bytes: config.fork_window_bytes,
        })?;
        for block in &descending {
            candidate.push_head(block.clone())?;
        }
        candidate.advance_finalized(finalized.clone())?;
        self.loaded = false;
        for block in descending {
            if persist::head(block.clone(), config, store, metrics, shutdown).await?
                == Transition::Shutdown
            {
                return Ok(());
            }
        }
        self.install_window(candidate);
        self.finalized = Some(finalized);
        self.loaded = true;
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
        let mut candidate = self.window.clone();
        candidate.advance_finalized(finalized.clone())?;
        self.loaded = false;
        if persist::head(finalized.clone(), config, store, metrics, shutdown).await?
            == Transition::Shutdown
        {
            return Ok(());
        }
        self.install_window(candidate);
        self.finalized = Some(finalized);
        self.loaded = true;
        Ok(())
    }

    fn install_window(&mut self, candidate: CanonicalWindow) {
        let retired = mem::replace(&mut self.window, candidate);
        drop(retired);
    }
}

fn resolved_window(
    window: &CanonicalWindow,
    resolution: &ForkResolution,
) -> Result<CanonicalWindow, ForkError> {
    let mut candidate = window.clone();
    candidate.apply_resolution(resolution)?;
    Ok(candidate)
}

fn resolved_window_for_recovery(
    window: &CanonicalWindow,
    resolution: &ForkResolution,
    metrics: &Metrics,
) -> Result<CanonicalWindow, RuntimeError> {
    resolved_window(window, resolution).map_err(|error| {
        if matches!(error, ForkError::BlockBudget | ForkError::ByteBudget) {
            metrics.source_gap();
        }
        error.into()
    })
}

fn classify_correction_error(metrics: &Metrics, error: RuntimeError) -> RuntimeError {
    match error {
        RuntimeError::Store(error) if upstream::recoverable_store_error(&error) => {
            correction_recovery(metrics, error)
        }
        error => error,
    }
}

fn correction_recovery(metrics: &Metrics, error: StoreError) -> RuntimeError {
    metrics.source_gap();
    RuntimeError::RecoveryLog(format!("durable correction requires recovery: {error}"))
}

fn same_block(left: &BlockRef, right: &BlockRef) -> bool {
    left.cursor.chain_id == right.cursor.chain_id
        && left.cursor.block_number == right.cursor.block_number
        && left.cursor.execution_block_number == right.cursor.execution_block_number
        && left.cursor.block_hash == right.cursor.block_hash
        && left.parent_hash == right.parent_hash
}

#[cfg(test)]
#[path = "forks/local_tests.rs"]
mod tests;
