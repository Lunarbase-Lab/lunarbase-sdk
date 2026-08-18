//! Bounded canonical-range validation, independent of the optional observer.

use super::CoreEventSink;
use crate::indexer::engine::validate_core_recovery_log;
use crate::indexer::errors::IndexerError;
use crate::indexer::runtime_helpers::source_operation;
use crate::model::{
    BackfillRequest, ChainCursor, Checkpoint, Commitment, ContractFilter, ContractLog,
};
use crate::source::ChainDataSource;
use crate::state::reducer::ReducerError;
use std::time::Duration;

const RECOVERY_PAGE_BLOCKS: u64 = 1_000;

#[derive(Clone, Copy)]
pub(in crate::indexer) struct RecoveryLimits {
    max_logs: usize,
    max_bytes: usize,
}

impl RecoveryLimits {
    pub(in crate::indexer) const fn new(max_logs: usize, max_bytes: usize) -> Self {
        Self {
            max_logs,
            max_bytes,
        }
    }
}

pub(in crate::indexer) const RECOVERY_LIMITS: RecoveryLimits =
    RecoveryLimits::new(65_536, 64 * 1024 * 1024);

#[derive(Default)]
pub(in crate::indexer) struct RecoveryUsage {
    logs: usize,
    bytes: usize,
}

impl RecoveryUsage {
    pub(in crate::indexer) fn admit(
        &mut self,
        page: &[ContractLog],
        limits: RecoveryLimits,
    ) -> Result<(), IndexerError> {
        if page.len() > limits.max_logs.saturating_sub(self.logs) {
            return Err(recovery_budget_error());
        }
        let page_bytes = page.iter().try_fold(0_usize, |bytes, log| {
            log.retained_bytes()
                .checked_add(bytes)
                .filter(|total| *total <= limits.max_bytes.saturating_sub(self.bytes))
                .ok_or_else(recovery_budget_error)
        })?;
        self.logs += page.len();
        self.bytes += page_bytes;
        Ok(())
    }
}

fn recovery_budget_error() -> IndexerError {
    IndexerError::Gap("canonical recovery exceeded its count or byte budget".into())
}

pub(crate) fn recovery_page_end(from_block: u64, to_block: u64) -> u64 {
    from_block
        .saturating_add(RECOVERY_PAGE_BLOCKS.saturating_sub(1))
        .min(to_block)
}

pub(super) async fn validate_floors<S: ChainDataSource>(
    source: &S,
    stable: &Checkpoint,
    finalized: Option<&Checkpoint>,
    operation_timeout: Duration,
) -> Result<(), IndexerError> {
    for checkpoint in std::iter::once(stable).chain(finalized) {
        if !source_operation(
            "recovery finalized-floor validation",
            operation_timeout,
            source.validate_checkpoint(checkpoint),
        )
        .await?
        {
            return Err(IndexerError::Gap(
                "stable finalized recovery floor is no longer canonical".into(),
            ));
        }
    }
    Ok(())
}

pub(super) async fn load<S: ChainDataSource>(
    source: &S,
    filter: &ContractFilter,
    from: &ChainCursor,
    to: &ChainCursor,
    core_event_sink: Option<&CoreEventSink>,
    operation_timeout: Duration,
) -> Result<Vec<ContractLog>, IndexerError> {
    let retain = core_event_sink.is_some_and(|sink| sink.accepts(Commitment::Canonical));
    if from.chain_id != to.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if from.block_number > to.block_number {
        return Err(IndexerError::Gap(
            "canonical recovery head regressed below the pre-gap cursor".into(),
        ));
    }
    let mut logs = Vec::new();
    let mut usage = RecoveryUsage::default();
    let mut page_start = from.block_number;
    loop {
        let page_end = recovery_page_end(page_start, to.block_number);
        let mut page = source_operation(
            "recovery backfill",
            operation_timeout,
            source.backfill(BackfillRequest {
                from_block: page_start,
                to_block: page_end,
                filter: filter.clone(),
            }),
        )
        .await?;
        for log in &page {
            validate_core_recovery_log(log, filter.address, to.chain_id, page_start..=page_end)?;
        }
        usage.admit(&page, RECOVERY_LIMITS)?;
        if retain {
            for log in &mut page {
                log.normalize_for_retention();
            }
            page.sort_by_key(|log| log.cursor.event_order());
            logs.extend(page);
        }
        if page_end == to.block_number {
            break;
        }
        page_start = page_end.saturating_add(1);
    }
    logs.sort_by_key(|log| log.cursor.event_order());
    Ok(logs)
}
