//! Private recovery validation, one-swap install, and post-ready notices.

use super::{ReducerRuntime, SharedQuoteState, correction};
use crate::bootstrap::BootstrapSnapshot;
use crate::indexer::client::publish;
use crate::indexer::engine::{CorrectionNotice, validate_core_log_identity};
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::indexer::event_delivery::{same_core_event_identity, try_observe_core_event};
use crate::model::{ChainUpdate, ContractLog};
use std::sync::atomic::Ordering;

pub(super) struct RecoveredInstall {
    observer_logs: Vec<ContractLog>,
    corrections: Vec<CorrectionNotice>,
    history_usage: (usize, usize, u64, u64),
}

pub(super) fn install(
    shared: &SharedQuoteState,
    snapshot: BootstrapSnapshot,
    buffered: Vec<&ChainUpdate>,
    backfill_logs: Vec<ContractLog>,
) -> Result<RecoveredInstall, IndexerError> {
    let (generation, mut candidate) = shared.indexer_candidate()?;
    let mut observer_logs = backfill_logs;
    let corrections = candidate.bootstrap_normalized_borrowed_with_notices(snapshot, buffered)?;
    observer_logs.sort_by_key(|log| log.cursor.event_order());
    observer_logs.dedup_by(|right, left| same_core_event_identity(left, right));
    for log in &observer_logs {
        validate_core_log_identity(
            log,
            candidate.deployment().core,
            candidate.deployment().chain_id,
        )?;
    }
    let history_usage = candidate.correction_history_usage();
    let retired = shared
        .publish_indexer_if_generation(generation, candidate)?
        .ok_or_else(|| {
            IndexerError::Gap("published quote state changed during recovery install".into())
        })?;
    drop(retired);
    Ok(RecoveredInstall {
        observer_logs,
        corrections,
        history_usage,
    })
}

impl RecoveredInstall {
    pub(super) fn record_stats(&self, runtime: &ReducerRuntime) {
        runtime
            .stats
            .corrections
            .fetch_add(self.corrections.len() as u64, Ordering::Relaxed);
        correction::sync_history_stats(runtime, self.history_usage);
    }

    pub(super) fn publish_after_ready(self, runtime: &ReducerRuntime, staged: Vec<ChainUpdate>) {
        let Self {
            observer_logs,
            corrections,
            ..
        } = self;
        let mut ordered = observer_logs;
        if let Some(sink) = runtime.core_event_sink.as_ref() {
            ordered.extend(staged.into_iter().filter_map(|update| match update {
                ChainUpdate::Log(log) if sink.accepts(log.cursor.commitment) => Some(log),
                _ => None,
            }));
        }
        ordered.sort_by_key(|log| log.cursor.event_order());
        ordered.dedup_by(|right, left| same_core_event_identity(left, right));
        for log in ordered {
            try_observe_core_event(runtime.core_event_sink.as_ref(), log, &runtime.stats);
        }
        for notice in corrections {
            publish(
                &runtime.events,
                ClientRuntimeEvent::CorrectionApplied {
                    common_ancestor: notice.common_ancestor,
                    old_tip_hash: notice.old_tip_hash,
                    new_tip_hash: notice.new_tip_hash,
                    replacement_logs: notice.replacement_logs,
                },
            );
        }
    }
}
