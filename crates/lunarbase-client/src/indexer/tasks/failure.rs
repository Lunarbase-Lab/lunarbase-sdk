//! Fail-closed reducer transition handling and bounded shutdown drain.

use super::{ReducerRuntime, SharedQuoteState, apply_live_segment, collect_live_segment};
use crate::indexer::client::publish;
use crate::indexer::client_types::QueuedChainUpdate;
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

pub(super) async fn drain_pending_updates(
    shared: &SharedQuoteState,
    pending: &mut VecDeque<QueuedChainUpdate>,
    updates: &mut mpsc::Receiver<QueuedChainUpdate>,
    runtime: &ReducerRuntime,
) {
    let mut state_valid = true;
    loop {
        let queued = match pending.pop_front() {
            Some(queued) => Some(queued),
            None => updates.recv().await,
        };
        let Some(queued) = queued else {
            return;
        };
        if state_valid {
            let segment = collect_live_segment(queued, updates, pending);
            #[cfg(feature = "perf-trace")]
            let applied = apply_live_segment(shared, segment, None, runtime, None);
            #[cfg(not(feature = "perf-trace"))]
            let applied = apply_live_segment(shared, segment, None, runtime);
            if let Err(mut failed) = applied {
                failed.queued.extend(pending.drain(..));
                record_transition_failure(shared, failed.error, runtime);
                for queued in failed.queued {
                    drop(queued);
                }
                state_valid = false;
            }
        } else {
            drop(queued);
        }
    }
}

pub(super) fn record_transition_failure(
    shared: &SharedQuoteState,
    error: IndexerError,
    runtime: &ReducerRuntime,
) {
    shared.revoke_available();
    let _ = shared.mutate_indexer(|indexer| indexer.reducer.mark_not_ready());
    runtime.stats.gaps.fetch_add(1, Ordering::Relaxed);
    publish(
        &runtime.events,
        ClientRuntimeEvent::StateTransitionFailed {
            detail: error.to_string(),
        },
    );
}
