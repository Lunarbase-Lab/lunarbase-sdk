//! Zero-wait ordinary-update segmentation and coherent one-swap publication.

use super::{ReducerRuntime, correction};
use crate::indexer::client_types::{QueuedChainUpdate, SharedQuoteState};
use crate::indexer::errors::IndexerError;
use crate::indexer::event_delivery::try_observe_core_event;
#[cfg(feature = "perf-trace")]
use crate::indexer::perf_trace::PerfTraceReducerSegmentBuilder;
use crate::model::{ChainCursor, ChainUpdate};
use std::collections::VecDeque;
use tokio::sync::mpsc;

const MAX_ORDINARY_SEGMENT_UPDATES: usize = 256;

pub(super) struct FailedLiveUpdate {
    pub(super) error: IndexerError,
    pub(super) queued: Vec<QueuedChainUpdate>,
    pub(super) failed_index: usize,
    pub(super) prior_cursor: Option<ChainCursor>,
}

pub(super) fn collect_live_segment(
    first: QueuedChainUpdate,
    updates: &mut mpsc::Receiver<QueuedChainUpdate>,
    pending: &mut VecDeque<QueuedChainUpdate>,
) -> Vec<QueuedChainUpdate> {
    let identity = ordinary_cursor(first.update()).cloned();
    let mut segment = Vec::with_capacity(MAX_ORDINARY_SEGMENT_UPDATES);
    segment.push(first);
    let Some(identity) = identity else {
        return segment;
    };

    while segment.len() < MAX_ORDINARY_SEGMENT_UPDATES {
        let Ok(next) = updates.try_recv() else {
            break;
        };
        #[cfg(feature = "perf-trace")]
        let next = {
            let mut next = next;
            next.mark_received(std::time::Instant::now());
            next
        };
        if ordinary_log_joins(&identity, next.update()) {
            segment.push(next);
        } else {
            pending.push_back(next);
            break;
        }
    }
    segment
}

fn ordinary_cursor(update: &ChainUpdate) -> Option<&ChainCursor> {
    match update {
        ChainUpdate::Head(head) => Some(&head.cursor),
        ChainUpdate::Log(log) => Some(&log.cursor),
        ChainUpdate::Reorg { .. } | ChainUpdate::Correction(_) | ChainUpdate::Gap { .. } => None,
    }
}

fn ordinary_log_joins(identity: &ChainCursor, update: &ChainUpdate) -> bool {
    let ChainUpdate::Log(log) = update else {
        return false;
    };
    let cursor = &log.cursor;
    cursor.chain_id == identity.chain_id
        && cursor.block_number == identity.block_number
        && cursor.execution_block_number == identity.execution_block_number
        && cursor.block_hash == identity.block_hash
}

pub(super) fn apply_live_segment(
    shared: &SharedQuoteState,
    mut queued: Vec<QueuedChainUpdate>,
    source_lease: Option<u64>,
    runtime: &ReducerRuntime,
    #[cfg(feature = "perf-trace")] perf_trace: Option<&mut PerfTraceReducerSegmentBuilder>,
) -> Result<(), Box<FailedLiveUpdate>> {
    debug_assert!(!queued.is_empty());
    if ordinary_cursor(queued[0].update()).is_some() {
        #[cfg(feature = "perf-trace")]
        return apply_ordinary_segment(shared, queued, source_lease, runtime, perf_trace);
        #[cfg(not(feature = "perf-trace"))]
        return apply_ordinary_segment(shared, queued, source_lease, runtime);
    }
    let correction_admission = queued[0].take_correction_admission();
    if let ChainUpdate::Correction(correction) = queued[0].update() {
        #[cfg(feature = "perf-trace")]
        let correction_result = correction::apply_live_correction(
            shared,
            correction,
            runtime,
            correction_admission,
            perf_trace,
        );
        #[cfg(not(feature = "perf-trace"))]
        let correction_result =
            correction::apply_live_correction(shared, correction, runtime, correction_admission);
        if let Err(error) = correction_result {
            return Err(Box::new(FailedLiveUpdate {
                error,
                queued,
                failed_index: 0,
                prior_cursor: published_cursor(shared),
            }));
        }
        drop(
            queued
                .into_iter()
                .next()
                .expect("correction segment is non-empty")
                .dequeue(),
        );
        return Ok(());
    }
    apply_control_segment(shared, queued, source_lease, runtime)
}

fn apply_ordinary_segment(
    shared: &SharedQuoteState,
    queued: Vec<QueuedChainUpdate>,
    source_lease: Option<u64>,
    runtime: &ReducerRuntime,
    #[cfg(feature = "perf-trace")] mut perf_trace: Option<&mut PerfTraceReducerSegmentBuilder>,
) -> Result<(), Box<FailedLiveUpdate>> {
    let (generation, mut candidate) = match shared.indexer_candidate() {
        Ok(candidate) => candidate,
        Err(error) => {
            return Err(Box::new(FailedLiveUpdate {
                error,
                queued,
                failed_index: 0,
                prior_cursor: None,
            }));
        }
    };
    let prior_cursor = candidate.reducer.cursor().cloned();
    #[cfg(feature = "perf-trace")]
    if let Some(trace) = perf_trace.as_deref_mut() {
        trace.candidate_ready(std::time::Instant::now());
    }
    let history_generation = candidate.correction_history_generation();
    let mut deliveries = Vec::with_capacity(queued.len());
    let mut failed = None;
    for (index, queued_update) in queued.iter().enumerate() {
        match apply_candidate_update(&mut candidate, queued_update.update(), runtime) {
            Ok(deliver_log) => deliveries.push(deliver_log),
            Err(error) => {
                failed = Some((index, error));
                break;
            }
        }
    }
    if let Some((failed_index, error)) = failed {
        return Err(Box::new(FailedLiveUpdate {
            error,
            queued,
            failed_index,
            prior_cursor,
        }));
    }

    #[cfg(feature = "perf-trace")]
    if let Some(trace) = perf_trace.as_deref_mut() {
        trace.post_apply(std::time::Instant::now());
    }
    let history_usage = (candidate.correction_history_generation() != history_generation)
        .then(|| candidate.correction_history_usage());
    let failed_index = queued.len() - 1;
    #[cfg(feature = "perf-trace")]
    if let Some(trace) = perf_trace.as_deref_mut() {
        trace.prewrite(std::time::Instant::now());
    }
    #[cfg(feature = "perf-trace")]
    let published = match shared.publish_indexer_if_generation_traced(generation, candidate) {
        Ok((published, timing)) => {
            let returned_at = std::time::Instant::now();
            if let Some(trace) = perf_trace.as_deref_mut() {
                trace.publication_timing(timing);
                trace.publication_returned(returned_at);
            }
            published
        }
        Err(error) => {
            return Err(Box::new(FailedLiveUpdate {
                error,
                queued,
                failed_index,
                prior_cursor,
            }));
        }
    };
    #[cfg(not(feature = "perf-trace"))]
    let published = match shared.publish_indexer_if_generation(generation, candidate) {
        Ok(published) => published,
        Err(error) => {
            return Err(Box::new(FailedLiveUpdate {
                error,
                queued,
                failed_index,
                prior_cursor,
            }));
        }
    };
    let retired = match published {
        Some(retired) => retired,
        None => {
            return Err(Box::new(FailedLiveUpdate {
                error: IndexerError::Gap(
                    "published quote state changed during ordinary update segment".into(),
                ),
                queued,
                failed_index,
                prior_cursor,
            }));
        }
    };
    drop(retired);
    #[cfg(feature = "perf-trace")]
    if let Some(trace) = perf_trace {
        trace.post_drop(std::time::Instant::now());
    }

    for (queued_update, deliver_log) in queued.into_iter().zip(deliveries) {
        let update = queued_update.dequeue();
        if deliver_log {
            let ChainUpdate::Log(log) = update else {
                unreachable!("only a borrowed log requests observer delivery");
            };
            try_observe_core_event(runtime.core_event_sink.as_ref(), log, &runtime.stats);
        }
    }
    if let Some(history_usage) = history_usage {
        correction::sync_history_stats(runtime, history_usage);
    }
    runtime.stats.record_state_update();
    if let Some(source_lease) = source_lease {
        shared.publish_available_if(source_lease);
    }
    Ok(())
}

fn apply_control_segment(
    shared: &SharedQuoteState,
    queued: Vec<QueuedChainUpdate>,
    source_lease: Option<u64>,
    runtime: &ReducerRuntime,
) -> Result<(), Box<FailedLiveUpdate>> {
    debug_assert_eq!(queued.len(), 1);
    let prior_cursor = published_cursor(shared);
    let transition = shared.mutate_indexer(|indexer| {
        let history_generation = indexer.correction_history_generation();
        apply_candidate_update(indexer, queued[0].update(), runtime).map(|deliver_log| {
            let history_usage = (indexer.correction_history_generation() != history_generation)
                .then(|| indexer.correction_history_usage());
            (deliver_log, history_usage)
        })
    });
    let (deliver_log, history_usage) = match transition {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) | Err(error) => {
            return Err(Box::new(FailedLiveUpdate {
                error,
                queued,
                failed_index: 0,
                prior_cursor,
            }));
        }
    };

    let update = queued
        .into_iter()
        .next()
        .expect("control segment is non-empty")
        .dequeue();
    if deliver_log {
        let ChainUpdate::Log(log) = update else {
            unreachable!("only a borrowed log requests observer delivery");
        };
        try_observe_core_event(runtime.core_event_sink.as_ref(), log, &runtime.stats);
    }
    if let Some(history_usage) = history_usage {
        correction::sync_history_stats(runtime, history_usage);
    }
    runtime.stats.record_state_update();
    if let Some(source_lease) = source_lease {
        shared.publish_available_if(source_lease);
    }
    Ok(())
}

fn apply_candidate_update(
    indexer: &mut crate::indexer::engine::QuoteIndexer,
    update: &ChainUpdate,
    runtime: &ReducerRuntime,
) -> Result<bool, IndexerError> {
    match update {
        ChainUpdate::Log(log)
            if runtime
                .core_event_sink
                .as_ref()
                .is_some_and(|sink| sink.accepts(log.cursor.commitment)) =>
        {
            indexer.apply_core_log_borrowed(log)
        }
        update => indexer.apply_core_update_borrowed(update).map(|_| false),
    }
}

fn published_cursor(shared: &SharedQuoteState) -> Option<ChainCursor> {
    shared
        .load_indexer()
        .ok()
        .and_then(|indexer| indexer.reducer.cursor().cloned())
}
#[cfg(test)]
#[path = "microbatch_tests.rs"]
mod tests;
