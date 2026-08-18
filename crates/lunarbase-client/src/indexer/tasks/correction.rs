//! Private correction construction and one-swap publication.

use super::{ReducerRuntime, SharedQuoteState};
use crate::indexer::client::publish;
use crate::indexer::client_types::PendingCorrectionAdmission;
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
#[cfg(feature = "perf-trace")]
use crate::indexer::perf_trace::{PerfTraceReducerSegmentBuilder, PerfTraceStage};
use crate::model::ChainCorrection;
use std::sync::atomic::Ordering;

pub(super) fn apply_live_correction(
    shared: &SharedQuoteState,
    correction: &ChainCorrection,
    runtime: &ReducerRuntime,
    correction_admission: Option<PendingCorrectionAdmission>,
    #[cfg(feature = "perf-trace")] mut perf_trace: Option<&mut PerfTraceReducerSegmentBuilder>,
) -> Result<(), IndexerError> {
    let progress = CorrectionProgress::start(shared, correction_admission)?;
    #[cfg(feature = "perf-trace")]
    trace_stage(runtime, correction, PerfTraceStage::CorrectionBegin);
    let common_ancestor = correction.common_ancestor.cursor.block_number;
    let old_tip_hash = correction.old_tip.cursor.block_hash;
    let new_tip_hash = correction.new_tip.cursor.block_hash;
    let replacement_logs = correction.replacement_logs.len();

    // `QuoteIndexer::clone` only clones Arc handles. Journal copy-on-write,
    // validation, decoding, and replay all happen after releasing this guard.
    let (generation, base) = shared.indexer_candidate()?;
    #[cfg(feature = "perf-trace")]
    trace_stage(runtime, correction, PerfTraceStage::CandidateReady);
    #[cfg(feature = "perf-trace")]
    if let Some(trace) = perf_trace.as_deref_mut() {
        trace.candidate_ready(std::time::Instant::now());
    }
    let (candidate, applied) = base.into_corrected_core(correction)?;
    #[cfg(feature = "perf-trace")]
    trace_stage(runtime, correction, PerfTraceStage::BuildReady);
    #[cfg(feature = "perf-trace")]
    if let Some(trace) = perf_trace.as_deref_mut() {
        trace.post_apply(std::time::Instant::now());
    }
    if !applied {
        runtime.stats.record_state_update();
        if !progress.complete() {
            return Err(IndexerError::Gap(
                "source disconnected while correction was being verified".into(),
            ));
        }
        return Ok(());
    }
    let history_usage = candidate.correction_history_usage();

    // Publish one coherent state. Drop the retired journal/state after the
    // writer gate is gone so allocator/destructor work cannot stall publishers.
    #[cfg(feature = "perf-trace")]
    trace_stage(runtime, correction, PerfTraceStage::PreWrite);
    #[cfg(feature = "perf-trace")]
    if let Some(trace) = perf_trace.as_deref_mut() {
        trace.prewrite(std::time::Instant::now());
    }
    #[cfg(feature = "perf-trace")]
    let (published, publication_timing) =
        shared.publish_indexer_if_generation_traced(generation, candidate)?;
    #[cfg(not(feature = "perf-trace"))]
    let published = shared.publish_indexer_if_generation(generation, candidate)?;
    #[cfg(feature = "perf-trace")]
    {
        let returned_at = std::time::Instant::now();
        if let Some(trace) = perf_trace.as_deref_mut() {
            trace.publication_timing(publication_timing);
            trace.publication_returned(returned_at);
        }
        if let (Some(new_tip_hash), Some(recorded_at)) =
            (new_tip_hash, publication_timing.pre_store_at)
        {
            runtime
                .stats
                .trace_correction_at(new_tip_hash, PerfTraceStage::PreStore, recorded_at);
        }
    }
    let retired = published.ok_or_else(|| {
        IndexerError::Gap("published quote state changed during correction".into())
    })?;
    drop(retired);
    #[cfg(feature = "perf-trace")]
    if let Some(trace) = perf_trace {
        trace.post_drop(std::time::Instant::now());
    }
    #[cfg(feature = "perf-trace")]
    trace_stage(runtime, correction, PerfTraceStage::PostRetiredDrop);

    runtime.stats.record_state_update();
    #[cfg(feature = "perf-trace")]
    trace_stage(runtime, correction, PerfTraceStage::StateStatsRecorded);
    if !progress.complete() {
        return Err(IndexerError::Gap(
            "source disconnected while correction was being installed".into(),
        ));
    }
    #[cfg(feature = "perf-trace")]
    trace_stage(runtime, correction, PerfTraceStage::CorrectionCompleted);
    runtime.stats.corrections.fetch_add(1, Ordering::Relaxed);
    #[cfg(feature = "perf-trace")]
    trace_stage(runtime, correction, PerfTraceStage::CounterRecorded);
    sync_history_stats(runtime, history_usage);
    publish(
        &runtime.events,
        ClientRuntimeEvent::CorrectionApplied {
            common_ancestor,
            old_tip_hash: old_tip_hash.expect("validated correction old tip has a hash"),
            new_tip_hash: new_tip_hash.expect("validated correction new tip has a hash"),
            replacement_logs,
        },
    );
    #[cfg(feature = "perf-trace")]
    trace_stage(runtime, correction, PerfTraceStage::EventPublished);
    Ok(())
}

struct CorrectionProgress<'a> {
    shared: &'a SharedQuoteState,
    token: u64,
    queued: Option<PendingCorrectionAdmission>,
    completed: bool,
}

impl<'a> CorrectionProgress<'a> {
    fn start(
        shared: &'a SharedQuoteState,
        queued: Option<PendingCorrectionAdmission>,
    ) -> Result<Self, IndexerError> {
        let token = match queued.as_ref() {
            Some(admission) => {
                debug_assert!(admission.belongs_to(shared));
                admission.token()
            }
            None => shared.begin_correction().ok_or(IndexerError::NotReady)?,
        };
        Ok(Self {
            shared,
            token,
            queued,
            completed: false,
        })
    }

    fn complete(mut self) -> bool {
        let published = self.shared.complete_correction(self.token);
        if let Some(queued) = self.queued.take() {
            queued.disarm();
        }
        self.completed = true;
        published
    }
}

impl Drop for CorrectionProgress<'_> {
    fn drop(&mut self) {
        if !self.completed && self.queued.is_none() {
            self.shared.fail_correction(self.token);
        }
    }
}

pub(super) fn sync_history_stats(
    runtime: &ReducerRuntime,
    (blocks, bytes, evictions, _generation): (usize, usize, u64, u64),
) {
    runtime
        .stats
        .correction_history_blocks
        .store(blocks, Ordering::Relaxed);
    runtime
        .stats
        .correction_history_bytes
        .store(bytes, Ordering::Relaxed);
    let previous = runtime
        .stats
        .correction_history_evictions
        .swap(evictions, Ordering::Relaxed);
    // Keep the cumulative counter exact without broadcasting on every block
    // once a hot chain has filled its steady-state rollback window.
    if evictions > previous && evictions.is_power_of_two() {
        publish(
            &runtime.events,
            ClientRuntimeEvent::CorrectionHistoryPruned {
                total_evictions: evictions,
            },
        );
    }
}

#[cfg(feature = "perf-trace")]
fn trace_stage(runtime: &ReducerRuntime, correction: &ChainCorrection, stage: PerfTraceStage) {
    if let Some(new_tip_hash) = correction.new_tip.cursor.block_hash {
        runtime.stats.trace_correction(new_tip_hash, stage);
    }
}
