//! Feature-only reducer-segment trace lifecycle.

use super::ReducerRuntime;
use crate::indexer::client_types::QueuedChainUpdate;
use crate::indexer::perf_trace::{
    PerfTraceReducerSegmentBuilder, PerfTraceReducerSegmentOutcome, PerfTraceStage, correction_hash,
};
use lunarbase_math::B256;
use std::collections::VecDeque;
use std::time::Instant;
use tokio::sync::mpsc;

pub(super) struct SegmentTrace {
    builder: Option<PerfTraceReducerSegmentBuilder>,
    correction_hash: Option<B256>,
    collect_started_at: Instant,
}

impl SegmentTrace {
    pub(super) fn begin(
        update: &mut QueuedChainUpdate,
        updates: &mpsc::Receiver<QueuedChainUpdate>,
        runtime: &ReducerRuntime,
    ) -> Self {
        let first_received_at = update.mark_received(Instant::now());
        let first_admitted_at = update.admitted_at();
        let collect_started_at = Instant::now();
        let correction_hash = correction_hash(update.update());
        let builder = runtime.stats.begin_reducer_segment(
            update.update(),
            first_admitted_at,
            first_received_at,
            collect_started_at,
            updates.len(),
        );
        Self {
            builder,
            correction_hash,
            collect_started_at,
        }
    }

    pub(super) fn collected(
        &mut self,
        segment: &[QueuedChainUpdate],
        updates: &mpsc::Receiver<QueuedChainUpdate>,
        pending: &VecDeque<QueuedChainUpdate>,
        runtime: &ReducerRuntime,
    ) {
        let post_collect_at = Instant::now();
        if let Some(trace) = self.builder.as_mut() {
            let last = segment.last().expect("reducer segment is non-empty");
            let last_received_at = last
                .received_at()
                .expect("every collected queue item has a receive timestamp");
            trace.collected(
                last.update(),
                segment.len(),
                last.admitted_at(),
                last_received_at,
                updates.len(),
                pending.len(),
                runtime.stats.queue_depth(),
                runtime.stats.queue_bytes(),
                post_collect_at,
            );
        }
        if let Some(new_tip_hash) = self.correction_hash {
            runtime.stats.trace_correction_segment_at(
                new_tip_hash,
                PerfTraceStage::SegmentEntry,
                self.collect_started_at,
                Some(segment.len()),
                Some(updates.len()),
            );
        }
    }

    pub(super) fn builder_mut(&mut self) -> Option<&mut PerfTraceReducerSegmentBuilder> {
        self.builder.as_mut()
    }

    pub(super) fn finish(mut self, succeeded: bool) {
        if let Some(trace) = self.builder.take() {
            let outcome = if succeeded {
                PerfTraceReducerSegmentOutcome::Completed
            } else {
                PerfTraceReducerSegmentOutcome::Failed
            };
            trace.finish(outcome);
        }
    }
}
