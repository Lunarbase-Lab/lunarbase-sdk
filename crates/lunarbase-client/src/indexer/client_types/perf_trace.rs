//! Default-off reducer diagnostic hooks.

use super::{ClientRuntimeStats, QueuedChainUpdate};
use crate::indexer::perf_trace::{
    PerfTrace, PerfTraceReducerSegmentBuilder, PerfTraceSegment, PerfTraceStage,
};
use crate::model::ChainUpdate;
use lunarbase_math::B256;
use std::time::Instant;

impl ClientRuntimeStats {
    pub(in crate::indexer) fn attach_perf_trace(&mut self, trace: PerfTrace) {
        self.perf_trace = Some(trace);
    }

    pub(in crate::indexer) fn trace_correction(&self, new_tip_hash: B256, stage: PerfTraceStage) {
        if let Some(trace) = self.perf_trace.as_ref() {
            trace.record(new_tip_hash, stage);
        }
    }

    pub(in crate::indexer) fn trace_correction_at(
        &self,
        new_tip_hash: B256,
        stage: PerfTraceStage,
        recorded_at: Instant,
    ) {
        if let Some(trace) = self.perf_trace.as_ref() {
            trace.record_at(new_tip_hash, stage, recorded_at);
        }
    }

    pub(in crate::indexer) fn trace_correction_segment_at(
        &self,
        new_tip_hash: B256,
        stage: PerfTraceStage,
        recorded_at: Instant,
        segment_len: Option<usize>,
        receiver_len: Option<usize>,
    ) {
        if let Some(trace) = self.perf_trace.as_ref() {
            trace.record_at_with_segment(
                new_tip_hash,
                stage,
                recorded_at,
                Some(PerfTraceSegment {
                    segment_len,
                    receiver_len,
                    queue_depth: self.queue_depth(),
                    queue_bytes: self.queue_bytes(),
                }),
            );
        }
    }

    pub(in crate::indexer) fn begin_reducer_segment(
        &self,
        first: &ChainUpdate,
        first_admitted_at: Instant,
        first_received_at: Instant,
        collect_started_at: Instant,
        receiver_len_before_collect: usize,
    ) -> Option<PerfTraceReducerSegmentBuilder> {
        self.perf_trace.as_ref().and_then(|trace| {
            trace.begin_reducer_segment(
                first,
                first_admitted_at,
                first_received_at,
                collect_started_at,
                receiver_len_before_collect,
                self.queue_depth(),
                self.queue_bytes(),
            )
        })
    }
}

impl QueuedChainUpdate {
    pub(in crate::indexer) fn admitted_at(&self) -> Instant {
        self.admitted_at
    }

    pub(in crate::indexer) fn mark_received(&mut self, recorded_at: Instant) -> Instant {
        *self.received_at.get_or_insert(recorded_at)
    }

    pub(in crate::indexer) fn received_at(&self) -> Option<Instant> {
        self.received_at
    }
}
