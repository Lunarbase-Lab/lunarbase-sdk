//! Fixed-capacity reducer-segment records for diagnostic throughput attribution.

use super::PerfTrace;
use crate::model::{ChainCursor, ChainUpdate};
use lunarbase_math::B256;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

/// Maximum reducer segments retained by one diagnostic trace.
///
/// This covers the worst-case singleton segmentation of the three-second r25
/// proof workload, including its warmup, without overwriting older records.
pub const PERF_TRACE_REDUCER_SEGMENT_CAPACITY: usize = 4_096;

/// First-update classification for one ordered reducer segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PerfTraceReducerSegmentKind {
    /// An ordinary segment beginning with a block head.
    OrdinaryHead,
    /// An ordinary segment beginning with a log.
    OrdinaryLog,
    /// An optimistic correction barrier.
    Correction,
    /// A reorg control barrier.
    Reorg,
    /// A source-gap control barrier.
    Gap,
}

/// Terminal state of one reducer-segment record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PerfTraceReducerSegmentOutcome {
    /// The segment completed its normal reducer path.
    Completed,
    /// The segment returned a reducer error and entered recovery handling.
    Failed,
    /// The trace builder was dropped before an explicit terminal outcome.
    Incomplete,
}

/// Compact source identity used to group segment fragments without allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PerfTraceReducerIdentity {
    /// EIP-155 chain identifier.
    pub chain_id: u64,
    /// Logical block height.
    pub block_number: u64,
    /// Execution context used for quote evaluation.
    pub execution_block_number: u64,
    /// Exact block identity when supplied by the source.
    pub block_hash: Option<B256>,
    /// Source transport sequence when supplied by the source.
    pub source_sequence: Option<u64>,
    /// Source-local sub-index when supplied by the source.
    pub source_sub_index: Option<u32>,
    /// Transaction index for event positions.
    pub transaction_index: Option<u32>,
    /// Log index for event positions.
    pub log_index: Option<u32>,
}

impl From<&ChainCursor> for PerfTraceReducerIdentity {
    fn from(cursor: &ChainCursor) -> Self {
        Self {
            chain_id: cursor.chain_id,
            block_number: cursor.block_number,
            execution_block_number: cursor.execution_block_number,
            block_hash: cursor.block_hash,
            source_sequence: cursor.source_sequence,
            source_sub_index: cursor.source_sub_index,
            transaction_index: cursor.transaction_index,
            log_index: cursor.log_index,
        }
    }
}

/// Internal publication timestamps captured around the exact ArcSwap store.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PerfTracePublicationTiming {
    /// Timestamp immediately after acquiring the writer-only gate.
    pub(crate) writer_gate_acquired_at: Option<Instant>,
    /// Hard visibility lower bound captured immediately before the atomic store.
    pub(crate) pre_store_at: Option<Instant>,
    /// Diagnostic timestamp captured after the atomic store returned.
    pub(crate) store_returned_at: Option<Instant>,
    /// Diagnostic timestamp captured after releasing the writer-only gate.
    pub(crate) writer_gate_released_at: Option<Instant>,
}

/// One complete diagnostic record for a reducer segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PerfTraceReducerSegment {
    /// Reservation order; use timestamps for logical ordering.
    pub sequence: usize,
    /// Classification derived from the segment's first update.
    pub kind: PerfTraceReducerSegmentKind,
    /// First update identity.
    pub first_identity: Option<PerfTraceReducerIdentity>,
    /// Last collected update identity.
    pub last_identity: Option<PerfTraceReducerIdentity>,
    /// Queue-admission timestamp of the segment's oldest item.
    pub first_admitted_at: Instant,
    /// Queue-admission timestamp of the segment's newest item.
    pub last_admitted_at: Instant,
    /// Number of updates applied by this segment.
    pub segment_len: usize,
    /// Receiver items visible before zero-wait collection.
    pub receiver_len_before_collect: usize,
    /// Receiver items left after collection.
    pub receiver_len_after_collect: usize,
    /// Locally retained barriers left after collection.
    pub pending_len_after_collect: usize,
    /// Exact retained queue items when the first update was selected.
    pub queue_depth_at_start: usize,
    /// Exact retained queue bytes when the first update was selected.
    pub queue_bytes_at_start: usize,
    /// Exact retained queue items immediately after collection.
    pub queue_depth_after_collect: usize,
    /// Exact retained queue bytes immediately after collection.
    pub queue_bytes_after_collect: usize,
    /// Time at which the reducer first removed the oldest item from the channel.
    pub first_received_at: Instant,
    /// Time at which the reducer first removed the newest item from the channel.
    pub last_received_at: Instant,
    /// Time immediately before collection began.
    pub collect_started_at: Instant,
    /// Time immediately after collection returned.
    pub post_collect_at: Option<Instant>,
    /// Time after the private publication candidate was cloned.
    pub candidate_ready_at: Option<Instant>,
    /// Time after every update was applied to the private candidate.
    pub post_apply_at: Option<Instant>,
    /// Time immediately before requesting the writer-only publication gate.
    pub prewrite_at: Option<Instant>,
    /// Time immediately after acquiring the writer-only gate.
    pub writer_gate_acquired_at: Option<Instant>,
    /// Hard visibility lower bound captured immediately before the atomic store.
    pub pre_store_at: Option<Instant>,
    /// Diagnostic timestamp captured after the atomic store returned.
    pub store_returned_at: Option<Instant>,
    /// Diagnostic timestamp captured after releasing the writer-only gate.
    pub writer_gate_released_at: Option<Instant>,
    /// Caller-side timestamp after the publication helper returned.
    pub publication_returned_at: Option<Instant>,
    /// Timestamp after the retired state was dropped outside the guard.
    pub post_drop_at: Option<Instant>,
    /// Explicit normal, failed, or incomplete terminal state.
    pub outcome: PerfTraceReducerSegmentOutcome,
}

/// Immutable view of bounded reducer-segment diagnostics.
#[derive(Clone, Debug)]
pub struct PerfTraceReducerSegmentSnapshot {
    /// Fully published records in reservation order.
    pub segments: Vec<PerfTraceReducerSegment>,
    /// True when more segments started than the fixed storage can retain.
    pub overflowed: bool,
    /// Segments rejected after the fixed storage filled.
    pub dropped_segments: usize,
    /// Reserved or published records without an explicit terminal outcome.
    pub incomplete_segments: usize,
    /// Segments whose reducer application returned an error.
    pub failed_segments: usize,
}

#[derive(Debug)]
pub(super) struct PerfTraceReducerSegmentStorage {
    next: AtomicUsize,
    overflowed: AtomicBool,
    dropped: AtomicUsize,
    slots: Vec<OnceLock<PerfTraceReducerSegment>>,
}

impl PerfTraceReducerSegmentStorage {
    pub(super) fn new() -> Self {
        Self {
            next: AtomicUsize::new(0),
            overflowed: AtomicBool::new(false),
            dropped: AtomicUsize::new(0),
            slots: (0..PERF_TRACE_REDUCER_SEGMENT_CAPACITY)
                .map(|_| OnceLock::new())
                .collect(),
        }
    }

    fn reserve(&self) -> Option<usize> {
        let sequence = self.next.fetch_add(1, Ordering::Relaxed);
        if sequence < PERF_TRACE_REDUCER_SEGMENT_CAPACITY {
            return Some(sequence);
        }
        self.overflowed.store(true, Ordering::Release);
        self.dropped.fetch_add(1, Ordering::Relaxed);
        None
    }

    fn publish(&self, record: PerfTraceReducerSegment) {
        let published = self.slots[record.sequence].set(record).is_ok();
        debug_assert!(published, "reducer trace slot is published exactly once");
        if !published {
            self.overflowed.store(true, Ordering::Release);
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn snapshot(&self) -> PerfTraceReducerSegmentSnapshot {
        let reserved = self
            .next
            .load(Ordering::Acquire)
            .min(PERF_TRACE_REDUCER_SEGMENT_CAPACITY);
        let mut segments = Vec::with_capacity(reserved);
        let mut incomplete_segments = 0;
        let mut failed_segments = 0;
        for slot in &self.slots[..reserved] {
            match slot.get().copied() {
                Some(segment) => {
                    incomplete_segments +=
                        usize::from(segment.outcome == PerfTraceReducerSegmentOutcome::Incomplete);
                    failed_segments +=
                        usize::from(segment.outcome == PerfTraceReducerSegmentOutcome::Failed);
                    segments.push(segment);
                }
                None => incomplete_segments += 1,
            }
        }
        PerfTraceReducerSegmentSnapshot {
            segments,
            overflowed: self.overflowed.load(Ordering::Acquire),
            dropped_segments: self.dropped.load(Ordering::Acquire),
            incomplete_segments,
            failed_segments,
        }
    }
}

pub(crate) struct PerfTraceReducerSegmentBuilder {
    trace: PerfTrace,
    record: Option<PerfTraceReducerSegment>,
}

impl PerfTraceReducerSegmentBuilder {
    pub(super) fn begin(
        trace: PerfTrace,
        first: &ChainUpdate,
        first_admitted_at: Instant,
        first_received_at: Instant,
        collect_started_at: Instant,
        receiver_len_before_collect: usize,
        queue_depth_at_start: usize,
        queue_bytes_at_start: usize,
    ) -> Option<Self> {
        let sequence = trace.inner.reducer_segments.reserve()?;
        Some(Self {
            trace,
            record: Some(PerfTraceReducerSegment {
                sequence,
                kind: segment_kind(first),
                first_identity: update_cursor(first).map(Into::into),
                last_identity: update_cursor(first).map(Into::into),
                segment_len: 1,
                first_admitted_at,
                last_admitted_at: first_admitted_at,
                receiver_len_before_collect,
                receiver_len_after_collect: receiver_len_before_collect,
                pending_len_after_collect: 0,
                queue_depth_at_start,
                queue_bytes_at_start,
                queue_depth_after_collect: queue_depth_at_start,
                queue_bytes_after_collect: queue_bytes_at_start,
                first_received_at,
                last_received_at: first_received_at,
                collect_started_at,
                post_collect_at: None,
                candidate_ready_at: None,
                post_apply_at: None,
                prewrite_at: None,
                writer_gate_acquired_at: None,
                pre_store_at: None,
                store_returned_at: None,
                writer_gate_released_at: None,
                publication_returned_at: None,
                post_drop_at: None,
                outcome: PerfTraceReducerSegmentOutcome::Incomplete,
            }),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn collected(
        &mut self,
        last: &ChainUpdate,
        segment_len: usize,
        last_admitted_at: Instant,
        last_received_at: Instant,
        receiver_len_after_collect: usize,
        pending_len_after_collect: usize,
        queue_depth_after_collect: usize,
        queue_bytes_after_collect: usize,
        recorded_at: Instant,
    ) {
        let record = self.record_mut();
        record.last_identity = update_cursor(last).map(Into::into);
        record.segment_len = segment_len;
        record.last_admitted_at = last_admitted_at;
        record.last_received_at = last_received_at;
        record.receiver_len_after_collect = receiver_len_after_collect;
        record.pending_len_after_collect = pending_len_after_collect;
        record.queue_depth_after_collect = queue_depth_after_collect;
        record.queue_bytes_after_collect = queue_bytes_after_collect;
        record.post_collect_at = Some(recorded_at);
    }

    pub(crate) fn candidate_ready(&mut self, recorded_at: Instant) {
        self.record_mut().candidate_ready_at = Some(recorded_at);
    }

    pub(crate) fn post_apply(&mut self, recorded_at: Instant) {
        self.record_mut().post_apply_at = Some(recorded_at);
    }

    pub(crate) fn prewrite(&mut self, recorded_at: Instant) {
        self.record_mut().prewrite_at = Some(recorded_at);
    }

    pub(crate) fn publication_timing(&mut self, timing: PerfTracePublicationTiming) {
        let record = self.record_mut();
        record.writer_gate_acquired_at = timing.writer_gate_acquired_at;
        record.pre_store_at = timing.pre_store_at;
        record.store_returned_at = timing.store_returned_at;
        record.writer_gate_released_at = timing.writer_gate_released_at;
    }

    pub(crate) fn publication_returned(&mut self, recorded_at: Instant) {
        self.record_mut().publication_returned_at = Some(recorded_at);
    }

    pub(crate) fn post_drop(&mut self, recorded_at: Instant) {
        self.record_mut().post_drop_at = Some(recorded_at);
    }

    pub(crate) fn finish(mut self, outcome: PerfTraceReducerSegmentOutcome) {
        let mut record = self.record.take().expect("reducer trace builder is live");
        record.outcome = outcome;
        self.trace.inner.reducer_segments.publish(record);
    }

    fn record_mut(&mut self) -> &mut PerfTraceReducerSegment {
        self.record.as_mut().expect("reducer trace builder is live")
    }
}

impl Drop for PerfTraceReducerSegmentBuilder {
    fn drop(&mut self) {
        if let Some(record) = self.record.take() {
            self.trace.inner.reducer_segments.publish(record);
        }
    }
}

fn segment_kind(update: &ChainUpdate) -> PerfTraceReducerSegmentKind {
    match update {
        ChainUpdate::Head(_) => PerfTraceReducerSegmentKind::OrdinaryHead,
        ChainUpdate::Log(_) => PerfTraceReducerSegmentKind::OrdinaryLog,
        ChainUpdate::Correction(_) => PerfTraceReducerSegmentKind::Correction,
        ChainUpdate::Reorg { .. } => PerfTraceReducerSegmentKind::Reorg,
        ChainUpdate::Gap { .. } => PerfTraceReducerSegmentKind::Gap,
    }
}

fn update_cursor(update: &ChainUpdate) -> Option<&ChainCursor> {
    match update {
        ChainUpdate::Head(head) => Some(&head.cursor),
        ChainUpdate::Log(log) => Some(&log.cursor),
        ChainUpdate::Correction(correction) => Some(&correction.new_tip.cursor),
        ChainUpdate::Reorg { new_head, .. } => Some(&new_head.cursor),
        ChainUpdate::Gap { cursor, .. } => cursor.as_ref(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BlockRef, Commitment};
    use std::time::Duration;

    #[test]
    fn completed_builder_preserves_admission_and_publication_bounds() {
        let trace = PerfTrace::new();
        let update = head(7);
        let admitted = Instant::now();
        let received = after(admitted, 10);
        let collect_started = after(received, 10);
        let post_collect = after(collect_started, 10);
        let candidate_ready = after(post_collect, 10);
        let post_apply = after(candidate_ready, 10);
        let prewrite = after(post_apply, 10);
        let gate_acquired = after(prewrite, 10);
        let pre_store = after(gate_acquired, 10);
        let store_returned = after(pre_store, 10);
        let gate_released = after(store_returned, 10);
        let returned = after(gate_released, 10);
        let post_drop = after(returned, 10);

        let mut builder = PerfTraceReducerSegmentBuilder::begin(
            trace.clone(),
            &update,
            admitted,
            received,
            collect_started,
            3,
            4,
            512,
        )
        .expect("segment trace has capacity");
        builder.collected(&update, 1, admitted, received, 2, 0, 4, 512, post_collect);
        builder.candidate_ready(candidate_ready);
        builder.post_apply(post_apply);
        builder.prewrite(prewrite);
        builder.publication_timing(PerfTracePublicationTiming {
            writer_gate_acquired_at: Some(gate_acquired),
            pre_store_at: Some(pre_store),
            store_returned_at: Some(store_returned),
            writer_gate_released_at: Some(gate_released),
        });
        builder.publication_returned(returned);
        builder.post_drop(post_drop);
        builder.finish(PerfTraceReducerSegmentOutcome::Completed);

        let snapshot = trace.snapshot().reducer_segments;
        assert!(!snapshot.overflowed);
        assert_eq!(snapshot.dropped_segments, 0);
        assert_eq!(snapshot.incomplete_segments, 0);
        assert_eq!(snapshot.failed_segments, 0);
        assert_eq!(snapshot.segments.len(), 1);
        let segment = snapshot.segments[0];
        assert_eq!(segment.kind, PerfTraceReducerSegmentKind::OrdinaryHead);
        assert_eq!(segment.first_admitted_at, admitted);
        assert_eq!(segment.last_admitted_at, admitted);
        assert_eq!(segment.first_received_at, received);
        assert_eq!(segment.last_received_at, received);
        assert_eq!(segment.segment_len, 1);
        assert_eq!(segment.writer_gate_acquired_at, Some(gate_acquired));
        assert_eq!(segment.pre_store_at, Some(pre_store));
        assert_eq!(segment.store_returned_at, Some(store_returned));
        assert_eq!(segment.writer_gate_released_at, Some(gate_released));
        assert_eq!(segment.publication_returned_at, Some(returned));
        assert_eq!(segment.post_drop_at, Some(post_drop));
        assert_eq!(segment.outcome, PerfTraceReducerSegmentOutcome::Completed);
    }

    #[test]
    fn dropped_builder_is_reported_as_incomplete() {
        let trace = PerfTrace::new();
        let update = head(8);
        let admitted = Instant::now();
        let received = after(admitted, 10);
        let builder = PerfTraceReducerSegmentBuilder::begin(
            trace.clone(),
            &update,
            admitted,
            received,
            received,
            0,
            1,
            128,
        )
        .expect("segment trace has capacity");

        drop(builder);

        let snapshot = trace.snapshot().reducer_segments;
        assert_eq!(snapshot.segments.len(), 1);
        assert_eq!(snapshot.incomplete_segments, 1);
        assert_eq!(
            snapshot.segments[0].outcome,
            PerfTraceReducerSegmentOutcome::Incomplete
        );
    }

    fn after(instant: Instant, micros: u64) -> Instant {
        instant
            .checked_add(Duration::from_micros(micros))
            .expect("test timestamp fits")
    }

    fn head(block: u64) -> ChainUpdate {
        ChainUpdate::Head(BlockRef::new(
            ChainCursor::block(
                1,
                block,
                Some(B256::new([block as u8; 32])),
                Commitment::Realtime,
            ),
            None,
        ))
    }
}
