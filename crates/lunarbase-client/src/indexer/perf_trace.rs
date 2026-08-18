//! Default-off, process-local correction-stage tracing for release diagnostics.
//!
//! Enabled only by the perf-trace feature. Recording uses a fixed lock-free
//! event array and never writes to an external sink.

mod reducer_segment;
use crate::model::ChainUpdate;
use lunarbase_math::B256;
pub use reducer_segment::{
    PERF_TRACE_REDUCER_SEGMENT_CAPACITY, PerfTraceReducerIdentity, PerfTraceReducerSegment,
    PerfTraceReducerSegmentKind, PerfTraceReducerSegmentOutcome, PerfTraceReducerSegmentSnapshot,
};
pub(crate) use reducer_segment::{PerfTracePublicationTiming, PerfTraceReducerSegmentBuilder};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Maximum distinct corrections represented by one diagnostic trace.
pub const PERF_TRACE_CORRECTION_CAPACITY: usize = 1_024;

const PERF_TRACE_STAGE_COUNT: usize = 13;
const PERF_TRACE_EVENT_CAPACITY: usize = PERF_TRACE_CORRECTION_CAPACITY * PERF_TRACE_STAGE_COUNT;
const UNSET_METRIC: usize = usize::MAX;

/// Ordered SDK stages following the harness-side publish timestamps T0..T3.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum PerfTraceStage {
    /// T4: the source pump yielded a correction item.
    SourceItem = 0,
    /// T5: the normalized correction entered the bounded reducer queue.
    QueueAdmission = 1,
    /// T6: the reducer began the segment containing the correction.
    SegmentEntry = 2,
    /// T7: the availability lease admitted correction construction.
    CorrectionBegin = 3,
    /// T8: the reducer cloned the exact published candidate base.
    CandidateReady = 4,
    /// T9: validation, rollback, and replacement replay completed privately.
    BuildReady = 5,
    /// T10: the reducer is about to enter the publication write operation.
    PreWrite = 6,
    /// T11: hard visibility lower bound captured immediately before the
    /// lock-free coherent snapshot store.
    PreStore = 7,
    /// T12: the retired indexer was dropped outside the writer-only gate.
    PostRetiredDrop = 8,
    /// T13: state-publication runtime statistics were recorded.
    StateStatsRecorded = 9,
    /// T14: the correction availability lease returned to Ready.
    CorrectionCompleted = 10,
    /// T15: the applied-correction counter was incremented.
    CounterRecorded = 11,
    /// T16: the operational CorrectionApplied event was published.
    EventPublished = 12,
}

impl PerfTraceStage {
    fn from_raw(raw: u8) -> Option<Self> {
        Some(match raw {
            0 => Self::SourceItem,
            1 => Self::QueueAdmission,
            2 => Self::SegmentEntry,
            3 => Self::CorrectionBegin,
            4 => Self::CandidateReady,
            5 => Self::BuildReady,
            6 => Self::PreWrite,
            7 => Self::PreStore,
            8 => Self::PostRetiredDrop,
            9 => Self::StateStatsRecorded,
            10 => Self::CorrectionCompleted,
            11 => Self::CounterRecorded,
            12 => Self::EventPublished,
            _ => return None,
        })
    }
}

/// Queue and segment state captured at T5 or T6.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PerfTraceSegment {
    /// Number of updates in the reducer segment, present only at T6.
    pub segment_len: Option<usize>,
    /// Updates waiting in the receiver after collection, present only at T6.
    pub receiver_len: Option<usize>,
    /// Exact retained items at the instant of this event.
    pub queue_depth: usize,
    /// Exact retained bytes at the instant of this event.
    pub queue_bytes: usize,
}

/// One timestamped stage for one correction identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PerfTraceEvent {
    /// Monotonic reservation order within this trace handle.
    pub sequence: usize,
    /// Replacement-tip identity used to join SDK and harness stages.
    pub new_tip_hash: B256,
    /// SDK stage recorded by this event.
    pub stage: PerfTraceStage,
    /// Monotonic process-local timestamp.
    pub recorded_at: Instant,
    /// Queue/segment metrics for T5/T6; absent for other stages.
    pub segment: Option<PerfTraceSegment>,
}

/// Immutable view of every fully published event in one trace handle.
#[derive(Clone, Debug)]
pub struct PerfTraceSnapshot {
    /// Events in reservation order. Duplicate hash/stage pairs are preserved.
    ///
    /// Concurrent recorders may reserve after a later logical stage has already
    /// captured its timestamp, so consumers must use `recorded_at` for timeline
    /// ordering rather than `sequence`.
    pub events: Vec<PerfTraceEvent>,
    /// Distinct correction hashes in events, including warmup corrections.
    pub correction_count: usize,
    /// True if the correction or fixed event capacity was exceeded.
    pub overflowed: bool,
    /// Events rejected after the fixed event array filled.
    pub dropped_events: usize,
    /// Bounded records for every reducer segment, including warmup segments.
    pub reducer_segments: PerfTraceReducerSegmentSnapshot,
}

/// Cloneable handle for one bounded in-memory diagnostic trace.
#[derive(Clone, Debug)]
pub struct PerfTrace {
    inner: Arc<PerfTraceInner>,
}

impl Default for PerfTrace {
    fn default() -> Self {
        Self::new()
    }
}

impl PerfTrace {
    /// Allocates one fixed-capacity trace without global registration.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(PerfTraceInner {
                epoch: Instant::now(),
                next: AtomicUsize::new(0),
                overflowed: AtomicBool::new(false),
                dropped_events: AtomicUsize::new(0),
                events: (0..PERF_TRACE_EVENT_CAPACITY)
                    .map(|_| RawPerfTraceEvent::new())
                    .collect(),
                reducer_segments: reducer_segment::PerfTraceReducerSegmentStorage::new(),
            }),
        }
    }

    /// Copies fully published events without resetting the live trace.
    pub fn snapshot(&self) -> PerfTraceSnapshot {
        let reserved = self
            .inner
            .next
            .load(Ordering::Acquire)
            .min(PERF_TRACE_EVENT_CAPACITY);
        let mut events = Vec::with_capacity(reserved);
        let mut corrections = HashSet::with_capacity(PERF_TRACE_CORRECTION_CAPACITY);
        for (sequence, raw) in self.inner.events[..reserved].iter().enumerate() {
            let Some(event) = raw.snapshot(sequence, self.inner.epoch) else {
                continue;
            };
            corrections.insert(event.new_tip_hash);
            events.push(event);
        }
        let correction_count = corrections.len();
        let reducer_segments = self.inner.reducer_segments.snapshot();
        PerfTraceSnapshot {
            events,
            correction_count,
            overflowed: self.inner.overflowed.load(Ordering::Acquire)
                || correction_count > PERF_TRACE_CORRECTION_CAPACITY
                || reducer_segments.overflowed,
            dropped_events: self.inner.dropped_events.load(Ordering::Acquire),
            reducer_segments,
        }
    }

    pub(crate) fn begin_reducer_segment(
        &self,
        first: &ChainUpdate,
        first_admitted_at: Instant,
        first_received_at: Instant,
        collect_started_at: Instant,
        receiver_len_before_collect: usize,
        queue_depth_at_start: usize,
        queue_bytes_at_start: usize,
    ) -> Option<PerfTraceReducerSegmentBuilder> {
        PerfTraceReducerSegmentBuilder::begin(
            self.clone(),
            first,
            first_admitted_at,
            first_received_at,
            collect_started_at,
            receiver_len_before_collect,
            queue_depth_at_start,
            queue_bytes_at_start,
        )
    }

    pub(crate) fn record(&self, new_tip_hash: B256, stage: PerfTraceStage) {
        self.record_at(new_tip_hash, stage, Instant::now());
    }

    pub(crate) fn record_at(
        &self,
        new_tip_hash: B256,
        stage: PerfTraceStage,
        recorded_at: Instant,
    ) {
        self.record_at_with_segment(new_tip_hash, stage, recorded_at, None);
    }

    pub(crate) fn record_at_with_segment(
        &self,
        new_tip_hash: B256,
        stage: PerfTraceStage,
        recorded_at: Instant,
        segment: Option<PerfTraceSegment>,
    ) {
        let sequence = self.inner.next.fetch_add(1, Ordering::Relaxed);
        let Some(slot) = self.inner.events.get(sequence) else {
            self.inner.overflowed.store(true, Ordering::Release);
            self.inner.dropped_events.fetch_add(1, Ordering::Relaxed);
            return;
        };
        slot.publish(
            new_tip_hash,
            stage,
            elapsed_nanos_at(self.inner.epoch, recorded_at),
            segment,
        );
    }
}

#[derive(Debug)]
struct PerfTraceInner {
    epoch: Instant,
    next: AtomicUsize,
    overflowed: AtomicBool,
    dropped_events: AtomicUsize,
    events: Vec<RawPerfTraceEvent>,
    reducer_segments: reducer_segment::PerfTraceReducerSegmentStorage,
}

#[derive(Debug)]
struct RawPerfTraceEvent {
    ready: AtomicBool,
    hash: [AtomicU64; 4],
    stage: AtomicU8,
    elapsed_nanos: AtomicU64,
    segment_len: AtomicUsize,
    receiver_len: AtomicUsize,
    queue_depth: AtomicUsize,
    queue_bytes: AtomicUsize,
}

impl RawPerfTraceEvent {
    fn new() -> Self {
        Self {
            ready: AtomicBool::new(false),
            hash: std::array::from_fn(|_| AtomicU64::new(0)),
            stage: AtomicU8::new(0),
            elapsed_nanos: AtomicU64::new(0),
            segment_len: AtomicUsize::new(UNSET_METRIC),
            receiver_len: AtomicUsize::new(UNSET_METRIC),
            queue_depth: AtomicUsize::new(UNSET_METRIC),
            queue_bytes: AtomicUsize::new(UNSET_METRIC),
        }
    }

    fn publish(
        &self,
        new_tip_hash: B256,
        stage: PerfTraceStage,
        elapsed_nanos: u64,
        segment: Option<PerfTraceSegment>,
    ) {
        for (word, bytes) in self
            .hash
            .iter()
            .zip(new_tip_hash.as_slice().chunks_exact(8))
        {
            word.store(
                u64::from_be_bytes(bytes.try_into().expect("hash word has eight bytes")),
                Ordering::Relaxed,
            );
        }
        self.stage.store(stage as u8, Ordering::Relaxed);
        self.elapsed_nanos.store(elapsed_nanos, Ordering::Relaxed);
        if let Some(segment) = segment {
            self.segment_len.store(
                segment.segment_len.unwrap_or(UNSET_METRIC),
                Ordering::Relaxed,
            );
            self.receiver_len.store(
                segment.receiver_len.unwrap_or(UNSET_METRIC),
                Ordering::Relaxed,
            );
            self.queue_depth
                .store(segment.queue_depth, Ordering::Relaxed);
            self.queue_bytes
                .store(segment.queue_bytes, Ordering::Relaxed);
        }
        self.ready.store(true, Ordering::Release);
    }

    fn snapshot(&self, sequence: usize, epoch: Instant) -> Option<PerfTraceEvent> {
        if !self.ready.load(Ordering::Acquire) {
            return None;
        }
        let mut hash = [0_u8; 32];
        for (bytes, word) in hash.chunks_exact_mut(8).zip(&self.hash) {
            bytes.copy_from_slice(&word.load(Ordering::Relaxed).to_be_bytes());
        }
        let stage = PerfTraceStage::from_raw(self.stage.load(Ordering::Relaxed))?;
        let segment_len = self.segment_len.load(Ordering::Relaxed);
        let receiver_len = self.receiver_len.load(Ordering::Relaxed);
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        let queue_bytes = self.queue_bytes.load(Ordering::Relaxed);
        let segment = (queue_depth != UNSET_METRIC && queue_bytes != UNSET_METRIC).then_some(
            PerfTraceSegment {
                segment_len: (segment_len != UNSET_METRIC).then_some(segment_len),
                receiver_len: (receiver_len != UNSET_METRIC).then_some(receiver_len),
                queue_depth,
                queue_bytes,
            },
        );
        Some(PerfTraceEvent {
            sequence,
            new_tip_hash: B256::new(hash),
            stage,
            recorded_at: epoch
                .checked_add(Duration::from_nanos(
                    self.elapsed_nanos.load(Ordering::Relaxed),
                ))
                .unwrap_or(epoch),
            segment,
        })
    }
}

fn elapsed_nanos_at(epoch: Instant, recorded_at: Instant) -> u64 {
    let elapsed = recorded_at
        .checked_duration_since(epoch)
        .unwrap_or_default();
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn correction_hash(update: &ChainUpdate) -> Option<B256> {
    let ChainUpdate::Correction(correction) = update else {
        return None;
    };
    correction.new_tip.cursor.block_hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_ordered_and_preserves_metrics() {
        let trace = PerfTrace::new();
        let hash = B256::new([7; 32]);
        trace.record(hash, PerfTraceStage::SourceItem);
        trace.record_at_with_segment(
            hash,
            PerfTraceStage::SegmentEntry,
            Instant::now(),
            Some(PerfTraceSegment {
                segment_len: Some(1),
                receiver_len: Some(9),
                queue_depth: 10,
                queue_bytes: 1_024,
            }),
        );

        let snapshot = trace.snapshot();
        assert!(!snapshot.overflowed);
        assert_eq!(snapshot.dropped_events, 0);
        assert_eq!(snapshot.correction_count, 1);
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(snapshot.events[0].sequence, 0);
        assert_eq!(snapshot.events[0].stage, PerfTraceStage::SourceItem);
        assert_eq!(
            snapshot.events[1].segment,
            Some(PerfTraceSegment {
                segment_len: Some(1),
                receiver_len: Some(9),
                queue_depth: 10,
                queue_bytes: 1_024,
            })
        );
    }

    #[test]
    fn supplied_timestamps_are_preserved_independently_of_reservation_order() {
        let trace = PerfTrace::new();
        let hash = B256::new([8; 32]);
        let earlier = trace
            .inner
            .epoch
            .checked_add(Duration::from_millis(1))
            .expect("test timestamp fits");
        let later = earlier
            .checked_add(Duration::from_millis(1))
            .expect("test timestamp fits");

        trace.record_at(hash, PerfTraceStage::SegmentEntry, later);
        trace.record_at(hash, PerfTraceStage::QueueAdmission, earlier);

        let snapshot = trace.snapshot();
        assert_eq!(snapshot.events[0].sequence, 0);
        assert_eq!(snapshot.events[0].recorded_at, later);
        assert_eq!(snapshot.events[1].sequence, 1);
        assert_eq!(snapshot.events[1].recorded_at, earlier);
        assert!(snapshot.events[1].recorded_at < snapshot.events[0].recorded_at);
    }

    #[test]
    fn event_capacity_overflow_is_hard_visible() {
        let trace = PerfTrace::new();
        for index in 0..=PERF_TRACE_EVENT_CAPACITY {
            trace.record(
                B256::new([(index % 251) as u8; 32]),
                PerfTraceStage::SourceItem,
            );
        }

        let snapshot = trace.snapshot();
        assert!(snapshot.overflowed);
        assert_eq!(snapshot.dropped_events, 1);
        assert_eq!(snapshot.events.len(), PERF_TRACE_EVENT_CAPACITY);
    }
}
