//! Connection configuration and shared runtime synchronization state.

#[path = "client_types/availability.rs"]
mod availability;
#[cfg(feature = "perf-trace")]
#[path = "client_types/perf_trace.rs"]
mod perf_trace;
#[path = "client_types/publication.rs"]
mod publication;
#[cfg(test)]
#[path = "client_types/publication_tests.rs"]
mod publication_tests;
use availability::{Availability, QuoteAdmission};

use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::IndexerError;
use crate::model::{
    ChainUpdate, Commitment, ContractFilter, ContractLog, DeploymentConfig,
    MIN_UPDATE_QUEUE_BYTE_CAPACITY, SourceError,
};
use crate::protocol::abi::quote_critical_topics;
use arc_swap::ArcSwap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;
const QUOTE_COOPERATIVE_WORK_BUDGET: usize = 1024;
thread_local!(static QUOTE_WORK_SINCE_PARK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) });

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};

#[derive(Clone, Debug)]
/// Connection and bounded-queue settings for an embeddable client.
pub struct ClientConnectConfig {
    /// Immutable chain, Core contract, router, and endpoint identity.
    pub deployment: DeploymentConfig,
    /// Core address; topics are empty or the complete quote-critical set.
    pub filter: ContractFilter,
    /// Maximum number of normalized updates waiting for the reducer.
    pub buffer_capacity: usize,
    /// Maximum retained bytes across normalized updates waiting for the reducer.
    pub buffer_byte_capacity: usize,
    /// Delay before reopening a failed realtime subscription.
    pub reconnect_delay: Duration,
    /// Maximum interval without any realtime update before readiness is revoked.
    pub source_stall_timeout: Duration,
    /// Maximum duration of one source subscribe, snapshot, or recovery operation.
    pub source_operation_timeout: Duration,
}

impl ClientConnectConfig {
    /// Validates deployment identity and lifecycle bounds.
    pub fn validate(&self) -> Result<(), IndexerError> {
        self.deployment.validate()?;
        if self.filter.address != self.deployment.core {
            return Err(SourceError::NetworkMismatch.into());
        }
        let expected_topics = quote_critical_topics();
        let topics = &self.filter.topics;
        let has_duplicate = topics
            .iter()
            .enumerate()
            .any(|(index, topic)| topics[..index].contains(topic));
        if !topics.is_empty()
            && (topics.len() != expected_topics.len()
                || has_duplicate
                || topics.iter().any(|topic| !expected_topics.contains(topic)))
        {
            return Err(SourceError::Unavailable(
                "filter topics must be empty or exactly match all quote-critical Core topics"
                    .into(),
            )
            .into());
        }
        if self.buffer_capacity == 0
            || self.buffer_byte_capacity < MIN_UPDATE_QUEUE_BYTE_CAPACITY
            || self.buffer_byte_capacity > u32::MAX as usize
            || self.reconnect_delay.is_zero()
            || self.source_stall_timeout.is_zero()
            || self.source_operation_timeout.is_zero()
        {
            return Err(SourceError::Unavailable(
                "client count/byte buffer and reconnect bounds must be valid; byte capacity must be at least 1024".into(),
            )
            .into());
        }
        Ok(())
    }
}

/// Selects which ordered Core logs are offered to the optional observer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoreEventSinkPolicy {
    /// Lowest source-provided commitment accepted by the observer.
    pub minimum_commitment: Commitment,
}

impl Default for CoreEventSinkPolicy {
    fn default() -> Self {
        Self {
            minimum_commitment: Commitment::Realtime,
        }
    }
}

impl CoreEventSinkPolicy {
    /// Returns whether a log at `commitment` should be forwarded.
    pub fn accepts(self, commitment: Commitment) -> bool {
        commitment >= self.minimum_commitment
    }
}

#[derive(Clone, Debug)]
pub(super) struct CoreEventSink {
    pub(super) sender: mpsc::Sender<ContractLog>,
    pub(super) policy: CoreEventSinkPolicy,
}

impl CoreEventSink {
    pub(super) fn new(sender: mpsc::Sender<ContractLog>, policy: CoreEventSinkPolicy) -> Self {
        Self { sender, policy }
    }

    pub(super) fn accepts(&self, commitment: Commitment) -> bool {
        self.policy.accepts(commitment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MATH_COMPATIBILITY_VERSION, Network};
    use lunarbase_math::{Address, B256};

    #[test]
    fn filter_accepts_only_empty_or_complete_quote_critical_topics() {
        let required = quote_critical_topics();

        let mut empty = config();
        empty.filter.topics.clear();
        assert!(empty.validate().is_ok());

        let mut reordered = config();
        reordered.filter.topics.reverse();
        assert!(reordered.validate().is_ok());

        let subset = required[..required.len() - 1].to_vec();
        let mut unknown = required.to_vec();
        *unknown.last_mut().unwrap() = B256::new([0x99; 32]);
        let mut duplicate = required.to_vec();
        *duplicate.last_mut().unwrap() = required[0];

        for topics in [subset, unknown, duplicate] {
            let mut invalid = config();
            invalid.filter.topics = topics;
            assert!(matches!(
                invalid.validate(),
                Err(IndexerError::Source(SourceError::Unavailable(ref detail)))
                    if detail == "filter topics must be empty or exactly match all quote-critical Core topics"
            ));
        }
    }

    fn config() -> ClientConnectConfig {
        let core = Address::new([1; 20]);
        ClientConnectConfig {
            deployment: DeploymentConfig {
                network: Network::Base,
                chain_id: 8453,
                core,
                fee_class: lunarbase_math::FeeClass::Whitelisted,
                verified_router: None,
                deployment_block: 1,
                expected_implementation: Address::new([3; 20]),
                expected_implementation_code_hash: B256::new([4; 32]),
                contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
                explicit_lane_assets: vec![Address::new([5; 20])],
            },
            filter: ContractFilter {
                address: core,
                topics: quote_critical_topics().to_vec(),
            },
            buffer_capacity: 16,
            buffer_byte_capacity: 1024 * 1024,
            reconnect_delay: Duration::from_millis(10),
            source_stall_timeout: Duration::from_secs(1),
            source_operation_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn event_sink_policy_accepts_only_requested_commitments() {
        let policy = CoreEventSinkPolicy {
            minimum_commitment: Commitment::Canonical,
        };
        assert!(!policy.accepts(Commitment::Realtime));
        assert!(policy.accepts(Commitment::Canonical));
        assert!(policy.accepts(Commitment::Finalized));

        assert!(CoreEventSinkPolicy::default().accepts(Commitment::Realtime));
    }
}

#[derive(Debug)]
/// Quote state and its publication gate shared by the client and reducer.
///
/// Each field retains its own synchronization semantics. Wrapping the
/// structure in one `Arc` shares ownership without introducing a structure-wide
/// lock.
pub(super) struct SharedQuoteState {
    /// Atomically published immutable quote snapshot.
    indexer: ArcSwap<QuoteIndexer>,
    /// Serializes publication and pairs each swap with its generation update.
    publication_writer: Mutex<()>,
    /// Monotonic identity sampled and advanced while holding the writer gate.
    publication_generation: AtomicU64,
    /// Versioned lock-free Invalid/Ready/Publishing admission state.
    availability: Availability,
    /// Notification used to wake commitment/readiness waiters after recovery.
    pub(super) ready: Notify,
}

impl SharedQuoteState {
    /// Creates an unpublished state used while subscription and snapshot bootstrap.
    pub(super) fn new_not_ready(indexer: QuoteIndexer) -> Self {
        Self {
            indexer: ArcSwap::from_pointee(indexer),
            publication_writer: Mutex::new(()),
            publication_generation: AtomicU64::new(0),
            availability: Availability::new(),
            ready: Notify::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn publication_generation(&self) -> u64 {
        self.publication_generation.load(Ordering::Acquire)
    }

    /// Publishes a recovered candidate only while its source-activity lease is
    /// still current.
    pub(super) fn publish_available_if(&self, lease: u64) -> bool {
        if self.availability.publish_if(lease) {
            self.ready.notify_waiters();
            true
        } else {
            false
        }
    }

    /// Returns whether quotes remain admitted in Ready or Publishing state.
    pub(super) fn is_available(&self) -> bool {
        self.availability.is_available()
    }

    /// Admits quotes lock-free while cooperatively prioritizing background work.
    pub(super) fn quote_path_available(&self, work_items: usize) -> bool {
        loop {
            match self.availability.quote_admission() {
                QuoteAdmission::Unavailable => return false,
                QuoteAdmission::Ready => {
                    if cooperative_quote_checkpoint(work_items) {
                        continue;
                    }
                    return true;
                }
                QuoteAdmission::Publishing => {
                    if self.publication_writer.is_poisoned() {
                        return false;
                    }
                    // Block briefly rather than leaving 128+ hot quote workers
                    // runnable while the reducer owns publication priority.
                    std::thread::park_timeout(Duration::from_micros(50));
                }
            }
        }
    }

    /// Captures the exact versioned state used by the freshness watchdog CAS.
    pub(super) fn availability_token(&self) -> u64 {
        self.availability.token()
    }

    /// Returns whether one captured token represents an in-flight correction.
    pub(super) const fn token_is_correcting(token: u64) -> bool {
        Availability::token_is_correcting(token)
    }

    /// Leases readiness to a private correction build without exposing NotReady.
    pub(super) fn begin_correction(&self) -> Option<u64> {
        self.availability.begin_correction()
    }

    /// Completes a correction only if shutdown/failure did not revoke its lease.
    pub(super) fn complete_correction(&self, correcting: u64) -> bool {
        self.availability.complete_correction(correcting)
    }

    /// Invalidates an abandoned correction lease without overwriting shutdown.
    pub(super) fn fail_correction(&self, correcting: u64) {
        if self.availability.fail_correction(correcting) {
            self.ready.notify_waiters();
        }
    }

    /// Revokes admission and advances the state version.
    pub(super) fn revoke_available(&self) {
        if self.availability.revoke() {
            self.ready.notify_waiters();
        }
    }

    /// Revokes admission and invalidates every in-flight source lease.
    pub(super) fn invalidate_source_lease(&self) {
        if self.availability.invalidate_source_lease() {
            self.ready.notify_waiters();
        }
    }

    /// Irreversibly closes quote admission for runtime shutdown.
    pub(super) fn stop(&self) {
        if self.availability.stop() {
            self.ready.notify_waiters();
        }
    }

    /// Leases exactly the Ready version sampled by the watchdog.
    pub(super) fn begin_expiration(&self, ready: u64) -> Option<u64> {
        self.availability.begin_expiration(ready)
    }

    /// Commits or restores a watchdog lease after rechecking progress.
    pub(super) fn finish_expiration(&self, expiring: u64, unchanged: bool) {
        if self.availability.finish_expiration(expiring, unchanged) {
            self.ready.notify_waiters();
        }
    }
}

#[derive(Debug)]
/// Lock-free counters shared by the source pump, reducer, and metrics reader.
///
/// Individual fields are atomic, but [`Self::snapshot`] is intentionally not a
/// transactional view across all counters. This is sufficient for operational
/// metrics and avoids a lock on the ingestion path.
pub(super) struct ClientRuntimeStats {
    /// Exact retained-item accounting shared with every queued owner.
    queue: Arc<QueueAccounting>,
    /// Immutable hard bound configured for the reducer queue.
    queue_capacity: usize,
    /// Minimum weighted-byte charge that also enforces the count bound.
    pub(super) queue_item_byte_floor: usize,
    /// Immutable hard byte bound configured for the reducer queue.
    pub(super) queue_byte_capacity: usize,
    /// Weighted permit pool shared by the sole source producer and receiver.
    pub(super) queue_byte_budget: Arc<Semaphore>,
    /// Number of source subscription reopen attempts.
    pub(super) source_reconnects: AtomicU64,
    /// Number of discontinuities that revoked quote readiness.
    pub(super) gaps: AtomicU64,
    /// Number of optimistic branch corrections published without downtime.
    pub(super) corrections: AtomicU64,
    /// Eventful blocks currently retained for bounded optimistic rollback.
    pub(super) correction_history_blocks: AtomicUsize,
    /// Conservative bytes currently retained by optimistic rollback history.
    pub(super) correction_history_bytes: AtomicUsize,
    /// Cumulative before-images evicted by correction-history budgets.
    pub(super) correction_history_evictions: AtomicU64,
    /// Number of canonical recoveries completed successfully.
    pub(super) recoveries: AtomicU64,
    /// Number of canonical recovery attempts that failed.
    pub(super) recovery_failures: AtomicU64,
    /// Logs dropped because the optional observer queue was full or closed.
    pub(super) event_observer_drops: AtomicU64,
    /// Unix milliseconds when the pump last delivered a normalized update.
    pub(super) last_source_update_unix_millis: AtomicU64,
    /// Unix milliseconds when the reducer last published verified quote state.
    pub(super) last_state_update_unix_millis: AtomicU64,
    /// Progress generation used only by the rare freshness-expiration path.
    pub(super) state_update_generation: AtomicU64,
    /// Default-off bounded correction trace used only by release diagnostics.
    #[cfg(feature = "perf-trace")]
    perf_trace: Option<crate::indexer::perf_trace::PerfTrace>,
}

impl ClientRuntimeStats {
    /// Creates zeroed counters for one bounded reducer queue.
    pub(super) fn new(queue_capacity: usize, queue_byte_capacity: usize) -> Self {
        Self {
            queue: Arc::new(QueueAccounting::default()),
            queue_capacity,
            queue_item_byte_floor: queue_byte_capacity.div_ceil(queue_capacity),
            queue_byte_capacity,
            queue_byte_budget: Arc::new(Semaphore::new(queue_byte_capacity)),
            source_reconnects: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
            corrections: AtomicU64::new(0),
            correction_history_blocks: AtomicUsize::new(0),
            correction_history_bytes: AtomicUsize::new(0),
            correction_history_evictions: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
            recovery_failures: AtomicU64::new(0),
            event_observer_drops: AtomicU64::new(0),
            last_source_update_unix_millis: AtomicU64::new(0),
            last_state_update_unix_millis: AtomicU64::new(0),
            state_update_generation: AtomicU64::new(0),
            #[cfg(feature = "perf-trace")]
            perf_trace: None,
        }
    }

    /// Records one verified state publication outside the quote hot path.
    pub(super) fn record_state_update(&self) {
        self.last_state_update_unix_millis
            .store(unix_millis(), Ordering::Relaxed);
        self.state_update_generation.fetch_add(1, Ordering::Release);
    }

    pub(super) fn queue_depth(&self) -> usize {
        self.queue.depth.load(Ordering::Relaxed)
    }

    pub(super) fn queue_bytes(&self) -> usize {
        self.queue.bytes.load(Ordering::Relaxed)
    }

    pub(super) fn queue_accounting(&self) -> Arc<QueueAccounting> {
        Arc::clone(&self.queue)
    }

    /// Samples each counter independently using relaxed atomic loads.
    pub(super) fn snapshot(&self) -> ClientRuntimeStatsSnapshot {
        ClientRuntimeStatsSnapshot {
            queue_depth: self.queue_depth(),
            queue_capacity: self.queue_capacity,
            queue_bytes: self.queue_bytes(),
            queue_byte_capacity: self.queue_byte_capacity,
            source_reconnects: self.source_reconnects.load(Ordering::Relaxed),
            gaps: self.gaps.load(Ordering::Relaxed),
            corrections: self.corrections.load(Ordering::Relaxed),
            correction_history_blocks: self.correction_history_blocks.load(Ordering::Relaxed),
            correction_history_bytes: self.correction_history_bytes.load(Ordering::Relaxed),
            correction_history_evictions: self.correction_history_evictions.load(Ordering::Relaxed),
            recoveries: self.recoveries.load(Ordering::Relaxed),
            recovery_failures: self.recovery_failures.load(Ordering::Relaxed),
            event_observer_drops: self.event_observer_drops.load(Ordering::Relaxed),
            last_source_update_unix_millis: self
                .last_source_update_unix_millis
                .load(Ordering::Relaxed),
            last_state_update_unix_millis: self
                .last_state_update_unix_millis
                .load(Ordering::Relaxed),
        }
    }
}

fn cooperative_quote_checkpoint(work_items: usize) -> bool {
    let should_park = QUOTE_WORK_SINCE_PARK.with(|since| {
        let total = since.get().saturating_add(work_items.max(1));
        if total < QUOTE_COOPERATIVE_WORK_BUDGET {
            since.set(total);
            false
        } else {
            since.set(total % QUOTE_COOPERATIVE_WORK_BUDGET);
            true
        }
    });
    if should_park {
        std::thread::park_timeout(Duration::from_micros(50));
    }
    should_park
}

#[derive(Debug, Default)]
pub(super) struct QueueAccounting {
    depth: AtomicUsize,
    bytes: AtomicUsize,
}

impl QueueAccounting {
    fn retain(&self, bytes: usize) {
        self.depth.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn release(&self, bytes: usize) {
        let previous_depth = self.depth.fetch_sub(1, Ordering::Relaxed);
        let previous_bytes = self.bytes.fetch_sub(bytes, Ordering::Relaxed);
        debug_assert!(previous_depth > 0, "queue depth accounting underflow");
        debug_assert!(previous_bytes >= bytes, "queue byte accounting underflow");
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Point-in-time runtime counters for metrics.
pub struct ClientRuntimeStatsSnapshot {
    /// Number of updates currently waiting in the reducer queue.
    pub queue_depth: usize,
    /// Configured hard bound of the reducer queue.
    pub queue_capacity: usize,
    /// Conservative retained bytes currently waiting in the reducer queue.
    pub queue_bytes: usize,
    /// Configured hard byte bound of the reducer queue.
    pub queue_byte_capacity: usize,
    /// Number of realtime subscription reopen attempts.
    pub source_reconnects: u64,
    /// Number of continuity gaps observed by the runtime.
    pub gaps: u64,
    /// Number of optimistic corrections published without revoking readiness.
    pub corrections: u64,
    /// Eventful blocks currently retained for optimistic rollback.
    pub correction_history_blocks: usize,
    /// Conservative retained bytes charged to optimistic rollback history.
    pub correction_history_bytes: usize,
    /// Cumulative history entries evicted by count or byte budgets.
    pub correction_history_evictions: u64,
    /// Number of canonical recoveries completed successfully.
    pub recoveries: u64,
    /// Number of canonical recovery attempts that failed.
    pub recovery_failures: u64,
    /// Logs dropped by the explicitly enabled best-effort event observer.
    pub event_observer_drops: u64,
    /// Unix milliseconds when a normalized source update was last queued.
    pub last_source_update_unix_millis: u64,
    /// Unix milliseconds when verified quote state last advanced or refreshed.
    pub last_state_update_unix_millis: u64,
}

/// One reducer-queue item that releases accounting and byte budget on drop.
pub(super) struct QueuedChainUpdate {
    update: Option<ChainUpdate>,
    bytes: usize,
    accounting: Arc<QueueAccounting>,
    correction_admission: Option<PendingCorrectionAdmission>,
    #[cfg(feature = "perf-trace")]
    admitted_at: std::time::Instant,
    #[cfg(feature = "perf-trace")]
    received_at: Option<std::time::Instant>,
    _byte_permit: OwnedSemaphorePermit,
}

impl QueuedChainUpdate {
    pub(super) fn new(
        update: ChainUpdate,
        bytes: usize,
        byte_permit: OwnedSemaphorePermit,
        accounting: Arc<QueueAccounting>,
    ) -> Self {
        debug_assert_eq!(bytes, Self::retained_bytes(&update));
        accounting.retain(bytes);
        Self {
            update: Some(update),
            bytes,
            accounting,
            correction_admission: None,
            #[cfg(feature = "perf-trace")]
            admitted_at: std::time::Instant::now(),
            #[cfg(feature = "perf-trace")]
            received_at: None,
            _byte_permit: byte_permit,
        }
    }

    pub(super) fn retained_bytes(update: &ChainUpdate) -> usize {
        update.retained_bytes().saturating_add(
            std::mem::size_of::<Self>().saturating_sub(std::mem::size_of::<ChainUpdate>()),
        )
    }

    pub(super) fn update(&self) -> &ChainUpdate {
        self.update.as_ref().expect("queued update is present")
    }

    pub(super) fn update_mut(&mut self) -> &mut ChainUpdate {
        self.update.as_mut().expect("queued update is present")
    }

    pub(super) fn dequeue(mut self) -> ChainUpdate {
        self.update.take().expect("queued update is present")
    }

    pub(super) fn with_correction_admission(
        mut self,
        admission: Option<PendingCorrectionAdmission>,
    ) -> Self {
        self.correction_admission = admission;
        self
    }

    pub(super) fn take_correction_admission(&mut self) -> Option<PendingCorrectionAdmission> {
        self.correction_admission.take()
    }
}

/// An enqueue-time quote-admission lease carried by one pending correction.
pub(super) struct PendingCorrectionAdmission {
    shared: Arc<SharedQuoteState>,
    token: u64,
    completed: bool,
}

impl PendingCorrectionAdmission {
    pub(super) fn begin(shared: Arc<SharedQuoteState>) -> Option<Self> {
        let token = shared.begin_correction()?;
        Some(Self {
            shared,
            token,
            completed: false,
        })
    }

    pub(super) fn token(&self) -> u64 {
        self.token
    }

    pub(super) fn belongs_to(&self, shared: &SharedQuoteState) -> bool {
        std::ptr::eq(self.shared.as_ref(), shared)
    }

    pub(super) fn disarm(mut self) {
        self.completed = true;
    }
}

impl Drop for PendingCorrectionAdmission {
    fn drop(&mut self) {
        if !self.completed {
            self.shared.fail_correction(self.token);
        }
    }
}

impl Drop for QueuedChainUpdate {
    fn drop(&mut self) {
        self.accounting.release(self.bytes);
    }
}

/// Returns a saturating wall-clock timestamp for operational age metrics.
pub(super) fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
