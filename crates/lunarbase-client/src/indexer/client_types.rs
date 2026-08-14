//! Connection configuration and shared runtime synchronization state.

use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::IndexerError;
use crate::model::{Commitment, ContractFilter, ContractLog, DeploymentConfig, SourceError};
use crate::protocol::abi::quote_critical_topics;
use std::sync::{
    RwLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, mpsc};

#[derive(Clone, Debug)]
/// Connection and bounded-queue settings for an embeddable client.
pub struct ClientConnectConfig {
    /// Immutable chain, Core contract, router, and endpoint identity.
    pub deployment: DeploymentConfig,
    /// Core address; topics are empty or the complete quote-critical set.
    pub filter: ContractFilter,
    /// Maximum number of normalized updates waiting for the reducer.
    pub buffer_capacity: usize,
    /// Delay before reopening a failed realtime subscription.
    pub reconnect_delay: Duration,
    /// Maximum interval without any realtime update before readiness is revoked.
    pub source_stall_timeout: Duration,
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
            || self.reconnect_delay.is_zero()
            || self.source_stall_timeout.is_zero()
        {
            return Err(SourceError::Unavailable(
                "client buffer and reconnect bounds must be non-zero".into(),
            )
            .into());
        }
        Ok(())
    }
}

/// Selects which ordered Core logs are forwarded to a required event sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoreEventSinkPolicy {
    /// Lowest source-provided commitment accepted by the event sink.
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
            reconnect_delay: Duration::from_millis(10),
            source_stall_timeout: Duration::from_secs(1),
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
    /// Hot quote state protected by short shared-read and exclusive-write guards.
    pub(super) indexer: RwLock<QuoteIndexer>,
    /// Lock-free readiness gate checked before entering the quote read path.
    pub(super) available: AtomicBool,
    /// Notification used to wake commitment/readiness waiters after recovery.
    pub(super) ready: Notify,
}

impl SharedQuoteState {
    /// Creates an unpublished state used while subscription and snapshot bootstrap.
    pub(super) fn new_not_ready(indexer: QuoteIndexer) -> Self {
        Self {
            indexer: RwLock::new(indexer),
            available: AtomicBool::new(false),
            ready: Notify::new(),
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
    /// Number of normalized updates currently waiting for the reducer.
    pub(super) queue_depth: AtomicUsize,
    /// Immutable hard bound configured for the reducer queue.
    queue_capacity: usize,
    /// Number of source subscription reopen attempts.
    pub(super) source_reconnects: AtomicU64,
    /// Number of discontinuities that revoked quote readiness.
    pub(super) gaps: AtomicU64,
    /// Number of canonical recoveries completed successfully.
    pub(super) recoveries: AtomicU64,
    /// Number of canonical recovery attempts that failed.
    pub(super) recovery_failures: AtomicU64,
    /// Unix milliseconds when the pump last delivered a normalized update.
    pub(super) last_source_update_unix_millis: AtomicU64,
}

impl ClientRuntimeStats {
    /// Creates zeroed counters for one bounded reducer queue.
    pub(super) fn new(queue_capacity: usize) -> Self {
        Self {
            queue_depth: AtomicUsize::new(0),
            queue_capacity,
            source_reconnects: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
            recovery_failures: AtomicU64::new(0),
            last_source_update_unix_millis: AtomicU64::new(0),
        }
    }

    /// Samples each counter independently using relaxed atomic loads.
    pub(super) fn snapshot(&self) -> ClientRuntimeStatsSnapshot {
        ClientRuntimeStatsSnapshot {
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_capacity: self.queue_capacity,
            source_reconnects: self.source_reconnects.load(Ordering::Relaxed),
            gaps: self.gaps.load(Ordering::Relaxed),
            recoveries: self.recoveries.load(Ordering::Relaxed),
            recovery_failures: self.recovery_failures.load(Ordering::Relaxed),
            last_source_update_unix_millis: self
                .last_source_update_unix_millis
                .load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Point-in-time runtime counters for metrics.
pub struct ClientRuntimeStatsSnapshot {
    /// Number of updates currently waiting in the reducer queue.
    pub queue_depth: usize,
    /// Configured hard bound of the reducer queue.
    pub queue_capacity: usize,
    /// Number of realtime subscription reopen attempts.
    pub source_reconnects: u64,
    /// Number of continuity gaps observed by the runtime.
    pub gaps: u64,
    /// Number of canonical recoveries completed successfully.
    pub recoveries: u64,
    /// Number of canonical recovery attempts that failed.
    pub recovery_failures: u64,
    /// Unix milliseconds when a normalized source update was last queued.
    pub last_source_update_unix_millis: u64,
}

/// Returns a saturating wall-clock timestamp for operational age metrics.
pub(super) fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
