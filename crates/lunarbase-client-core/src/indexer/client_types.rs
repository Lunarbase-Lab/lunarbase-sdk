//! Connection configuration and shared runtime synchronization state.

use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::IndexerError;
use crate::model::{ContractFilter, DeploymentConfig, SourceError};
use std::sync::{
    RwLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Debug)]
/// Connection and bounded-queue settings for an embeddable client.
pub struct ClientConnectConfig {
    /// Immutable chain, Core contract, router, and endpoint identity.
    pub deployment: DeploymentConfig,
    /// Core address and topic set accepted by the realtime source.
    pub filter: ContractFilter,
    /// Maximum number of normalized updates waiting for the reducer.
    pub buffer_capacity: usize,
    /// Delay before reopening a failed realtime subscription.
    pub reconnect_delay: Duration,
}

impl ClientConnectConfig {
    /// Validates deployment identity and lifecycle bounds.
    pub fn validate(&self) -> Result<(), IndexerError> {
        self.deployment.validate()?;
        if self.filter.address != self.deployment.core {
            return Err(SourceError::NetworkMismatch.into());
        }
        if self.buffer_capacity == 0 || self.reconnect_delay.is_zero() {
            return Err(SourceError::Unavailable(
                "client buffer and reconnect bounds must be non-zero".into(),
            )
            .into());
        }
        Ok(())
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
    /// Publishes an initially ready quote indexer.
    pub(super) fn new(indexer: QuoteIndexer) -> Self {
        Self {
            indexer: RwLock::new(indexer),
            available: AtomicBool::new(true),
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
}
