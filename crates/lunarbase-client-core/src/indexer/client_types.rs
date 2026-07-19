use crate::indexer::errors::IndexerError;
use crate::model::{ContractFilter, DeploymentConfig, SourceError};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Clone, Debug)]
/// Connection and bounded-queue settings for an embeddable client.
pub struct ClientConnectConfig {
    pub deployment: DeploymentConfig,
    pub filter: ContractFilter,
    pub buffer_capacity: usize,
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
pub(super) struct ClientRuntimeStats {
    pub(super) queue_depth: AtomicUsize,
    queue_capacity: usize,
    pub(super) source_reconnects: AtomicU64,
    pub(super) gaps: AtomicU64,
    pub(super) recoveries: AtomicU64,
    pub(super) recovery_failures: AtomicU64,
}

impl ClientRuntimeStats {
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
    pub queue_depth: usize,
    pub queue_capacity: usize,
    pub source_reconnects: u64,
    pub gaps: u64,
    pub recoveries: u64,
    pub recovery_failures: u64,
}
