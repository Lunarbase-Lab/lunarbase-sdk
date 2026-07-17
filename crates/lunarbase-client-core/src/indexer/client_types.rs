/// Parameters shared by the high-level client lifecycle. The source is
/// started before the block-tagged snapshot so updates cannot be lost during
/// bootstrap. All queue and reconnect bounds are explicit.
#[derive(Clone, Debug)]
pub struct ClientConnectConfig {
    pub deployment: DeploymentConfig,
    pub filter: ContractFilter,
    pub lane_assets: Vec<Address>,
    pub routers: Vec<Address>,
    pub buffer_capacity: usize,
    pub reconnect_delay: Duration,
}

impl ClientConnectConfig {
    /// Validates deployment identity, source filtering, and lifecycle bounds
    /// before any background task is spawned.
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

/// Lock-free counters sampled by process metrics without touching reducer
/// state or adding contention to quote execution.
#[derive(Debug)]
struct ClientRuntimeStats {
    queue_depth: AtomicUsize,
    queue_capacity: usize,
    source_reconnects: AtomicU64,
    gaps: AtomicU64,
    recoveries: AtomicU64,
    recovery_failures: AtomicU64,
    checkpoint_commits: AtomicU64,
    checkpoint_failures: AtomicU64,
    checkpoint_latency_nanoseconds: AtomicU64,
}

impl ClientRuntimeStats {
    fn new(queue_capacity: usize) -> Self {
        Self {
            queue_depth: AtomicUsize::new(0),
            queue_capacity,
            source_reconnects: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
            recovery_failures: AtomicU64::new(0),
            checkpoint_commits: AtomicU64::new(0),
            checkpoint_failures: AtomicU64::new(0),
            checkpoint_latency_nanoseconds: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> ClientRuntimeStatsSnapshot {
        ClientRuntimeStatsSnapshot {
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_capacity: self.queue_capacity,
            source_reconnects: self.source_reconnects.load(Ordering::Relaxed),
            gaps: self.gaps.load(Ordering::Relaxed),
            recoveries: self.recoveries.load(Ordering::Relaxed),
            recovery_failures: self.recovery_failures.load(Ordering::Relaxed),
            checkpoint_commits: self.checkpoint_commits.load(Ordering::Relaxed),
            checkpoint_failures: self.checkpoint_failures.load(Ordering::Relaxed),
            checkpoint_latency_nanoseconds: self
                .checkpoint_latency_nanoseconds
                .load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time runtime counters exported by the service metrics endpoint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientRuntimeStatsSnapshot {
    pub queue_depth: usize,
    pub queue_capacity: usize,
    pub source_reconnects: u64,
    pub gaps: u64,
    pub recoveries: u64,
    pub recovery_failures: u64,
    pub checkpoint_commits: u64,
    pub checkpoint_failures: u64,
    pub checkpoint_latency_nanoseconds: u64,
}

/// Fully connected asynchronous client. The reducer remains single-writer;
/// quote callers only receive cloned/immutable state snapshots.
pub struct ConnectedQuoteClient {
    indexer: Arc<Mutex<QuoteIndexer>>,
    source: Arc<dyn ChainEventSource>,
    filter: ContractFilter,
    checkpoint_store: Option<SharedCheckpointStore>,
    ready: Arc<Notify>,
    available: Arc<AtomicBool>,
    cancel: watch::Sender<bool>,
    runtime_events: broadcast::Sender<ClientRuntimeEvent>,
    stats: Arc<ClientRuntimeStats>,
    stop: Mutex<Option<JoinHandle<()>>>,
    pump: Mutex<Option<JoinHandle<()>>>,
}
