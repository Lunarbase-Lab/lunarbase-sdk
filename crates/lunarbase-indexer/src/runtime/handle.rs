/// Process role exposed through readiness and metrics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeRole {
    Starting,
    Standby,
    Active,
    LeaseLost,
    Stopping,
}

impl RuntimeRole {
    /// Stable lowercase label for HTTP and Prometheus output.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Standby => "standby",
            Self::Active => "active",
            Self::LeaseLost => "lease_lost",
            Self::Stopping => "stopping",
        }
    }
}

/// Current process role and a concise operator-facing explanation.
#[derive(Clone, Debug)]
pub struct RuntimeStatus {
    pub role: RuntimeRole,
    pub detail: String,
}

#[derive(Clone)]
struct RuntimeState {
    status: RuntimeStatus,
    client: Option<Arc<ConnectedQuoteClient>>,
    retired_stats: lunarbase_client_core::ClientRuntimeStatsSnapshot,
}

/// Runtime-level lifecycle event. Client events are forwarded into the same
/// bounded channel so alerts and metrics have one process-wide event source.
#[derive(Clone, Debug)]
pub enum ServiceRuntimeEvent {
    Client(lunarbase_client_core::ClientRuntimeEvent),
    LeaseAcquired,
    LeaseAcquireFailed { detail: String },
    LeaseRenewFailed { detail: String },
    LeaseLost,
    LeaseReleaseFailed { detail: String },
    RuntimeConnectFailed { detail: String },
    RuntimeEventsLagged { skipped: u64 },
}

impl ServiceRuntimeEvent {
    /// Stable code used by alerts and counters.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Client(event) => event.code(),
            Self::LeaseAcquired => "writer_lease_acquired",
            Self::LeaseAcquireFailed { .. } => "writer_lease_acquire_failed",
            Self::LeaseRenewFailed { .. } => "writer_lease_renew_failed",
            Self::LeaseLost => "writer_lease_lost",
            Self::LeaseReleaseFailed { .. } => "writer_lease_release_failed",
            Self::RuntimeConnectFailed { .. } => "runtime_connect_failed",
            Self::RuntimeEventsLagged { .. } => "runtime_events_lagged",
        }
    }

    /// Human-readable context for logs and webhook alerts.
    pub fn detail(&self) -> String {
        match self {
            Self::Client(event) => event.detail(),
            Self::LeaseAcquired => "this replica became the active writer".into(),
            Self::LeaseAcquireFailed { detail }
            | Self::LeaseRenewFailed { detail }
            | Self::LeaseReleaseFailed { detail }
            | Self::RuntimeConnectFailed { detail } => detail.clone(),
            Self::RuntimeEventsLagged { skipped } => {
                format!("runtime event forwarding dropped {skipped} events")
            }
            Self::LeaseLost => {
                "writer lease ownership was lost; quote serving stopped immediately".into()
            }
        }
    }
}

/// Cheap cloneable view used by HTTP, metrics, and alert supervisors.
#[derive(Clone)]
pub struct RuntimeHandle {
    state: Arc<RwLock<RuntimeState>>,
    events: broadcast::Sender<ServiceRuntimeEvent>,
}

impl RuntimeHandle {
    /// Creates a handle in the fail-closed starting state.
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(SERVICE_EVENT_CAPACITY);
        Self {
            state: Arc::new(RwLock::new(RuntimeState {
                status: RuntimeStatus {
                    role: RuntimeRole::Starting,
                    detail: "runtime supervisor is starting".into(),
                },
                client: None,
                retired_stats: lunarbase_client_core::ClientRuntimeStatsSnapshot::default(),
            })),
            events,
        }
    }

    /// Returns the active client only while this replica owns the writer role.
    pub async fn client(&self) -> Option<Arc<ConnectedQuoteClient>> {
        self.state.read().await.client.clone()
    }

    /// Returns the current process role without touching network dependencies.
    pub async fn status(&self) -> RuntimeStatus {
        self.state.read().await.status.clone()
    }

    /// Subscribes to bounded process-wide lifecycle events.
    pub fn subscribe_events(&self) -> broadcast::Receiver<ServiceRuntimeEvent> {
        self.events.subscribe()
    }

    /// Returns counters accumulated across the current and all retired writer
    /// instances owned by this process.
    pub async fn runtime_stats(&self) -> lunarbase_client_core::ClientRuntimeStatsSnapshot {
        let state = self.state.read().await;
        let mut stats = state.retired_stats;
        if let Some(client) = &state.client {
            let current = client.runtime_stats();
            add_runtime_stats(&mut stats, current);
            stats.queue_depth = current.queue_depth;
            stats.queue_capacity = current.queue_capacity;
        } else {
            stats.queue_depth = 0;
        }
        stats
    }

    async fn transition(
        &self,
        role: RuntimeRole,
        detail: impl Into<String>,
        client: Option<Arc<ConnectedQuoteClient>>,
    ) {
        let mut state = self.state.write().await;
        if client.is_none() {
            if let Some(previous) = state.client.take() {
                let previous = previous.runtime_stats();
                add_runtime_stats(&mut state.retired_stats, previous);
                state.retired_stats.queue_depth = 0;
                state.retired_stats.queue_capacity = previous.queue_capacity;
            }
        }
        state.status = RuntimeStatus {
            role,
            detail: detail.into(),
        };
        state.client = client;
    }

    fn publish(&self, event: ServiceRuntimeEvent) {
        let _ = self.events.send(event);
    }
}

fn add_runtime_stats(
    total: &mut lunarbase_client_core::ClientRuntimeStatsSnapshot,
    value: lunarbase_client_core::ClientRuntimeStatsSnapshot,
) {
    total.source_reconnects = total
        .source_reconnects
        .saturating_add(value.source_reconnects);
    total.gaps = total.gaps.saturating_add(value.gaps);
    total.recoveries = total.recoveries.saturating_add(value.recoveries);
    total.recovery_failures = total
        .recovery_failures
        .saturating_add(value.recovery_failures);
    total.checkpoint_commits = total
        .checkpoint_commits
        .saturating_add(value.checkpoint_commits);
    total.checkpoint_failures = total
        .checkpoint_failures
        .saturating_add(value.checkpoint_failures);
    total.checkpoint_latency_nanoseconds = total
        .checkpoint_latency_nanoseconds
        .saturating_add(value.checkpoint_latency_nanoseconds);
}

