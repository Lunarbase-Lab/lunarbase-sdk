//! Composition of the common runtime with one compiled network client.

use crate::config::ValidatedConfig;
use lunarbase_client_core::{
    CheckpointStore, ClientConnectConfig, ConnectedQuoteClient, ContractFilter, IndexerError,
    RedisCheckpointStore, RpcHttpClient, RpcSnapshotProvider, SharedCheckpointStore,
    MATH_COMPATIBILITY_VERSION,
};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{broadcast, watch, RwLock};
use tokio::time::{interval, sleep, MissedTickBehavior};

const SERVICE_EVENT_CAPACITY: usize = 512;

/// Failure while composing the selected network source and common runtime.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Indexer(#[from] IndexerError),
    #[error("Redis startup failed: {0}")]
    Redis(String),
    #[cfg(not(all(feature = "base", feature = "monad", feature = "arbitrum")))]
    #[error("network `{0}` was not compiled into this binary")]
    FeatureDisabled(&'static str),
}

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

/// Connects the selected source, snapshot provider, and persistence store.
pub async fn connect(
    config: &ValidatedConfig,
    store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    let rpc = RpcHttpClient::new(config.deployment.http_rpc_url.clone());
    let provider = RpcSnapshotProvider::new(rpc.clone(), config.snapshot_tag.clone());
    let connect = ClientConnectConfig {
        deployment: config.deployment.clone(),
        filter: ContractFilter {
            address: config.deployment.core,
            topics: Vec::new(),
        },
        lane_assets: config.deployment.explicit_lane_assets.clone(),
        routers: config.deployment.eager_routers.clone(),
        buffer_capacity: config.runtime.buffer_capacity,
        reconnect_delay: Duration::from_millis(config.runtime.reconnect_delay_milliseconds),
    };

    match config.deployment.network {
        lunarbase_client_core::Network::Base => {
            connect_base(config, rpc, &provider, connect, store).await
        }
        lunarbase_client_core::Network::Monad => {
            connect_monad(config, &provider, connect, store).await
        }
        lunarbase_client_core::Network::Arbitrum => {
            connect_arbitrum(config, rpc, &provider, connect, store).await
        }
    }
}

/// Opens and compatibility-checks the configured checkpoint store.
pub fn build_store(
    config: &ValidatedConfig,
) -> Result<Option<SharedCheckpointStore>, RuntimeError> {
    if !config.redis_enabled {
        return Ok(None);
    }
    let store = RedisCheckpointStore::connect_with_io_timeout(
        &config.deployment.redis.url,
        config.deployment.namespace(),
        config.deployment.redis.stream_max_len,
        config.deployment.redis.dedup_ttl_seconds,
        config.redis_io_timeout,
    )
    .map_err(|error| RuntimeError::Redis(error.to_string()))?;
    store.health().map_err(RuntimeError::Redis)?;
    if store.load_meta().map_err(RuntimeError::Redis)?.is_some()
        && !store
            .validate_meta(
                config.deployment.expected_runtime_code_hash,
                MATH_COMPATIBILITY_VERSION,
            )
            .map_err(RuntimeError::Redis)?
    {
        return Err(RuntimeError::Redis(
            "existing checkpoint metadata is incompatible".into(),
        ));
    }
    let store: Box<dyn CheckpointStore> = Box::new(store);
    Ok(Some(Arc::new(tokio::sync::Mutex::new(store))))
}

/// Runs active/standby election until shutdown. Losing or failing to renew the
/// lease clears the active client before any slow cleanup begins.
pub async fn supervise(
    config: &ValidatedConfig,
    store: Option<SharedCheckpointStore>,
    handle: RuntimeHandle,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    if !config.writer_lease.enabled {
        let Some(client) = connect_or_shutdown(config, store, &mut shutdown).await? else {
            handle
                .transition(
                    RuntimeRole::Stopping,
                    "shutdown requested during writer initialization",
                    None,
                )
                .await;
            return Ok(());
        };
        let client = Arc::new(client);
        return supervise_unleased(config, handle, client, &mut shutdown).await;
    }
    let store = store.ok_or_else(|| {
        RuntimeError::Redis("writer lease enabled without a checkpoint store".into())
    })?;

    handle
        .transition(
            RuntimeRole::Standby,
            "waiting to acquire the Redis writer lease",
            None,
        )
        .await;
    loop {
        if shutdown_is_requested(&shutdown) {
            handle
                .transition(RuntimeRole::Stopping, "shutdown requested", None)
                .await;
            return Ok(());
        }
        match lease_acquire(&store, &config.writer_lease.owner, config.writer_lease.ttl).await {
            Ok(true) => {
                configure_lease_fencing(&store, Some(&config.writer_lease.owner)).await?;
                handle.publish(ServiceRuntimeEvent::LeaseAcquired);
                match connect_or_shutdown(config, Some(store.clone()), &mut shutdown).await {
                    Ok(Some(client)) => {
                        let client = Arc::new(client);
                        let keep_running =
                            supervise_leased(config, &store, &handle, client, &mut shutdown)
                                .await?;
                        if !keep_running {
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        handle
                            .transition(
                                RuntimeRole::Stopping,
                                "shutdown requested during writer initialization",
                                None,
                            )
                            .await;
                        release_after_failed_start(config, &store, &handle).await;
                        return Ok(());
                    }
                    Err(error) => {
                        let detail = error.to_string();
                        handle.publish(ServiceRuntimeEvent::RuntimeConnectFailed {
                            detail: detail.clone(),
                        });
                        handle
                            .transition(
                                RuntimeRole::Standby,
                                format!("writer initialization failed; waiting to retry: {detail}"),
                                None,
                            )
                            .await;
                        release_after_failed_start(config, &store, &handle).await;
                    }
                }
            }
            Ok(false) => {
                handle
                    .transition(
                        RuntimeRole::Standby,
                        "another replica owns the Redis writer lease",
                        None,
                    )
                    .await;
            }
            Err(detail) => {
                handle.publish(ServiceRuntimeEvent::LeaseAcquireFailed {
                    detail: detail.clone(),
                });
                handle
                    .transition(
                        RuntimeRole::Standby,
                        format!("writer lease acquisition failed: {detail}"),
                        None,
                    )
                    .await;
            }
        }
        if sleep_or_shutdown(config.writer_lease.retry_interval, &mut shutdown).await {
            handle
                .transition(RuntimeRole::Stopping, "shutdown requested", None)
                .await;
            return Ok(());
        }
    }
}

async fn connect_or_shutdown(
    config: &ValidatedConfig,
    store: Option<SharedCheckpointStore>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<ConnectedQuoteClient>, RuntimeError> {
    tokio::select! {
        biased;
        () = shutdown_requested(shutdown) => Ok(None),
        result = connect(config, store) => result.map(Some),
    }
}

async fn supervise_unleased(
    config: &ValidatedConfig,
    handle: RuntimeHandle,
    client: Arc<ConnectedQuoteClient>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    let mut client_events = client.subscribe_runtime_events();
    handle
        .transition(
            RuntimeRole::Active,
            "active writer; distributed lease is disabled",
            Some(client.clone()),
        )
        .await;
    loop {
        tokio::select! {
            biased;
            () = shutdown_requested(shutdown) => break,
            event = client_events.recv() => forward_client_event(&handle, event),
        }
    }
    handle
        .transition(RuntimeRole::Stopping, "shutdown requested", None)
        .await;
    client
        .shutdown_gracefully(config.shutdown_timeout)
        .await
        .map_err(RuntimeError::from)
}

/// Returns `false` for process shutdown and `true` when the replica should
/// return to standby after lease loss.
async fn supervise_leased(
    config: &ValidatedConfig,
    store: &SharedCheckpointStore,
    handle: &RuntimeHandle,
    client: Arc<ConnectedQuoteClient>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool, RuntimeError> {
    let mut renew = interval(config.writer_lease.renew_interval);
    renew.set_missed_tick_behavior(MissedTickBehavior::Delay);
    renew.tick().await;
    let mut client_events = client.subscribe_runtime_events();
    handle
        .transition(
            RuntimeRole::Active,
            "this replica owns the Redis writer lease",
            Some(client.clone()),
        )
        .await;

    loop {
        tokio::select! {
            biased;
            () = shutdown_requested(shutdown) => {
                handle.transition(RuntimeRole::Stopping, "shutdown requested", None).await;
                let shutdown_result = client.shutdown_gracefully(config.shutdown_timeout).await;
                let release_result = lease_release(store, &config.writer_lease.owner).await;
                if let Err(detail) = release_result {
                    handle.publish(ServiceRuntimeEvent::LeaseReleaseFailed {
                        detail: detail.clone(),
                    });
                    return Err(RuntimeError::Redis(detail));
                }
                configure_lease_fencing(store, None).await?;
                shutdown_result.map_err(RuntimeError::from)?;
                return Ok(false);
            }
            _ = renew.tick() => {
                match lease_renew(store, &config.writer_lease.owner, config.writer_lease.ttl).await {
                    Ok(true) => {}
                    Ok(false) => {
                        handle.transition(
                            RuntimeRole::LeaseLost,
                            "Redis reports that this replica no longer owns the writer lease",
                            None,
                        ).await;
                        handle.publish(ServiceRuntimeEvent::LeaseLost);
                        let _ = client
                            .shutdown_after_lease_loss(config.shutdown_timeout)
                            .await;
                        configure_lease_fencing(store, None).await?;
                        handle.transition(
                            RuntimeRole::Standby,
                            "writer stopped after lease loss; waiting to reacquire",
                            None,
                        ).await;
                        return Ok(true);
                    }
                    Err(detail) => {
                        handle.transition(
                            RuntimeRole::LeaseLost,
                            format!("writer lease renewal failed: {detail}"),
                            None,
                        ).await;
                        handle.publish(ServiceRuntimeEvent::LeaseRenewFailed {
                            detail,
                        });
                        let _ = client
                            .shutdown_after_lease_loss(config.shutdown_timeout)
                            .await;
                        configure_lease_fencing(store, None).await?;
                        handle.transition(
                            RuntimeRole::Standby,
                            "writer stopped after lease renewal failure; waiting to reacquire",
                            None,
                        ).await;
                        return Ok(true);
                    }
                }
            }
            event = client_events.recv() => forward_client_event(handle, event),
        }
    }
}

fn forward_client_event(
    handle: &RuntimeHandle,
    event: Result<lunarbase_client_core::ClientRuntimeEvent, broadcast::error::RecvError>,
) {
    match event {
        Ok(event) => handle.publish(ServiceRuntimeEvent::Client(event)),
        Err(broadcast::error::RecvError::Lagged(skipped)) => {
            handle.publish(ServiceRuntimeEvent::RuntimeEventsLagged { skipped });
        }
        Err(broadcast::error::RecvError::Closed) => {}
    }
}

async fn release_after_failed_start(
    config: &ValidatedConfig,
    store: &SharedCheckpointStore,
    handle: &RuntimeHandle,
) {
    if let Err(detail) = lease_release(store, &config.writer_lease.owner).await {
        handle.publish(ServiceRuntimeEvent::LeaseReleaseFailed { detail });
    }
    let _ = configure_lease_fencing(store, None).await;
}

async fn configure_lease_fencing(
    store: &SharedCheckpointStore,
    owner: Option<&str>,
) -> Result<(), RuntimeError> {
    let store = Arc::clone(store);
    let owner = owner.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        store
            .blocking_lock_owned()
            .configure_writer_lease(owner.as_deref());
    })
    .await
    .map_err(|error| RuntimeError::Redis(format!("lease fencing worker failed: {error}")))
}

async fn lease_acquire(
    store: &SharedCheckpointStore,
    owner: &str,
    ttl: Duration,
) -> Result<bool, String> {
    lease_operation(store, owner, Some(ttl), LeaseOperation::Acquire).await
}

async fn lease_renew(
    store: &SharedCheckpointStore,
    owner: &str,
    ttl: Duration,
) -> Result<bool, String> {
    lease_operation(store, owner, Some(ttl), LeaseOperation::Renew).await
}

async fn lease_release(store: &SharedCheckpointStore, owner: &str) -> Result<(), String> {
    lease_operation(store, owner, None, LeaseOperation::Release)
        .await
        .map(|_| ())
}

#[derive(Clone, Copy)]
enum LeaseOperation {
    Acquire,
    Renew,
    Release,
}

async fn lease_operation(
    store: &SharedCheckpointStore,
    owner: &str,
    ttl: Option<Duration>,
    operation: LeaseOperation,
) -> Result<bool, String> {
    let store = Arc::clone(store);
    let owner = owner.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut store = store.blocking_lock_owned();
        match operation {
            LeaseOperation::Acquire => store
                .acquire_writer_lease(&owner, ttl.expect("acquire lease operation requires a TTL")),
            LeaseOperation::Renew => {
                store.renew_writer_lease(&owner, ttl.expect("renew lease operation requires a TTL"))
            }
            LeaseOperation::Release => {
                store.release_writer_lease(&owner)?;
                Ok(true)
            }
        }
    })
    .await
    .map_err(|error| format!("writer lease worker failed: {error}"))?
}

async fn sleep_or_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        biased;
        () = shutdown_requested(shutdown) => true,
        () = sleep(delay) => false,
    }
}

fn shutdown_is_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    if shutdown_is_requested(shutdown) {
        return;
    }
    loop {
        if shutdown.changed().await.is_err() || shutdown_is_requested(shutdown) {
            return;
        }
    }
}

#[cfg(feature = "base")]
async fn connect_base(
    config: &ValidatedConfig,
    rpc: RpcHttpClient,
    provider: &RpcSnapshotProvider,
    connect: ClientConnectConfig,
    store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    use lunarbase_client_base::{make_base_source, BaseFlashblocksBackend, BaseFlashblocksConfig};
    let backend = Arc::new(BaseFlashblocksBackend::with_config(
        rpc,
        BaseFlashblocksConfig {
            ws_url: config.deployment.realtime_source.clone(),
            max_frame_bytes: config.transport.max_frame_bytes,
            reorder_capacity: config.transport.reorder_capacity,
        },
        config.deployment.chain_id,
    ));
    let source = Arc::new(make_base_source(backend));
    Ok(ConnectedQuoteClient::connect_with_store(provider, source, connect, store).await?)
}

#[cfg(not(feature = "base"))]
async fn connect_base(
    _config: &ValidatedConfig,
    _rpc: RpcHttpClient,
    _provider: &RpcSnapshotProvider,
    _connect: ClientConnectConfig,
    _store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::FeatureDisabled("base"))
}

#[cfg(feature = "monad")]
async fn connect_monad(
    config: &ValidatedConfig,
    provider: &RpcSnapshotProvider,
    connect: ClientConnectConfig,
    store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    use lunarbase_client_monad::{MonadParserConfig, MonadParserSource, MonadRpcCanonicalBackend};
    let canonical = Arc::new(MonadRpcCanonicalBackend::new(
        config.deployment.http_rpc_url.clone(),
        config.deployment.chain_id,
    ));
    let source = Arc::new(
        MonadParserSource::new(
            MonadParserConfig {
                ws_url: config.deployment.realtime_source.clone(),
                core: config.deployment.core,
                chain_id: config.deployment.chain_id,
                max_frame_bytes: config.transport.max_frame_bytes,
            },
            canonical,
        )
        .map_err(IndexerError::from)?,
    );
    Ok(ConnectedQuoteClient::connect_with_store(provider, source, connect, store).await?)
}

#[cfg(not(feature = "monad"))]
async fn connect_monad(
    _config: &ValidatedConfig,
    _provider: &RpcSnapshotProvider,
    _connect: ClientConnectConfig,
    _store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::FeatureDisabled("monad"))
}

#[cfg(feature = "arbitrum")]
async fn connect_arbitrum(
    config: &ValidatedConfig,
    rpc: RpcHttpClient,
    provider: &RpcSnapshotProvider,
    connect: ClientConnectConfig,
    store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    use lunarbase_client_arbitrum::{make_arbitrum_source, ArbitrumNitroBackend};
    use lunarbase_client_core::WsRpcConfig;
    let mut backend = ArbitrumNitroBackend::with_config(
        rpc,
        config.deployment.realtime_source.clone(),
        config.deployment.chain_id,
        WsRpcConfig {
            max_frame_bytes: config.transport.max_frame_bytes,
            reorder_capacity: config.transport.reorder_capacity,
        },
    );
    if !config.transport.require_evm_parent_context {
        backend = backend.allow_missing_evm_parent_context();
    }
    let source = Arc::new(make_arbitrum_source(Arc::new(backend)));
    Ok(ConnectedQuoteClient::connect_with_store(provider, source, connect, store).await?)
}

#[cfg(not(feature = "arbitrum"))]
async fn connect_arbitrum(
    _config: &ValidatedConfig,
    _rpc: RpcHttpClient,
    _provider: &RpcSnapshotProvider,
    _connect: ClientConnectConfig,
    _store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::FeatureDisabled("arbitrum"))
}
