//! Connected client lifecycle, quote access, and graceful shutdown.

use crate::indexer::checkpoint_recovery::recover_checkpoint;
use crate::indexer::client_types::{
    ClientConnectConfig, ClientRuntimeStats, ClientRuntimeStatsSnapshot, CoreEventSink,
    CoreEventSinkPolicy, SharedQuoteState,
};
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::indexer::event_delivery::emit_handoff_events;
use crate::indexer::quote_types::{ClientBatchQuote, ClientQuote, IndexerHealth};
use crate::indexer::runtime_helpers::{freshness_watchdog, source_operation};
use crate::indexer::tasks::{
    ReducerRuntime, SourcePumpRuntime, reducer_loop, source_pump, wait_for_source_active,
};
use crate::model::{Checkpoint, Commitment, ContractLog, SourceError};
use crate::source::ChainDataSource;
use futures_util::FutureExt;
use lunarbase_math::{QuoteRequest, QuoteState};
use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const RUNTIME_EVENT_CAPACITY: usize = 256;
/// Fully connected client with a synchronous, shared-read quote path.
pub struct ConnectedQuoteClient {
    /// Shared quote state, lock-free readiness gate, and recovery notification.
    pub(super) shared: Arc<SharedQuoteState>,
    /// Cooperative cancellation signal shared by all background tasks.
    pub(super) cancel: watch::Sender<bool>,
    /// Reducer cancellation is delayed until the source pump has stopped.
    pub(super) reducer_cancel: watch::Sender<bool>,
    /// Bounded fan-out channel for operational runtime events.
    pub(super) runtime_events: broadcast::Sender<ClientRuntimeEvent>,
    /// Shared lock-free counters exposed through `runtime_stats`.
    stats: Arc<ClientRuntimeStats>,
    /// Sole ownership of both background task handles during shutdown.
    pub(super) tasks: Mutex<RuntimeTasks>,
}

impl ConnectedQuoteClient {
    /// Connects a source, optionally restores a checkpoint, and starts the
    /// ordered reducer loop.
    ///
    /// Realtime subscription starts before checkpoint validation or snapshot
    /// RPC so no update can be lost during the bootstrap handoff.
    pub async fn connect<S>(
        config: ClientConnectConfig,
        source: Arc<S>,
        optional_checkpoint: Option<Checkpoint>,
    ) -> Result<Self, IndexerError>
    where
        S: ChainDataSource + 'static,
    {
        Self::connect_inner(config, source, optional_checkpoint, None).await
    }

    /// Connects with an explicitly enabled, best-effort Core event observer.
    ///
    /// Delivery never waits for channel capacity and never affects quote
    /// readiness. Full or closed channels increment
    /// [`ClientRuntimeStatsSnapshot::event_observer_drops`]. Use the standalone
    /// durable event worker when logs must not be lost.
    pub async fn connect_with_event_sink<S>(
        config: ClientConnectConfig,
        source: Arc<S>,
        optional_checkpoint: Option<Checkpoint>,
        core_event_sink: mpsc::Sender<ContractLog>,
    ) -> Result<Self, IndexerError>
    where
        S: ChainDataSource + 'static,
    {
        Self::connect_with_event_sink_policy(
            config,
            source,
            optional_checkpoint,
            core_event_sink,
            CoreEventSinkPolicy::default(),
        )
        .await
    }

    /// Connects with a minimum commitment for the best-effort observer.
    ///
    /// Source logs below `policy.minimum_commitment` are not sent to the sink.
    /// A source must actually emit logs at the requested level; the client does
    /// not promote realtime cursors into canonical or finalized cursors.
    /// Observer delivery is nonblocking and is not a durability mechanism.
    pub async fn connect_with_event_sink_policy<S>(
        config: ClientConnectConfig,
        source: Arc<S>,
        optional_checkpoint: Option<Checkpoint>,
        core_event_sink: mpsc::Sender<ContractLog>,
        policy: CoreEventSinkPolicy,
    ) -> Result<Self, IndexerError>
    where
        S: ChainDataSource + 'static,
    {
        Self::connect_inner(
            config,
            source,
            optional_checkpoint,
            Some(CoreEventSink::new(core_event_sink, policy)),
        )
        .await
    }

    async fn connect_inner<S>(
        config: ClientConnectConfig,
        source: Arc<S>,
        optional_checkpoint: Option<Checkpoint>,
        core_event_sink: Option<CoreEventSink>,
    ) -> Result<Self, IndexerError>
    where
        S: ChainDataSource + 'static,
    {
        config.validate()?;
        if source.network() != config.deployment.network {
            return Err(SourceError::NetworkMismatch.into());
        }
        let shared = Arc::new(SharedQuoteState::new_not_ready(QuoteIndexer::new(
            QuoteState::default(),
            config.deployment.clone(),
        )));
        let (updates_tx, mut updates_rx) = mpsc::channel(config.buffer_capacity);
        let (cancel, pump_cancel) = watch::channel(false);
        let (reducer_cancel, reducer_cancel_rx) = watch::channel(false);
        let (source_active_tx, mut source_active_rx) = watch::channel(false);
        let (runtime_events, _) = broadcast::channel(RUNTIME_EVENT_CAPACITY);
        let stats = Arc::new(ClientRuntimeStats::new(
            config.buffer_capacity,
            config.buffer_byte_capacity,
        ));
        let recovery_event_sink = core_event_sink;
        let pump_future = source_pump(
            source.clone(),
            config.filter.clone(),
            updates_tx,
            SourcePumpRuntime {
                reconnect_delay: config.reconnect_delay,
                stall_timeout: config.source_stall_timeout,
                operation_timeout: config.source_operation_timeout,
                source_active: source_active_tx,
                cancel: pump_cancel,
                events: runtime_events.clone(),
                stats: stats.clone(),
            },
        );
        let pump = tokio::spawn(supervise_task(
            "source-pump",
            pump_future,
            shared.clone(),
            runtime_events.clone(),
            cancel.subscribe(),
        ));
        let mut bootstrap_pump = BootstrapPump::new(cancel.clone(), pump);
        let mut bootstrap_cancel = cancel.subscribe();
        await_source_active(
            &mut source_active_rx,
            &mut bootstrap_cancel,
            config.source_operation_timeout,
            "before subscription was established",
        )
        .await?;

        let mut checkpoint_recovered = false;
        let mut initial = if let Some(checkpoint) = optional_checkpoint {
            if checkpoint.is_compatible(&config.deployment)
                && source_operation(
                    "checkpoint validation",
                    config.source_operation_timeout,
                    source.validate_checkpoint(&checkpoint),
                )
                .await?
            {
                match QuoteIndexer::from_checkpoint(checkpoint, config.deployment.clone()) {
                    Ok(mut indexer) => {
                        match recover_checkpoint(
                            &mut indexer,
                            source.as_ref(),
                            &config.filter,
                            recovery_event_sink.as_ref(),
                            &stats,
                            config.source_operation_timeout,
                        )
                        .await
                        {
                            Ok(()) => {
                                checkpoint_recovered = true;
                                indexer
                            }
                            Err(_) => snapshot_indexer(source.as_ref(), &config).await?,
                        }
                    }
                    Err(_) => snapshot_indexer(source.as_ref(), &config).await?,
                }
            } else {
                snapshot_indexer(source.as_ref(), &config).await?
            }
        } else {
            snapshot_indexer(source.as_ref(), &config).await?
        };

        let mut buffered = Vec::new();
        while let Ok(update) = updates_rx.try_recv() {
            buffered.push(update.dequeue(&stats));
        }
        crate::indexer::engine::sort_chain_updates(&mut buffered);
        emit_handoff_events(
            &mut initial,
            &buffered,
            recovery_event_sink.as_ref(),
            checkpoint_recovered,
            &stats,
        )?;
        initial.apply_handoff(buffered)?;
        await_source_active(
            &mut source_active_rx,
            &mut bootstrap_cancel,
            config.source_operation_timeout,
            "during bootstrap",
        )
        .await?;

        *shared
            .indexer
            .write()
            .map_err(|_| IndexerError::LockPoisoned)? = initial;
        stats.record_state_update();
        shared.publish_available();
        let freshness = tokio::spawn(supervise_task(
            "freshness-watchdog",
            freshness_watchdog(
                shared.clone(),
                stats.clone(),
                config.source_stall_timeout,
                cancel.subscribe(),
            ),
            shared.clone(),
            runtime_events.clone(),
            cancel.subscribe(),
        ));
        let reducer_future = reducer_loop(
            shared.clone(),
            source,
            config,
            updates_rx,
            reducer_cancel_rx,
            source_active_rx,
            ReducerRuntime {
                events: runtime_events.clone(),
                stats: stats.clone(),
                core_event_sink: recovery_event_sink,
            },
        );
        let reducer = tokio::spawn(supervise_task(
            "reducer",
            reducer_future,
            shared.clone(),
            runtime_events.clone(),
            cancel.subscribe(),
        ));
        let source_pump = bootstrap_pump.disarm();

        Ok(Self {
            shared,
            cancel,
            reducer_cancel,
            runtime_events,
            stats,
            tasks: Mutex::new(RuntimeTasks {
                reducer: Some(reducer),
                source_pump,
                freshness: Some(freshness),
            }),
        })
    }

    /// Subscribes to bounded operational events without backpressuring state.
    pub fn subscribe_runtime_events(&self) -> broadcast::Receiver<ClientRuntimeEvent> {
        self.runtime_events.subscribe()
    }

    /// Waits until the current state reaches at least `minimum` commitment.
    pub async fn await_ready(&self, minimum: Commitment) -> Result<(), IndexerError> {
        let mut cancel = self.cancel.subscribe();
        loop {
            let notified = self.shared.ready.notified();
            let health = self.health()?;
            if health.ready && health.commitment >= minimum {
                return Ok(());
            }
            tokio::select! {
                () = notified => {}
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        return Err(IndexerError::NotReady);
                    }
                }
            }
        }
    }

    /// Evaluates one quote under a short shared read guard.
    pub fn quote(&self, request: &QuoteRequest) -> Result<ClientQuote, IndexerError> {
        if !self.is_ready() {
            return Err(IndexerError::NotReady);
        }
        let prepared = {
            let indexer = self
                .shared
                .indexer
                .read()
                .map_err(|_| IndexerError::LockPoisoned)?;
            indexer.prepare_quote()?
        };
        prepared.evaluate(request)
    }

    /// Evaluates all requests under one state/cursor snapshot.
    pub fn quote_many(&self, requests: &[QuoteRequest]) -> Result<ClientBatchQuote, IndexerError> {
        if !self.is_ready() {
            return Err(IndexerError::NotReady);
        }
        let prepared = {
            let indexer = self
                .shared
                .indexer
                .read()
                .map_err(|_| IndexerError::LockPoisoned)?;
            indexer.prepare_quote_many(requests)?
        };
        prepared.evaluate_many(requests)
    }

    /// Returns current readiness and execution context.
    pub fn health(&self) -> Result<IndexerHealth, IndexerError> {
        let mut health = self
            .shared
            .indexer
            .read()
            .map_err(|_| IndexerError::LockPoisoned)?
            .health();
        health.ready &= self.is_ready();
        Ok(health)
    }

    /// Returns a durable checkpoint. The clone is explicit and off hot path.
    pub fn checkpoint(&self) -> Result<Option<Checkpoint>, IndexerError> {
        if !self.is_ready() {
            return Ok(None);
        }
        Ok(self
            .shared
            .indexer
            .read()
            .map_err(|_| IndexerError::LockPoisoned)?
            .checkpoint())
    }

    /// Returns the atomic readiness gate used by HTTP probes.
    pub fn is_ready(&self) -> bool {
        self.shared.available.load(Ordering::Acquire)
    }

    /// Samples ingestion and recovery counters.
    pub fn runtime_stats(&self) -> ClientRuntimeStatsSnapshot {
        self.stats.snapshot()
    }
}

/// Background tasks owned and joined as one runtime lifecycle unit.
pub(super) struct RuntimeTasks {
    /// Ordered reducer task.
    pub(super) reducer: Option<JoinHandle<()>>,
    /// Realtime source subscription task.
    pub(super) source_pump: Option<JoinHandle<()>>,
    /// Reducer-state freshness monitor.
    pub(super) freshness: Option<JoinHandle<()>>,
}

/// Cancels and aborts the subscription pump when bootstrap is dropped.
///
/// This makes `connect` cancellation-safe, including SIGTERM received while a
/// block-tagged snapshot RPC is still in flight.
struct BootstrapPump {
    /// Cooperative cancellation signal sent if bootstrap exits early.
    cancel: watch::Sender<bool>,
    /// Realtime source task aborted unless ownership transfers to the client.
    handle: Option<JoinHandle<()>>,
}

impl BootstrapPump {
    fn new(cancel: watch::Sender<bool>, handle: JoinHandle<()>) -> Self {
        Self {
            cancel,
            handle: Some(handle),
        }
    }

    fn disarm(&mut self) -> Option<JoinHandle<()>> {
        self.handle.take()
    }
}

impl Drop for BootstrapPump {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = self.cancel.send(true);
            handle.abort();
        }
    }
}

async fn snapshot_indexer<S: ChainDataSource>(
    source: &S,
    config: &ClientConnectConfig,
) -> Result<QuoteIndexer, IndexerError> {
    let snapshot = source_operation(
        "bootstrap snapshot",
        config.source_operation_timeout,
        source.snapshot(&config.deployment),
    )
    .await?;
    let mut indexer = QuoteIndexer::new(QuoteState::default(), config.deployment.clone());
    indexer.bootstrap(snapshot)?;
    Ok(indexer)
}

async fn await_source_active(
    active: &mut watch::Receiver<bool>,
    cancel: &mut watch::Receiver<bool>,
    deadline: Duration,
    phase: &'static str,
) -> Result<(), IndexerError> {
    match timeout(deadline, wait_for_source_active(active, cancel)).await {
        Ok(true) => Ok(()),
        Ok(false) => {
            Err(SourceError::Unavailable(format!("realtime source stopped {phase}")).into())
        }
        Err(_) => Err(SourceError::Unavailable(format!(
            "realtime source was not active {phase} within {} ms",
            deadline.as_millis()
        ))
        .into()),
    }
}

async fn supervise_task<F>(
    name: &'static str,
    future: F,
    shared: Arc<SharedQuoteState>,
    events: broadcast::Sender<ClientRuntimeEvent>,
    cancel: watch::Receiver<bool>,
) where
    F: Future<Output = ()> + Send,
{
    let result = AssertUnwindSafe(future).catch_unwind().await;
    shared.available.store(false, Ordering::Release);
    shared.ready.notify_waiters();
    match result {
        Err(payload) => publish(
            &events,
            ClientRuntimeEvent::BackgroundTaskPanicked {
                task: name,
                detail: panic_detail(payload),
            },
        ),
        Ok(()) if !*cancel.borrow() => publish(
            &events,
            ClientRuntimeEvent::BackgroundTaskStopped { task: name },
        ),
        Ok(()) => {}
    }
}

fn panic_detail(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".into()
    }
}

pub(super) fn publish(sender: &broadcast::Sender<ClientRuntimeEvent>, event: ClientRuntimeEvent) {
    let _ = sender.send(event);
}

impl Drop for ConnectedQuoteClient {
    fn drop(&mut self) {
        self.shared.stopping.store(true, Ordering::Release);
        let _ = self.cancel.send(true);
        let _ = self.reducer_cancel.send(true);
        let tasks = match self.tasks.get_mut() {
            Ok(tasks) => tasks,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(task) = tasks.reducer.as_ref() {
            task.abort();
        }
        if let Some(task) = tasks.source_pump.as_ref() {
            task.abort();
        }
        if let Some(task) = tasks.freshness.as_ref() {
            task.abort();
        }
    }
}
