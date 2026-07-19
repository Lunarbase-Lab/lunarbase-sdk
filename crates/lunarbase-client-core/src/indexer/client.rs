use crate::indexer::client_types::{
    ClientConnectConfig, ClientRuntimeStats, ClientRuntimeStatsSnapshot,
};
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::indexer::quote_types::{ClientBatchQuote, ClientQuote, IndexerHealth};
use crate::indexer::tasks::{ReducerRuntime, recover_checkpoint, reducer_loop, source_pump};
use crate::model::{Checkpoint, Commitment, SourceError};
use crate::source::ChainDataSource;
use lunarbase_math::state::{QuoteRequest, QuoteState};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const RUNTIME_EVENT_CAPACITY: usize = 256;
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Fully connected client with a synchronous, shared-read quote path.
pub struct ConnectedQuoteClient {
    indexer: Arc<RwLock<QuoteIndexer>>,
    ready: Arc<Notify>,
    available: Arc<AtomicBool>,
    cancel: watch::Sender<bool>,
    runtime_events: broadcast::Sender<ClientRuntimeEvent>,
    stats: Arc<ClientRuntimeStats>,
    stop: Mutex<Option<JoinHandle<()>>>,
    pump: Mutex<Option<JoinHandle<()>>>,
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
        config.validate()?;
        if source.network() != config.deployment.network {
            return Err(SourceError::NetworkMismatch.into());
        }
        let (updates_tx, mut updates_rx) = mpsc::channel(config.buffer_capacity);
        let (cancel, pump_cancel) = watch::channel(false);
        let (runtime_events, _) = broadcast::channel(RUNTIME_EVENT_CAPACITY);
        let stats = Arc::new(ClientRuntimeStats::new(config.buffer_capacity));
        let pump = tokio::spawn(source_pump(
            source.clone(),
            config.filter.clone(),
            updates_tx,
            config.reconnect_delay,
            pump_cancel,
            runtime_events.clone(),
            stats.clone(),
        ));
        let mut bootstrap_pump = BootstrapPump::new(cancel.clone(), pump);

        let mut initial = if let Some(checkpoint) = optional_checkpoint {
            if checkpoint.is_compatible(&config.deployment)
                && source.validate_checkpoint(&checkpoint).await?
            {
                match QuoteIndexer::from_checkpoint(checkpoint, config.deployment.clone()) {
                    Ok(mut indexer) => {
                        if recover_checkpoint(&mut indexer, source.as_ref(), &config.filter)
                            .await
                            .is_ok()
                        {
                            indexer
                        } else {
                            snapshot_indexer(source.as_ref(), &config).await?
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
            stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
            buffered.push(update);
        }
        initial.apply_handoff(buffered)?;

        let indexer = Arc::new(RwLock::new(initial));
        let ready = Arc::new(Notify::new());
        let available = Arc::new(AtomicBool::new(true));
        ready.notify_waiters();
        let stop = tokio::spawn(reducer_loop(
            indexer.clone(),
            source,
            config,
            updates_rx,
            cancel.subscribe(),
            ReducerRuntime {
                ready: ready.clone(),
                available: available.clone(),
                events: runtime_events.clone(),
                stats: stats.clone(),
            },
        ));
        let pump = bootstrap_pump.disarm();

        Ok(Self {
            indexer,
            ready,
            available,
            cancel,
            runtime_events,
            stats,
            stop: Mutex::new(Some(stop)),
            pump: Mutex::new(pump),
        })
    }

    /// Subscribes to bounded operational events without backpressuring state.
    pub fn subscribe_runtime_events(&self) -> broadcast::Receiver<ClientRuntimeEvent> {
        self.runtime_events.subscribe()
    }

    /// Waits until the current state reaches at least `minimum` commitment.
    pub async fn await_ready(&self, minimum: Commitment) -> Result<(), IndexerError> {
        loop {
            let notified = self.ready.notified();
            let health = self.health()?;
            if health.ready && health.commitment >= minimum {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Evaluates one quote under a short shared read guard.
    pub fn quote(&self, request: &QuoteRequest) -> Result<ClientQuote, IndexerError> {
        if !self.is_ready() {
            return Err(IndexerError::NotReady);
        }
        self.indexer
            .read()
            .map_err(|_| IndexerError::LockPoisoned)?
            .quote(request)
    }

    /// Evaluates all requests under one state/cursor snapshot.
    pub fn quote_many(&self, requests: &[QuoteRequest]) -> Result<ClientBatchQuote, IndexerError> {
        if !self.is_ready() {
            return Err(IndexerError::NotReady);
        }
        self.indexer
            .read()
            .map_err(|_| IndexerError::LockPoisoned)?
            .quote_many(requests)
    }

    /// Returns current readiness and execution context.
    pub fn health(&self) -> Result<IndexerHealth, IndexerError> {
        let mut health = self
            .indexer
            .read()
            .map_err(|_| IndexerError::LockPoisoned)?
            .health();
        health.ready &= self.is_ready();
        Ok(health)
    }

    /// Returns a durable checkpoint. The clone is explicit and off hot path.
    pub fn checkpoint(&self) -> Result<Option<Checkpoint>, IndexerError> {
        Ok(self
            .indexer
            .read()
            .map_err(|_| IndexerError::LockPoisoned)?
            .checkpoint())
    }

    /// Returns the atomic readiness gate used by HTTP probes.
    pub fn is_ready(&self) -> bool {
        self.available.load(Ordering::Acquire)
    }

    /// Samples ingestion and recovery counters.
    pub fn runtime_stats(&self) -> ClientRuntimeStatsSnapshot {
        self.stats.snapshot()
    }

    /// Cooperatively stops all workers.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_gracefully(DEFAULT_SHUTDOWN_TIMEOUT).await;
    }

    /// Stops workers within `deadline` and guarantees no detached tasks.
    pub async fn shutdown_gracefully(&self, deadline: Duration) -> Result<(), IndexerError> {
        let started = Instant::now();
        self.available.store(false, Ordering::Release);
        let _ = self.cancel.send(true);
        self.indexer
            .write()
            .map_err(|_| IndexerError::LockPoisoned)?
            .shutdown();
        self.ready.notify_waiters();

        let mut stop = self.stop.lock().await.take();
        let mut pump = self.pump.lock().await.take();
        let joined = timeout(deadline, async {
            if let Some(task) = stop.as_mut() {
                collect_join("reducer", task.await, &self.runtime_events)?;
            }
            if let Some(task) = pump.as_mut() {
                collect_join("source-pump", task.await, &self.runtime_events)?;
            }
            Ok::<(), IndexerError>(())
        })
        .await;
        match joined {
            Ok(result) => result,
            Err(_) => {
                publish(&self.runtime_events, ClientRuntimeEvent::ShutdownTimedOut);
                if let Some(task) = &stop {
                    task.abort();
                }
                if let Some(task) = &pump {
                    task.abort();
                }
                let remaining = deadline.saturating_sub(started.elapsed());
                let _ = timeout(remaining, async {
                    if let Some(task) = stop.as_mut() {
                        let _ = task.await;
                    }
                    if let Some(task) = pump.as_mut() {
                        let _ = task.await;
                    }
                })
                .await;
                Err(SourceError::Unavailable("graceful shutdown timed out".into()).into())
            }
        }
    }
}

/// Cancels and aborts the subscription pump when bootstrap is dropped.
///
/// This makes `connect` cancellation-safe, including SIGTERM received while a
/// block-tagged snapshot RPC is still in flight.
struct BootstrapPump {
    cancel: watch::Sender<bool>,
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
    let snapshot = source.snapshot(&config.deployment).await?;
    let mut indexer = QuoteIndexer::new(QuoteState::default(), config.deployment.clone());
    indexer.bootstrap(snapshot)?;
    Ok(indexer)
}

fn collect_join(
    task: &'static str,
    result: Result<(), tokio::task::JoinError>,
    events: &broadcast::Sender<ClientRuntimeEvent>,
) -> Result<(), IndexerError> {
    if let Err(error) = result {
        publish(
            events,
            ClientRuntimeEvent::BackgroundTaskPanicked {
                task,
                detail: error.to_string(),
            },
        );
        return Err(SourceError::Unavailable(format!("{task} task failed: {error}")).into());
    }
    Ok(())
}

pub(super) fn publish(sender: &broadcast::Sender<ClientRuntimeEvent>, event: ClientRuntimeEvent) {
    let _ = sender.send(event);
}

impl Drop for ConnectedQuoteClient {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Ok(stop) = self.stop.try_lock()
            && let Some(task) = stop.as_ref()
        {
            task.abort();
        }
        if let Ok(pump) = self.pump.try_lock()
            && let Some(task) = pump.as_ref()
        {
            task.abort();
        }
    }
}
