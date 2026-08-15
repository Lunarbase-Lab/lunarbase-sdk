//! Two-phase source/reducer shutdown and final checkpoint capture.

use crate::indexer::client::{ConnectedQuoteClient, publish};
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::model::{Checkpoint, SourceError};
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
use tokio::{sync::broadcast, time::timeout};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

impl ConnectedQuoteClient {
    /// Cooperatively stops all workers.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_gracefully(DEFAULT_SHUTDOWN_TIMEOUT).await;
    }

    /// Revokes quote admission before asynchronous shutdown work starts.
    pub fn begin_shutdown(&self) {
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.available.store(false, Ordering::Release);
        self.shared.ready.notify_waiters();
    }

    /// Stops workers within `deadline` and guarantees no detached Tokio tasks.
    pub async fn shutdown_gracefully(&self, deadline: Duration) -> Result<(), IndexerError> {
        self.shutdown_gracefully_with_checkpoint(deadline)
            .await
            .map(|_| ())
    }

    /// Stops ingestion, drains accepted updates, and returns the final state.
    pub async fn shutdown_gracefully_with_checkpoint(
        &self,
        deadline: Duration,
    ) -> Result<Option<Checkpoint>, IndexerError> {
        let started = Instant::now();
        self.begin_shutdown();
        let _ = self.cancel.send(true);
        let (mut reducer, mut source_pump, mut freshness) = {
            let mut tasks = self.tasks.lock().map_err(|_| IndexerError::LockPoisoned)?;
            (
                tasks.reducer.take(),
                tasks.source_pump.take(),
                tasks.freshness.take(),
            )
        };
        let joined = timeout(deadline, async {
            let pump_result = match source_pump.as_mut() {
                Some(task) => collect_join("source-pump", task.await, &self.runtime_events),
                None => Ok(()),
            };
            let freshness_result = match freshness.as_mut() {
                Some(task) => collect_join("freshness-watchdog", task.await, &self.runtime_events),
                None => Ok(()),
            };
            let _ = self.reducer_cancel.send(true);
            let reducer_result = match reducer.as_mut() {
                Some(task) => collect_join("reducer", task.await, &self.runtime_events),
                None => Ok(()),
            };
            pump_result.and(freshness_result).and(reducer_result)
        })
        .await;
        match joined {
            Ok(Ok(())) => {
                let checkpoint = self
                    .shared
                    .indexer
                    .read()
                    .map_err(|_| IndexerError::LockPoisoned)?
                    .checkpoint();
                self.mark_indexer_stopped()?;
                Ok(checkpoint)
            }
            Ok(Err(error)) => {
                self.mark_indexer_stopped()?;
                Err(error)
            }
            Err(_) => {
                publish(&self.runtime_events, ClientRuntimeEvent::ShutdownTimedOut);
                let _ = self.reducer_cancel.send(true);
                if let Some(task) = &reducer {
                    task.abort();
                }
                if let Some(task) = &source_pump {
                    task.abort();
                }
                if let Some(task) = &freshness {
                    task.abort();
                }
                let remaining = deadline.saturating_sub(started.elapsed());
                let _ = timeout(remaining, async {
                    if let Some(task) = reducer.as_mut() {
                        let _ = task.await;
                    }
                    if let Some(task) = source_pump.as_mut() {
                        let _ = task.await;
                    }
                    if let Some(task) = freshness.as_mut() {
                        let _ = task.await;
                    }
                })
                .await;
                self.mark_indexer_stopped()?;
                Err(SourceError::Unavailable("graceful shutdown timed out".into()).into())
            }
        }
    }

    fn mark_indexer_stopped(&self) -> Result<(), IndexerError> {
        self.shared
            .indexer
            .write()
            .map_err(|_| IndexerError::LockPoisoned)?
            .shutdown();
        Ok(())
    }
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
