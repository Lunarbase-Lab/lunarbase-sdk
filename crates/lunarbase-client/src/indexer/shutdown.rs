//! Two-phase source/reducer shutdown and final checkpoint capture.

use crate::indexer::client::{ConnectedQuoteClient, RuntimeTasks, publish};
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::model::{Checkpoint, SourceError};
use std::time::Duration;
use tokio::{sync::broadcast, task::JoinHandle, time::timeout};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

impl ConnectedQuoteClient {
    /// Cooperatively stops all workers.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_gracefully(DEFAULT_SHUTDOWN_TIMEOUT).await;
    }

    /// Revokes quote admission before asynchronous shutdown work starts.
    pub fn begin_shutdown(&self) {
        self.shared.stop();
    }

    /// Gives workers `deadline` to stop, then aborts and joins every remainder.
    /// A running bounded synchronous segment may extend the post-abort join.
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
        self.begin_shutdown();
        let _ = self.cancel.send(true);
        let mut tasks = self.tasks.lock().await;
        if let Some(completion) = tasks.completion.clone() {
            return completion;
        }
        if tasks.forced_abort {
            let _ = self.reducer_cancel.send(true);
            abort_and_join_remaining(&mut tasks, &self.runtime_events).await;
            return self.finish_shutdown(
                &mut tasks,
                Err(SourceError::Unavailable("graceful shutdown timed out".into()).into()),
            );
        }
        let joined = timeout(deadline, async {
            let pump_result =
                join_slot("source-pump", &mut tasks.source_pump, &self.runtime_events).await;
            let freshness_result = join_slot(
                "freshness-watchdog",
                &mut tasks.freshness,
                &self.runtime_events,
            )
            .await;
            let _ = self.reducer_cancel.send(true);
            let reducer_result =
                join_slot("reducer", &mut tasks.reducer, &self.runtime_events).await;
            pump_result.and(freshness_result).and(reducer_result)
        })
        .await;
        match joined {
            Ok(Ok(())) => {
                let outcome = self
                    .shared
                    .load_indexer()
                    .map(|indexer| indexer.checkpoint());
                self.finish_shutdown(&mut tasks, outcome)
            }
            Ok(Err(error)) => self.finish_shutdown(&mut tasks, Err(error)),
            Err(_) => {
                tasks.forced_abort = true;
                publish(&self.runtime_events, ClientRuntimeEvent::ShutdownTimedOut);
                let _ = self.reducer_cancel.send(true);
                abort_and_join_remaining(&mut tasks, &self.runtime_events).await;
                self.finish_shutdown(
                    &mut tasks,
                    Err(SourceError::Unavailable("graceful shutdown timed out".into()).into()),
                )
            }
        }
    }

    fn finish_shutdown(
        &self,
        tasks: &mut RuntimeTasks,
        outcome: Result<Option<Checkpoint>, IndexerError>,
    ) -> Result<Option<Checkpoint>, IndexerError> {
        let outcome = match self.mark_indexer_stopped() {
            Ok(()) => outcome,
            Err(error) => Err(error),
        };
        tasks.completion = Some(outcome.clone());
        outcome
    }

    fn mark_indexer_stopped(&self) -> Result<(), IndexerError> {
        self.shared.mutate_indexer(|indexer| indexer.shutdown())?;
        Ok(())
    }
}

async fn join_slot(
    name: &'static str,
    task: &mut Option<JoinHandle<()>>,
    events: &broadcast::Sender<ClientRuntimeEvent>,
) -> Result<(), IndexerError> {
    let result = match task.as_mut() {
        Some(handle) => handle.await,
        None => return Ok(()),
    };
    let _ = task.take();
    collect_join(name, result, events)
}

async fn abort_and_join_remaining(
    tasks: &mut RuntimeTasks,
    events: &broadcast::Sender<ClientRuntimeEvent>,
) {
    for task in [&tasks.reducer, &tasks.source_pump, &tasks.freshness]
        .into_iter()
        .flatten()
    {
        task.abort();
    }
    join_after_abort("reducer", &mut tasks.reducer, events).await;
    join_after_abort("source-pump", &mut tasks.source_pump, events).await;
    join_after_abort("freshness-watchdog", &mut tasks.freshness, events).await;
}

async fn join_after_abort(
    name: &'static str,
    task: &mut Option<JoinHandle<()>>,
    events: &broadcast::Sender<ClientRuntimeEvent>,
) {
    let result = match task.as_mut() {
        Some(handle) => handle.await,
        None => return,
    };
    let _ = task.take();
    if let Err(error) = result
        && !error.is_cancelled()
    {
        report_join_failure(name, &error, events);
    }
}

fn collect_join(
    task: &'static str,
    result: Result<(), tokio::task::JoinError>,
    events: &broadcast::Sender<ClientRuntimeEvent>,
) -> Result<(), IndexerError> {
    if let Err(error) = result {
        report_join_failure(task, &error, events);
        return Err(SourceError::Unavailable(format!("{task} task failed: {error}")).into());
    }
    Ok(())
}

fn report_join_failure(
    task: &'static str,
    error: &tokio::task::JoinError,
    events: &broadcast::Sender<ClientRuntimeEvent>,
) {
    publish(
        events,
        ClientRuntimeEvent::BackgroundTaskPanicked {
            task,
            detail: error.to_string(),
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::client_types::{ClientRuntimeStats, QueuedChainUpdate};
    use crate::model::ChainUpdate;
    use std::future::pending;
    use std::sync::{Arc, mpsc as std_mpsc};
    use tokio::sync::oneshot;

    struct DropNotice(Option<oneshot::Sender<()>>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            if let Some(notice) = self.0.take() {
                let _ = notice.send(());
            }
        }
    }

    fn pending_task(started: oneshot::Sender<()>, stopped: oneshot::Sender<()>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let _stopped = DropNotice(Some(stopped));
            let _ = started.send(());
            pending::<()>().await;
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeout_path_waits_for_sync_segment_and_releases_queue_ownership() {
        let stats = Arc::new(ClientRuntimeStats::new(1, 1024));
        let update = ChainUpdate::Gap {
            cursor: None,
            reason: "bounded sync shutdown segment".into(),
        };
        let bytes = QueuedChainUpdate::retained_bytes(&update);
        let permit = Arc::clone(&stats.queue_byte_budget)
            .acquire_many_owned(bytes.max(stats.queue_item_byte_floor) as u32)
            .await
            .unwrap();
        let queued = QueuedChainUpdate::new(update, bytes, permit, stats.queue_accounting());
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = std_mpsc::sync_channel(0);
        let reducer = tokio::spawn(async move {
            let _queued = queued;
            let _ = entered.send(());
            release_rx.recv().unwrap();
        });
        entered_rx.await.unwrap();

        let (source_started, source_started_rx) = oneshot::channel();
        let (source_stopped, source_stopped_rx) = oneshot::channel();
        let source_pump = pending_task(source_started, source_stopped);
        let (freshness_started, freshness_started_rx) = oneshot::channel();
        let (freshness_stopped, freshness_stopped_rx) = oneshot::channel();
        let freshness = pending_task(freshness_started, freshness_stopped);
        source_started_rx.await.unwrap();
        freshness_started_rx.await.unwrap();
        let (events, _) = broadcast::channel(4);
        let tasks = Arc::new(tokio::sync::Mutex::new(RuntimeTasks {
            reducer: Some(reducer),
            source_pump: Some(source_pump),
            freshness: Some(freshness),
            forced_abort: true,
            completion: None,
        }));
        let first_tasks = Arc::clone(&tasks);
        let first_events = events.clone();
        let first_shutdown = tokio::spawn(async move {
            let mut tasks = first_tasks.lock().await;
            abort_and_join_remaining(&mut tasks, &first_events).await;
        });
        source_stopped_rx.await.unwrap();
        freshness_stopped_rx.await.unwrap();

        assert!(!first_shutdown.is_finished());
        assert_eq!(stats.queue_depth(), 1);
        assert!(stats.queue_bytes() > 0);
        assert_eq!(stats.queue_byte_budget.available_permits(), 0);

        first_shutdown.abort();
        assert!(first_shutdown.await.unwrap_err().is_cancelled());
        {
            let tasks = tasks.lock().await;
            assert!(tasks.reducer.is_some());
            assert!(tasks.source_pump.is_some());
            assert!(tasks.freshness.is_some());
        }

        let second_tasks = Arc::clone(&tasks);
        let (resumed, resumed_rx) = oneshot::channel();
        let second_shutdown = tokio::spawn(async move {
            let mut tasks = second_tasks.lock().await;
            let _ = resumed.send(());
            abort_and_join_remaining(&mut tasks, &events).await;
        });
        resumed_rx.await.unwrap();
        assert!(!second_shutdown.is_finished());

        release.send(()).unwrap();
        timeout(Duration::from_secs(1), second_shutdown)
            .await
            .unwrap()
            .unwrap();
        let tasks = tasks.lock().await;
        assert!(
            tasks.reducer.is_none() && tasks.source_pump.is_none() && tasks.freshness.is_none()
        );
        assert_eq!(stats.queue_depth(), 0);
        assert_eq!(stats.queue_bytes(), 0);
        assert_eq!(stats.queue_byte_budget.available_permits(), 1024);
    }

    #[tokio::test]
    async fn concurrent_shutdown_owners_serialize_and_share_completion() {
        let (task_started, task_started_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let source_pump = tokio::spawn(async move {
            let _ = task_started.send(());
            release_rx.await.unwrap();
        });
        task_started_rx.await.unwrap();
        let tasks = Arc::new(tokio::sync::Mutex::new(RuntimeTasks {
            reducer: None,
            source_pump: Some(source_pump),
            freshness: None,
            forced_abort: false,
            completion: None,
        }));
        let (events, _) = broadcast::channel(4);
        let first_tasks = Arc::clone(&tasks);
        let first_events = events.clone();
        let (first_acquired, first_acquired_rx) = oneshot::channel();
        let first = tokio::spawn(async move {
            let mut tasks = first_tasks.lock().await;
            let _ = first_acquired.send(());
            join_slot("source-pump", &mut tasks.source_pump, &first_events)
                .await
                .unwrap();
            let outcome: Result<Option<Checkpoint>, IndexerError> = Ok(None);
            tasks.completion = Some(outcome.clone());
            outcome
        });
        first_acquired_rx.await.unwrap();

        let second_tasks = Arc::clone(&tasks);
        let (second_started, second_started_rx) = oneshot::channel();
        let (second_acquired, mut second_acquired_rx) = oneshot::channel();
        let second = tokio::spawn(async move {
            let _ = second_started.send(());
            let tasks = second_tasks.lock().await;
            let _ = second_acquired.send(());
            tasks.completion.clone().expect("first owner caches result")
        });
        second_started_rx.await.unwrap();
        assert!(matches!(
            second_acquired_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        release.send(()).unwrap();
        let first_result = first.await.unwrap();
        second_acquired_rx.await.unwrap();
        let second_result = second.await.unwrap();
        assert_eq!(first_result, second_result);
        let tasks = tasks.lock().await;
        assert!(tasks.source_pump.is_none());
        assert_eq!(tasks.completion, Some(first_result));
    }
}
