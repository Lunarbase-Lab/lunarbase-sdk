//! Background source and single-writer reducer tasks for a connected client.

use crate::indexer::client::publish;
use crate::indexer::client_types::{
    ClientConnectConfig, ClientRuntimeStats, SharedQuoteState, unix_millis,
};
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::model::{BackfillRequest, ChainUpdate, ContractFilter};
use crate::source::{ChainDataSource, SourceStream};
use crate::state::reducer::ReducerError;
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{sleep, timeout};

/// Operational event and counter handles used by the reducer task.
pub(super) struct ReducerRuntime {
    /// Bounded broadcast channel for operational lifecycle events.
    pub events: broadcast::Sender<ClientRuntimeEvent>,
    /// Lock-free runtime counters updated by the single reducer task.
    pub stats: Arc<ClientRuntimeStats>,
}

/// Source-specific timing, lifecycle, and observability handles.
pub(super) struct SourcePumpRuntime {
    /// Delay before opening another transport after a terminated attempt.
    pub reconnect_delay: Duration,
    /// Maximum interval without one normalized source update.
    pub stall_timeout: Duration,
    /// Handshake state observed by bootstrap and recovery.
    pub source_active: watch::Sender<bool>,
    /// Cooperative cancellation receiver owned by the source task.
    pub cancel: watch::Receiver<bool>,
    /// Bounded broadcast channel for operational lifecycle events.
    pub events: broadcast::Sender<ClientRuntimeEvent>,
    /// Lock-free queue, reconnect, and source-activity counters.
    pub stats: Arc<ClientRuntimeStats>,
}

pub(super) async fn source_pump<S>(
    source: Arc<S>,
    filter: ContractFilter,
    sender: mpsc::Sender<ChainUpdate>,
    runtime: SourcePumpRuntime,
) where
    S: ChainDataSource + 'static,
{
    let SourcePumpRuntime {
        reconnect_delay,
        stall_timeout,
        source_active,
        mut cancel,
        events,
        stats,
    } = runtime;
    let mut ever_active = false;
    loop {
        let _ = source_active.send(false);
        let stream = tokio::select! {
            biased;
            () = cancellation_requested(&mut cancel) => return,
            result = source.subscribe(filter.clone()) => result,
        };
        match stream {
            Ok(stream) => {
                ever_active = true;
                let _ = source_active.send(true);
                if !consume_stream(
                    stream,
                    &sender,
                    stall_timeout,
                    &source_active,
                    &mut cancel,
                    &events,
                    &stats,
                )
                .await
                {
                    return;
                }
            }
            Err(error) => {
                let detail = error.to_string();
                publish(
                    &events,
                    ClientRuntimeEvent::SourceSubscribeFailed {
                        detail: detail.clone(),
                    },
                );
                if ever_active
                    && !send_update(
                        &sender,
                        &mut cancel,
                        ChainUpdate::Gap {
                            cursor: None,
                            reason: format!("source subscribe failed: {detail}"),
                        },
                        &stats,
                    )
                    .await
                {
                    return;
                }
            }
        }
        let _ = source_active.send(false);
        stats.source_reconnects.fetch_add(1, Ordering::Relaxed);
        if sleep_or_cancel(reconnect_delay, &mut cancel).await {
            return;
        }
    }
}

async fn consume_stream(
    mut stream: SourceStream,
    sender: &mpsc::Sender<ChainUpdate>,
    stall_timeout: Duration,
    source_active: &watch::Sender<bool>,
    cancel: &mut watch::Receiver<bool>,
    events: &broadcast::Sender<ClientRuntimeEvent>,
    stats: &ClientRuntimeStats,
) -> bool {
    loop {
        let item = tokio::select! {
            biased;
            () = cancellation_requested(cancel) => return false,
            item = timeout(stall_timeout, stream.next()) => item,
        };
        let item = match item {
            Ok(item) => item,
            Err(_) => {
                let _ = source_active.send(false);
                publish(
                    events,
                    ClientRuntimeEvent::SourceStreamFailed {
                        detail: format!(
                            "source produced no updates for {} ms",
                            stall_timeout.as_millis()
                        ),
                    },
                );
                return send_update(
                    sender,
                    cancel,
                    ChainUpdate::Gap {
                        cursor: None,
                        reason: "realtime source stalled; canonical recovery required".into(),
                    },
                    stats,
                )
                .await;
            }
        };
        let Some(item) = item else {
            let _ = source_active.send(false);
            publish(events, ClientRuntimeEvent::SourceStreamClosed);
            return send_update(
                sender,
                cancel,
                ChainUpdate::Gap {
                    cursor: None,
                    reason: "source stream closed; canonical recovery required".into(),
                },
                stats,
            )
            .await;
        };
        let update = match item {
            Ok(update) => update,
            Err(error) => {
                let detail = error.to_string();
                publish(
                    events,
                    ClientRuntimeEvent::SourceStreamFailed {
                        detail: detail.clone(),
                    },
                );
                ChainUpdate::Gap {
                    cursor: None,
                    reason: format!("source stream failed: {detail}"),
                }
            }
        };
        let terminal = matches!(update, ChainUpdate::Gap { .. });
        if terminal {
            let _ = source_active.send(false);
        }
        if !send_update(sender, cancel, update, stats).await {
            return false;
        }
        if terminal {
            return true;
        }
    }
}

async fn send_update(
    sender: &mpsc::Sender<ChainUpdate>,
    cancel: &mut watch::Receiver<bool>,
    update: ChainUpdate,
    stats: &ClientRuntimeStats,
) -> bool {
    let permit = tokio::select! {
        biased;
        () = cancellation_requested(cancel) => return false,
        result = sender.reserve() => result,
    };
    let Ok(permit) = permit else {
        return false;
    };

    // Increment before publishing: `Permit::send` is synchronous, so the
    // receiver cannot decrement the counter before this update becomes visible.
    stats.queue_depth.fetch_add(1, Ordering::Relaxed);
    stats
        .last_source_update_unix_millis
        .store(unix_millis(), Ordering::Relaxed);
    permit.send(update);
    true
}

pub(super) async fn reducer_loop<S>(
    shared: Arc<SharedQuoteState>,
    source: Arc<S>,
    config: ClientConnectConfig,
    mut updates: mpsc::Receiver<ChainUpdate>,
    mut cancel: watch::Receiver<bool>,
    mut source_active: watch::Receiver<bool>,
    runtime: ReducerRuntime,
) where
    S: ChainDataSource + 'static,
{
    loop {
        let update = tokio::select! {
            biased;
            () = cancellation_requested(&mut cancel) => return,
            update = updates.recv() => update,
        };
        let Some(update) = update else {
            shared.available.store(false, Ordering::Release);
            shared.ready.notify_waiters();
            publish(
                &runtime.events,
                ClientRuntimeEvent::BackgroundTaskStopped { task: "reducer" },
            );
            return;
        };
        runtime.stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
        let result = shared
            .indexer
            .write()
            .map_err(|_| IndexerError::LockPoisoned)
            .and_then(|mut indexer| indexer.apply_core_update(update));
        if let Err(error) = result {
            shared.available.store(false, Ordering::Release);
            runtime.stats.gaps.fetch_add(1, Ordering::Relaxed);
            publish(
                &runtime.events,
                ClientRuntimeEvent::StateTransitionFailed {
                    detail: error.to_string(),
                },
            );
            if !recover_until_ready(
                shared.as_ref(),
                source.as_ref(),
                &config,
                &mut updates,
                &mut cancel,
                &mut source_active,
                &runtime,
            )
            .await
            {
                return;
            }
        }
    }
}

async fn recover_until_ready<S: ChainDataSource>(
    shared: &SharedQuoteState,
    source: &S,
    config: &ClientConnectConfig,
    updates: &mut mpsc::Receiver<ChainUpdate>,
    cancel: &mut watch::Receiver<bool>,
    source_active: &mut watch::Receiver<bool>,
    runtime: &ReducerRuntime,
) -> bool {
    loop {
        publish(&runtime.events, ClientRuntimeEvent::RecoveryStarted);
        if !wait_for_source_active(source_active, cancel).await {
            return false;
        }
        let snapshot = tokio::select! {
            biased;
            () = cancellation_requested(cancel) => return false,
            result = source.snapshot(&config.deployment) => result,
        };
        match snapshot {
            Ok(snapshot) => {
                let mut buffered = Vec::new();
                while let Ok(update) = updates.try_recv() {
                    runtime.stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
                    buffered.push(update);
                }
                let result = shared
                    .indexer
                    .write()
                    .map_err(|_| IndexerError::LockPoisoned)
                    .and_then(|mut indexer| indexer.bootstrap_normalized(snapshot, buffered));
                match result {
                    Ok(()) => {
                        runtime.stats.recoveries.fetch_add(1, Ordering::Relaxed);
                        shared.available.store(true, Ordering::Release);
                        shared.ready.notify_waiters();
                        publish(&runtime.events, ClientRuntimeEvent::RecoveryCompleted);
                        return true;
                    }
                    Err(error) => record_recovery_failure(error, &runtime.events, &runtime.stats),
                }
            }
            Err(error) => record_recovery_failure(error.into(), &runtime.events, &runtime.stats),
        }
        if sleep_or_cancel(config.reconnect_delay, cancel).await {
            return false;
        }
    }
}

fn record_recovery_failure(
    error: IndexerError,
    events: &broadcast::Sender<ClientRuntimeEvent>,
    stats: &ClientRuntimeStats,
) {
    stats.recovery_failures.fetch_add(1, Ordering::Relaxed);
    publish(
        events,
        ClientRuntimeEvent::RecoveryFailed {
            detail: error.to_string(),
        },
    );
}

pub(super) async fn recover_checkpoint<S: ChainDataSource>(
    indexer: &mut QuoteIndexer,
    source: &S,
    filter: &ContractFilter,
) -> Result<(), IndexerError> {
    let checkpoint_cursor = indexer
        .reducer
        .cursor()
        .cloned()
        .ok_or(IndexerError::NoCursor)?;
    indexer.reducer.mark_not_ready();
    let head = source.canonical_head().await?;
    if head.chain_id != checkpoint_cursor.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if head.block_number < checkpoint_cursor.block_number {
        return Err(IndexerError::Gap(
            "canonical head regressed below checkpoint".into(),
        ));
    }
    let from_block =
        if checkpoint_cursor.transaction_index.is_none() && checkpoint_cursor.log_index.is_none() {
            checkpoint_cursor.block_number.saturating_add(1)
        } else {
            checkpoint_cursor.block_number
        };
    if from_block <= head.block_number {
        let mut logs = source
            .backfill(BackfillRequest {
                from_block,
                to_block: head.block_number,
                filter: filter.clone(),
            })
            .await?;
        logs.sort_by_key(|log| log.cursor.event_order());
        for log in logs {
            if log.cursor.event_order() <= checkpoint_cursor.event_order() {
                continue;
            }
            indexer.apply_core_update(ChainUpdate::Log(log))?;
        }
    }
    indexer.apply_core_update(ChainUpdate::Head(head))?;
    indexer.reducer.publish_ready();
    Ok(())
}

async fn cancellation_requested(cancel: &mut watch::Receiver<bool>) {
    if *cancel.borrow() {
        return;
    }
    loop {
        if cancel.changed().await.is_err() || *cancel.borrow() {
            return;
        }
    }
}

async fn sleep_or_cancel(delay: Duration, cancel: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        biased;
        () = cancellation_requested(cancel) => true,
        () = sleep(delay) => false,
    }
}

pub(super) async fn wait_for_source_active(
    active: &mut watch::Receiver<bool>,
    cancel: &mut watch::Receiver<bool>,
) -> bool {
    loop {
        if *active.borrow() {
            return true;
        }
        tokio::select! {
            biased;
            () = cancellation_requested(cancel) => return false,
            changed = active.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::send_update;
    use crate::indexer::client_types::ClientRuntimeStats;
    use crate::model::ChainUpdate;
    use std::sync::atomic::Ordering;
    use tokio::sync::{mpsc, watch};

    #[tokio::test]
    async fn queue_depth_is_incremented_before_delivery() {
        let (sender, mut receiver) = mpsc::channel(1);
        let (_cancel_sender, mut cancel) = watch::channel(false);
        let stats = ClientRuntimeStats::new(1);
        let update = ChainUpdate::Gap {
            cursor: None,
            reason: "counter-order test".into(),
        };

        let (sent, observed_depth) =
            tokio::join!(send_update(&sender, &mut cancel, update, &stats), async {
                receiver.recv().await.expect("update is delivered");
                stats.queue_depth.load(Ordering::Relaxed)
            },);

        assert!(sent);
        assert_eq!(observed_depth, 1);
        stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(stats.queue_depth.load(Ordering::Relaxed), 0);
    }
}
