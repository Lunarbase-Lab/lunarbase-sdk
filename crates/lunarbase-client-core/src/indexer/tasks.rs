use super::client::publish;
use super::client_types::ClientRuntimeStats;
use super::{ClientConnectConfig, ClientRuntimeEvent, IndexerError, QuoteIndexer};
use crate::{BackfillRequest, ChainDataSource, ChainUpdate, ContractFilter, SourceStream};
use futures_util::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Notify, broadcast, mpsc, watch};
use tokio::time::sleep;

pub(super) struct ReducerRuntime {
    pub ready: Arc<Notify>,
    pub available: Arc<AtomicBool>,
    pub events: broadcast::Sender<ClientRuntimeEvent>,
    pub stats: Arc<ClientRuntimeStats>,
}

pub(super) async fn source_pump<S>(
    source: Arc<S>,
    filter: ContractFilter,
    sender: mpsc::Sender<ChainUpdate>,
    reconnect_delay: Duration,
    mut cancel: watch::Receiver<bool>,
    events: broadcast::Sender<ClientRuntimeEvent>,
    stats: Arc<ClientRuntimeStats>,
) where
    S: ChainDataSource + 'static,
{
    loop {
        let stream = tokio::select! {
            biased;
            () = cancellation_requested(&mut cancel) => return,
            result = source.subscribe(filter.clone()) => result,
        };
        match stream {
            Ok(stream) => {
                if !consume_stream(stream, &sender, &mut cancel, &events, &stats).await {
                    return;
                }
            }
            Err(error) => {
                stats.source_reconnects.fetch_add(1, Ordering::Relaxed);
                let detail = error.to_string();
                publish(
                    &events,
                    ClientRuntimeEvent::SourceSubscribeFailed {
                        detail: detail.clone(),
                    },
                );
                if !send_update(
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
        stats.source_reconnects.fetch_add(1, Ordering::Relaxed);
        if sleep_or_cancel(reconnect_delay, &mut cancel).await {
            return;
        }
    }
}

async fn consume_stream(
    mut stream: SourceStream,
    sender: &mpsc::Sender<ChainUpdate>,
    cancel: &mut watch::Receiver<bool>,
    events: &broadcast::Sender<ClientRuntimeEvent>,
    stats: &Arc<ClientRuntimeStats>,
) -> bool {
    loop {
        let item = tokio::select! {
            biased;
            () = cancellation_requested(cancel) => return false,
            item = stream.next() => item,
        };
        let Some(item) = item else {
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
    stats: &Arc<ClientRuntimeStats>,
) -> bool {
    let sent = tokio::select! {
        biased;
        () = cancellation_requested(cancel) => return false,
        result = sender.send(update) => result,
    };
    if sent.is_ok() {
        stats.queue_depth.fetch_add(1, Ordering::Relaxed);
        true
    } else {
        false
    }
}

pub(super) async fn reducer_loop<S>(
    indexer: Arc<RwLock<QuoteIndexer>>,
    source: Arc<S>,
    config: ClientConnectConfig,
    mut updates: mpsc::Receiver<ChainUpdate>,
    mut cancel: watch::Receiver<bool>,
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
            publish(
                &runtime.events,
                ClientRuntimeEvent::BackgroundTaskStopped { task: "reducer" },
            );
            return;
        };
        runtime.stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
        let result = indexer
            .write()
            .map_err(|_| IndexerError::LockPoisoned)
            .and_then(|mut indexer| indexer.apply_core_update(update));
        if let Err(error) = result {
            runtime.available.store(false, Ordering::Release);
            runtime.stats.gaps.fetch_add(1, Ordering::Relaxed);
            publish(
                &runtime.events,
                ClientRuntimeEvent::StateTransitionFailed {
                    detail: error.to_string(),
                },
            );
            if !recover_until_ready(
                &indexer,
                source.as_ref(),
                &config,
                &mut updates,
                &mut cancel,
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
    indexer: &Arc<RwLock<QuoteIndexer>>,
    source: &S,
    config: &ClientConnectConfig,
    updates: &mut mpsc::Receiver<ChainUpdate>,
    cancel: &mut watch::Receiver<bool>,
    runtime: &ReducerRuntime,
) -> bool {
    loop {
        publish(&runtime.events, ClientRuntimeEvent::RecoveryStarted);
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
                let result = indexer
                    .write()
                    .map_err(|_| IndexerError::LockPoisoned)
                    .and_then(|mut indexer| indexer.bootstrap_normalized(snapshot, buffered));
                match result {
                    Ok(()) => {
                        runtime.stats.recoveries.fetch_add(1, Ordering::Relaxed);
                        runtime.available.store(true, Ordering::Release);
                        runtime.ready.notify_waiters();
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
        return Err(crate::ReducerError::ChainIdMismatch.into());
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
