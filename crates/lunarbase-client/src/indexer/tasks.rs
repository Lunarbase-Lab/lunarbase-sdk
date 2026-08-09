//! Background source and single-writer reducer tasks for a connected client.

use crate::indexer::client::publish;
use crate::indexer::client_types::{
    ClientConnectConfig, ClientRuntimeStats, CoreEventSink, SharedQuoteState, unix_millis,
};
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::model::{BackfillRequest, ChainUpdate, Commitment, ContractFilter, ContractLog};
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
    /// Optional required, commitment-filtered Core event sink.
    pub core_event_sink: Option<CoreEventSink>,
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
        let result = apply_live_update(shared.as_ref(), update, &runtime).await;
        if let Err(error) = result {
            if matches!(error, IndexerError::EventSinkClosed) {
                shared.available.store(false, Ordering::Release);
                shared.ready.notify_waiters();
                publish(
                    &runtime.events,
                    ClientRuntimeEvent::BackgroundTaskStopped { task: "reducer" },
                );
                return;
            }
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

async fn apply_live_update(
    shared: &SharedQuoteState,
    update: ChainUpdate,
    runtime: &ReducerRuntime,
) -> Result<(), IndexerError> {
    let (apply_result, event_log) = {
        let mut indexer = shared
            .indexer
            .write()
            .map_err(|_| IndexerError::LockPoisoned)?;
        let event_log = if let ChainUpdate::Log(log) = &update {
            let covered = indexer.canonical_floor_covers_core_log(log)?;
            (!covered
                && runtime
                    .core_event_sink
                    .as_ref()
                    .is_some_and(|sink| sink.accepts(log.cursor.commitment)))
            .then(|| log.clone())
        } else {
            None
        };
        (indexer.apply_core_update(update), event_log)
    };
    if let Some(log) = event_log {
        send_required_core_event(runtime.core_event_sink.as_ref(), log).await?;
    }
    apply_result
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
    let recovery_from = match shared.indexer.read() {
        Ok(indexer) => indexer.reducer.cursor().cloned(),
        Err(_) => {
            record_recovery_failure(IndexerError::LockPoisoned, &runtime.events, &runtime.stats);
            return false;
        }
    };
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
                let backfill_logs = if let Some(recovery_from) = recovery_from.as_ref() {
                    match load_recovery_events(
                        source,
                        &config.filter,
                        recovery_from,
                        &snapshot.cursor,
                        runtime.core_event_sink.as_ref(),
                    )
                    .await
                    {
                        Ok(logs) => logs,
                        Err(error) => {
                            record_recovery_failure(error, &runtime.events, &runtime.stats);
                            if sleep_or_cancel(config.reconnect_delay, cancel).await {
                                return false;
                            }
                            continue;
                        }
                    }
                } else {
                    Vec::new()
                };
                let mut buffered = Vec::new();
                while let Ok(update) = updates.try_recv() {
                    runtime.stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
                    buffered.push(update);
                }
                let result = install_recovered_state(
                    shared,
                    snapshot,
                    buffered,
                    backfill_logs,
                    runtime.core_event_sink.as_ref(),
                )
                .await;
                match result {
                    Ok(()) => {
                        runtime.stats.recoveries.fetch_add(1, Ordering::Relaxed);
                        shared.available.store(true, Ordering::Release);
                        shared.ready.notify_waiters();
                        publish(&runtime.events, ClientRuntimeEvent::RecoveryCompleted);
                        return true;
                    }
                    Err(error) => {
                        let terminal = matches!(error, IndexerError::EventSinkClosed);
                        record_recovery_failure(error, &runtime.events, &runtime.stats);
                        if terminal {
                            return false;
                        }
                    }
                }
            }
            Err(error) => record_recovery_failure(error.into(), &runtime.events, &runtime.stats),
        }
        if sleep_or_cancel(config.reconnect_delay, cancel).await {
            return false;
        }
    }
}

async fn load_recovery_events<S: ChainDataSource>(
    source: &S,
    filter: &ContractFilter,
    from: &crate::model::ChainCursor,
    to: &crate::model::ChainCursor,
    core_event_sink: Option<&CoreEventSink>,
) -> Result<Vec<ContractLog>, IndexerError> {
    if core_event_sink.is_none_or(|sink| !sink.accepts(Commitment::Canonical)) {
        return Ok(Vec::new());
    }
    if from.chain_id != to.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if from.block_number > to.block_number {
        return Err(IndexerError::Gap(
            "canonical recovery head regressed below the pre-gap cursor".into(),
        ));
    }
    let mut logs = source
        .backfill(BackfillRequest {
            from_block: from.block_number,
            to_block: to.block_number,
            filter: filter.clone(),
        })
        .await?;
    logs.sort_by_key(|log| log.cursor.event_order());
    for log in &logs {
        if log.cursor.chain_id != to.chain_id {
            return Err(ReducerError::ChainIdMismatch.into());
        }
        if log.cursor.block_number < from.block_number
            || log.cursor.block_number > to.block_number
            || log.cursor.block_hash.is_none()
        {
            return Err(IndexerError::Gap(
                "canonical recovery backfill returned an out-of-range or hashless log".into(),
            ));
        }
    }
    Ok(logs)
}

async fn install_recovered_state(
    shared: &SharedQuoteState,
    snapshot: crate::bootstrap::BootstrapSnapshot,
    mut buffered: Vec<ChainUpdate>,
    backfill_logs: Vec<ContractLog>,
    core_event_sink: Option<&CoreEventSink>,
) -> Result<(), IndexerError> {
    crate::indexer::engine::sort_chain_updates(&mut buffered);

    // Validate the complete transition privately. Shared quote state remains
    // unavailable until every policy-accepted event crosses the bounded sink.
    let mut candidate = shared
        .indexer
        .read()
        .map_err(|_| IndexerError::LockPoisoned)?
        .clone();
    candidate.bootstrap_normalized(snapshot, buffered.clone())?;

    let mut ordered_logs = backfill_logs;
    ordered_logs.extend(buffered.iter().filter_map(|update| match update {
        ChainUpdate::Log(log) => Some(log.clone()),
        _ => None,
    }));
    ordered_logs.sort_by_key(|log| log.cursor.event_order());
    ordered_logs.dedup_by(|right, left| same_core_event_identity(left, right));
    for log in ordered_logs {
        send_required_core_event(core_event_sink, log).await?;
    }

    *shared
        .indexer
        .write()
        .map_err(|_| IndexerError::LockPoisoned)? = candidate;
    Ok(())
}

pub(super) async fn emit_handoff_events(
    indexer: &QuoteIndexer,
    buffered: &[ChainUpdate],
    core_event_sink: Option<&CoreEventSink>,
    skip_canonical_covered: bool,
) -> Result<(), IndexerError> {
    let mut logs = buffered
        .iter()
        .filter_map(|update| match update {
            ChainUpdate::Log(log) => Some(log.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    logs.sort_by_key(|log| log.cursor.event_order());
    logs.dedup_by(|right, left| same_core_event_identity(left, right));
    for log in logs {
        if skip_canonical_covered && indexer.canonical_floor_covers_core_log(&log)? {
            continue;
        }
        send_required_core_event(core_event_sink, log).await?;
    }
    Ok(())
}

fn same_core_event_identity(left: &ContractLog, right: &ContractLog) -> bool {
    left.address == right.address
        && left.transaction_hash == right.transaction_hash
        && left.topics == right.topics
        && left.data == right.data
        && left.removed == right.removed
        && left.cursor.chain_id == right.cursor.chain_id
        && left.cursor.block_hash == right.cursor.block_hash
        && left.cursor.event_order() == right.cursor.event_order()
}

async fn send_required_core_event(
    sink: Option<&CoreEventSink>,
    log: ContractLog,
) -> Result<(), IndexerError> {
    if let Some(sink) = sink
        && sink.accepts(log.cursor.commitment)
    {
        sink.sender
            .send(log)
            .await
            .map_err(|_| IndexerError::EventSinkClosed)?;
    }
    Ok(())
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
    core_event_sink: Option<&CoreEventSink>,
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
            send_required_core_event(core_event_sink, log.clone()).await?;
            indexer.apply_core_update(ChainUpdate::Log(log))?;
        }
    }
    indexer.apply_core_update(ChainUpdate::Head(head.clone()))?;
    indexer.set_canonical_floor(head);
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
#[path = "tasks_tests.rs"]
mod tests;
