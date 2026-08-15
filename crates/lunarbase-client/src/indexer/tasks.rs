//! Background source and single-writer reducer tasks for a connected client.

use crate::indexer::client::publish;
use crate::indexer::client_types::{
    ClientConnectConfig, ClientRuntimeStats, CoreEventSink, QueuedChainUpdate, SharedQuoteState,
    unix_millis,
};
use crate::indexer::engine::{validate_core_log_identity, validate_core_recovery_log};
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::indexer::event_delivery::{same_core_event_identity, try_observe_core_event};
use crate::indexer::runtime_helpers::source_operation;
use crate::model::{BackfillRequest, ChainUpdate, Commitment, ContractFilter, ContractLog};
use crate::source::{ChainDataSource, SourceStream};
use crate::state::reducer::ReducerError;
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{sleep, timeout};

const RECOVERY_PAGE_BLOCKS: u64 = 1_000;
const LEGACY_RECOVERY_LOG_LIMIT: usize = 65_536;
const LEGACY_RECOVERY_BYTE_LIMIT: usize = 64 * 1024 * 1024;

/// Operational event and counter handles used by the reducer task.
pub(super) struct ReducerRuntime {
    /// Bounded broadcast channel for operational lifecycle events.
    pub events: broadcast::Sender<ClientRuntimeEvent>,
    /// Lock-free runtime counters updated by the single reducer task.
    pub stats: Arc<ClientRuntimeStats>,
    /// Optional nonblocking, commitment-filtered Core event observer.
    pub core_event_sink: Option<CoreEventSink>,
}

/// Source-specific timing, lifecycle, and observability handles.
pub(super) struct SourcePumpRuntime {
    /// Delay before opening another transport after a terminated attempt.
    pub reconnect_delay: Duration,
    /// Maximum interval without one normalized source update.
    pub stall_timeout: Duration,
    /// Maximum duration of one subscription handshake.
    pub operation_timeout: Duration,
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
    sender: mpsc::Sender<QueuedChainUpdate>,
    runtime: SourcePumpRuntime,
) where
    S: ChainDataSource + 'static,
{
    let SourcePumpRuntime {
        reconnect_delay,
        stall_timeout,
        operation_timeout,
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
            result = source_operation(
                "subscription",
                operation_timeout,
                source.subscribe(filter.clone()),
            ) => result,
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
    sender: &mpsc::Sender<QueuedChainUpdate>,
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
    sender: &mpsc::Sender<QueuedChainUpdate>,
    cancel: &mut watch::Receiver<bool>,
    update: ChainUpdate,
    stats: &ClientRuntimeStats,
) -> bool {
    let mut update = update;
    let mut bytes = update.retained_bytes();
    if bytes > stats.queue_byte_capacity {
        update = ChainUpdate::Gap {
            cursor: None,
            reason: "source update exceeded reducer queue byte budget; canonical recovery required"
                .into(),
        };
        bytes = update.retained_bytes();
    }
    let byte_permit = tokio::select! {
        biased;
        () = cancellation_requested(cancel) => return false,
        result = stats.queue_byte_budget.clone().acquire_many_owned(bytes.max(1) as u32) => result,
    };
    let Ok(byte_permit) = byte_permit else {
        return false;
    };
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
    stats.queue_bytes.fetch_add(bytes, Ordering::Relaxed);
    stats
        .last_source_update_unix_millis
        .store(unix_millis(), Ordering::Relaxed);
    permit.send(QueuedChainUpdate::new(update, bytes, byte_permit));
    true
}

pub(super) async fn reducer_loop<S>(
    shared: Arc<SharedQuoteState>,
    source: Arc<S>,
    config: ClientConnectConfig,
    mut updates: mpsc::Receiver<QueuedChainUpdate>,
    mut cancel: watch::Receiver<bool>,
    mut source_active: watch::Receiver<bool>,
    runtime: ReducerRuntime,
) where
    S: ChainDataSource + 'static,
{
    loop {
        let update = tokio::select! {
            biased;
            () = cancellation_requested(&mut cancel) => {
                drain_pending_updates(shared.as_ref(), &mut updates, &runtime).await;
                return;
            }
            update = updates.recv() => update,
        };
        let Some(update) = update else {
            shared.available.store(false, Ordering::Release);
            shared.ready.notify_waiters();
            return;
        };
        let update = update.dequeue(&runtime.stats);
        let result = apply_live_update(shared.as_ref(), update, &runtime);
        if let Err(error) = result {
            record_transition_failure(shared.as_ref(), error, &runtime);
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

fn apply_live_update(
    shared: &SharedQuoteState,
    update: ChainUpdate,
    runtime: &ReducerRuntime,
) -> Result<(), IndexerError> {
    let event_log = {
        let mut indexer = shared
            .indexer
            .write()
            .map_err(|_| IndexerError::LockPoisoned)?;
        match update {
            ChainUpdate::Log(log)
                if runtime
                    .core_event_sink
                    .as_ref()
                    .is_some_and(|sink| sink.accepts(log.cursor.commitment)) =>
            {
                indexer.apply_core_log_for_delivery(log)?
            }
            update => {
                indexer.apply_core_update(update)?;
                None
            }
        }
    };
    if let Some(log) = event_log {
        try_observe_core_event(runtime.core_event_sink.as_ref(), log, &runtime.stats);
    }
    runtime.stats.record_state_update();
    shared.publish_available();
    Ok(())
}

async fn drain_pending_updates(
    shared: &SharedQuoteState,
    updates: &mut mpsc::Receiver<QueuedChainUpdate>,
    runtime: &ReducerRuntime,
) {
    let mut state_valid = true;
    while let Some(queued) = updates.recv().await {
        let update = queued.dequeue(&runtime.stats);
        if state_valid && let Err(error) = apply_live_update(shared, update, runtime) {
            record_transition_failure(shared, error, runtime);
            state_valid = false;
        }
    }
}

fn record_transition_failure(
    shared: &SharedQuoteState,
    error: IndexerError,
    runtime: &ReducerRuntime,
) {
    shared.available.store(false, Ordering::Release);
    runtime.stats.gaps.fetch_add(1, Ordering::Relaxed);
    publish(
        &runtime.events,
        ClientRuntimeEvent::StateTransitionFailed {
            detail: error.to_string(),
        },
    );
}

async fn recover_until_ready<S: ChainDataSource>(
    shared: &SharedQuoteState,
    source: &S,
    config: &ClientConnectConfig,
    updates: &mut mpsc::Receiver<QueuedChainUpdate>,
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
            result = source_operation(
                "recovery snapshot",
                config.source_operation_timeout,
                source.snapshot(&config.deployment),
            ) => result,
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
                        config.source_operation_timeout,
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
                    buffered.push(update.dequeue(&runtime.stats));
                }
                let result = install_recovered_state(
                    shared,
                    snapshot,
                    buffered,
                    backfill_logs,
                    runtime.core_event_sink.as_ref(),
                    &runtime.stats,
                );
                match result {
                    Ok(()) => {
                        runtime.stats.recoveries.fetch_add(1, Ordering::Relaxed);
                        runtime.stats.record_state_update();
                        shared.publish_available();
                        publish(&runtime.events, ClientRuntimeEvent::RecoveryCompleted);
                        return true;
                    }
                    Err(error) => {
                        record_recovery_failure(error, &runtime.events, &runtime.stats);
                    }
                }
            }
            Err(error) => record_recovery_failure(error, &runtime.events, &runtime.stats),
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
    operation_timeout: Duration,
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
    let mut logs = Vec::new();
    let mut retained_bytes = 0_usize;
    let mut page_start = from.block_number;
    loop {
        let page_end = recovery_page_end(page_start, to.block_number);
        let mut page = source_operation(
            "recovery backfill",
            operation_timeout,
            source.backfill(BackfillRequest {
                from_block: page_start,
                to_block: page_end,
                filter: filter.clone(),
            }),
        )
        .await?;
        page.sort_by_key(|log| log.cursor.event_order());
        for log in page {
            validate_core_recovery_log(&log, filter.address, to.chain_id, page_start..=page_end)?;
            let bytes = log.retained_bytes();
            if logs.len() >= LEGACY_RECOVERY_LOG_LIMIT
                || bytes > LEGACY_RECOVERY_BYTE_LIMIT.saturating_sub(retained_bytes)
            {
                return Err(IndexerError::Gap(
                    "legacy observer recovery exceeded its count or byte budget".into(),
                ));
            }
            retained_bytes += bytes;
            logs.push(log);
        }
        if page_end == to.block_number {
            break;
        }
        page_start = page_end.saturating_add(1);
    }
    logs.sort_by_key(|log| log.cursor.event_order());
    Ok(logs)
}

fn install_recovered_state(
    shared: &SharedQuoteState,
    snapshot: crate::bootstrap::BootstrapSnapshot,
    mut buffered: Vec<ChainUpdate>,
    backfill_logs: Vec<ContractLog>,
    core_event_sink: Option<&CoreEventSink>,
    stats: &ClientRuntimeStats,
) -> Result<(), IndexerError> {
    crate::indexer::engine::sort_chain_updates(&mut buffered);

    // Validate the complete transition privately before publishing it. The
    // optional observer is offered logs without delaying state publication.
    let mut candidate = shared
        .indexer
        .read()
        .map_err(|_| IndexerError::LockPoisoned)?
        .clone();
    let mut ordered_logs = backfill_logs;
    if let Some(sink) = core_event_sink {
        ordered_logs.extend(buffered.iter().filter_map(|update| match update {
            ChainUpdate::Log(log) if sink.accepts(log.cursor.commitment) => Some(log.clone()),
            _ => None,
        }));
    }
    candidate.bootstrap_normalized(snapshot, buffered)?;
    ordered_logs.sort_by_key(|log| log.cursor.event_order());
    ordered_logs.dedup_by(|right, left| same_core_event_identity(left, right));
    for log in ordered_logs {
        validate_core_log_identity(
            &log,
            candidate.deployment().core,
            candidate.deployment().chain_id,
        )?;
        try_observe_core_event(core_event_sink, log, stats);
    }

    *shared
        .indexer
        .write()
        .map_err(|_| IndexerError::LockPoisoned)? = candidate;
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

pub(super) fn recovery_page_end(from_block: u64, to_block: u64) -> u64 {
    from_block
        .saturating_add(RECOVERY_PAGE_BLOCKS.saturating_sub(1))
        .min(to_block)
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
