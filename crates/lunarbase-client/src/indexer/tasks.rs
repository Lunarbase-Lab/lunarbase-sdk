//! Background source and single-writer reducer tasks for a connected client.

use crate::indexer::client::publish;
use crate::indexer::client_types::{
    ClientConnectConfig, ClientRuntimeStats, CoreEventSink, QueuedChainUpdate, SharedQuoteState,
};
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::indexer::runtime_helpers::source_operation;
use crate::model::{ChainUpdate, ContractFilter};
use crate::source::ChainDataSource;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::sleep;

mod correction;
mod failure;
mod microbatch;
mod recovery_backfill;
mod recovery_install;
mod recovery_stage;
#[cfg(feature = "perf-trace")]
mod segment_trace;
mod source_activity;
mod source_queue;
mod source_stream;
use failure::{drain_pending_updates, record_transition_failure};
use microbatch::{FailedLiveUpdate, apply_live_segment, collect_live_segment};
pub(super) use recovery_backfill::{
    RECOVERY_LIMITS, RecoveryLimits, RecoveryUsage, recovery_page_end,
};
use recovery_stage::record_failure as record_recovery_failure;
#[cfg(test)]
use source_activity::source_activity_lease_after_observation;
use source_activity::{SourceInactiveGuard, mark_source_inactive};
pub(super) use source_activity::{source_activity_lease, wait_for_source_active};
pub(super) use source_queue::{RecoverySignal, send_update};

/// Operational event and counter handles used by the reducer task.
pub(super) struct ReducerRuntime {
    /// Bounded broadcast channel for operational lifecycle events.
    pub events: broadcast::Sender<ClientRuntimeEvent>,
    /// Lock-free runtime counters updated by the single reducer task.
    pub stats: Arc<ClientRuntimeStats>,
    /// Optional nonblocking, commitment-filtered Core event observer.
    pub core_event_sink: Option<CoreEventSink>,
    /// Signals that a retained recovery stage already represents the
    /// transport discontinuity, so the pump must not deadlock on another Gap.
    pub recovery: watch::Sender<RecoverySignal>,
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
    /// Admission state whose source lease is invalidated synchronously.
    pub shared: Arc<SharedQuoteState>,
    /// Cooperative cancellation receiver owned by the source task.
    pub cancel: watch::Receiver<bool>,
    /// True while the reducer owns a retained recovery stage.
    pub recovery: watch::Receiver<RecoverySignal>,
    /// Nonblocking terminal-control publisher paired with `recovery`.
    pub recovery_signal: watch::Sender<RecoverySignal>,
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
        shared,
        mut cancel,
        mut recovery,
        recovery_signal,
        events,
        stats,
    } = runtime;
    let _inactive_guard = SourceInactiveGuard::new(source_active.clone(), Arc::clone(&shared));
    let mut ever_active = false;
    loop {
        mark_source_inactive(&source_active, shared.as_ref());
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
                if !source_stream::consume(
                    stream,
                    source_stream::Runtime {
                        sender: &sender,
                        stall_timeout,
                        source_active: &source_active,
                        shared: &shared,
                        cancel: &mut cancel,
                        recovery: &mut recovery,
                        recovery_signal: &recovery_signal,
                        events: &events,
                        stats: &stats,
                    },
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
                        &mut recovery,
                        &recovery_signal,
                        ChainUpdate::Gap {
                            cursor: None,
                            reason: format!("source subscribe failed: {detail}"),
                        },
                        &stats,
                        true,
                    )
                    .await
                {
                    return;
                }
            }
        }
        mark_source_inactive(&source_active, shared.as_ref());
        stats.source_reconnects.fetch_add(1, Ordering::Relaxed);
        if sleep_or_cancel(reconnect_delay, &mut cancel).await {
            return;
        }
    }
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
    let mut pending = VecDeque::new();
    loop {
        let update = if let Some(update) = pending.pop_front() {
            Some(update)
        } else {
            tokio::select! {
                biased;
                () = cancellation_requested(&mut cancel) => {
                    drain_pending_updates(
                        shared.as_ref(),
                        &mut pending,
                        &mut updates,
                        &runtime,
                    )
                    .await;
                    return;
                }
                update = updates.recv() => update,
            }
        };
        let Some(update) = update else {
            shared.revoke_available();
            return;
        };
        #[cfg(feature = "perf-trace")]
        let mut update = update;
        #[cfg(feature = "perf-trace")]
        let mut segment_trace = segment_trace::SegmentTrace::begin(&mut update, &updates, &runtime);
        let segment = collect_live_segment(update, &mut updates, &mut pending);
        #[cfg(feature = "perf-trace")]
        segment_trace.collected(&segment, &updates, &pending, &runtime);
        let source_lease = source_activity_lease(&source_active, shared.as_ref());
        #[cfg(feature = "perf-trace")]
        let applied = apply_live_segment(
            shared.as_ref(),
            segment,
            source_lease,
            &runtime,
            segment_trace.builder_mut(),
        );
        #[cfg(not(feature = "perf-trace"))]
        let applied = apply_live_segment(shared.as_ref(), segment, source_lease, &runtime);
        #[cfg(feature = "perf-trace")]
        segment_trace.finish(applied.is_ok());
        if let Err(mut failed) = applied {
            failed.queued.extend(pending.drain(..));
            if !recover_until_ready(
                shared.as_ref(),
                source.as_ref(),
                &config,
                *failed,
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
    failed: FailedLiveUpdate,
    updates: &mut mpsc::Receiver<QueuedChainUpdate>,
    cancel: &mut watch::Receiver<bool>,
    source_active: &mut watch::Receiver<bool>,
    runtime: &ReducerRuntime,
) -> bool {
    let FailedLiveUpdate {
        error,
        queued,
        failed_index,
        prior_cursor,
    } = failed;
    runtime.recovery.send_modify(|signal| signal.active = true);
    record_transition_failure(shared, error, runtime);
    let Some(stable_checkpoint) = recovery_stage::stable_checkpoint(shared, runtime) else {
        return false;
    };
    let finalized_checkpoint =
        match recovery_stage::finalized_validation_checkpoint(shared, stable_checkpoint.as_ref()) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                record_recovery_failure(error, &runtime.events, &runtime.stats);
                return false;
            }
        };
    let recovery_from = prior_cursor.clone();
    let mut stage = recovery_stage::RecoveryStage::new_segment(queued, failed_index, prior_cursor);
    loop {
        let control = runtime.recovery.borrow().clone();
        stage.merge_control(&control);
        publish(&runtime.events, ClientRuntimeEvent::RecoveryStarted);
        stage.absorb(updates);
        if !wait_for_source_active(source_active, cancel).await {
            return false;
        }
        let Some(source_lease) = source_activity_lease(source_active, shared) else {
            continue;
        };
        let floor_validation = tokio::select! {
            biased;
            () = cancellation_requested(cancel) => return false,
            result = recovery_backfill::validate_floors(
                source,
                stable_checkpoint.as_ref(),
                finalized_checkpoint.as_ref(),
                config.source_operation_timeout,
            ) => result,
        };
        if let Err(error) = floor_validation {
            record_recovery_failure(error, &runtime.events, &runtime.stats);
            if sleep_or_cancel(config.reconnect_delay, cancel).await {
                return false;
            }
            continue;
        }
        stage.absorb(updates);
        if stage.needs_canonical_head() {
            let head = tokio::select! {
                biased;
                () = cancellation_requested(cancel) => return false,
                result = source_operation(
                    "recovery canonical head",
                    config.source_operation_timeout,
                    source.canonical_head(),
                ) => result,
            };
            match head {
                Ok(head) => stage.set_canonical_head(head),
                Err(error) => {
                    record_recovery_failure(error, &runtime.events, &runtime.stats);
                    if sleep_or_cancel(config.reconnect_delay, cancel).await {
                        return false;
                    }
                    continue;
                }
            }
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
                stage.absorb(updates);
                if stage.needs_canonical_head() {
                    let head = tokio::select! {
                        biased;
                        () = cancellation_requested(cancel) => return false,
                        result = source_operation(
                            "recovery canonical head",
                            config.source_operation_timeout,
                            source.canonical_head(),
                        ) => result,
                    };
                    match head {
                        Ok(head) => stage.set_canonical_head(head),
                        Err(error) => {
                            record_recovery_failure(error, &runtime.events, &runtime.stats);
                            if sleep_or_cancel(config.reconnect_delay, cancel).await {
                                return false;
                            }
                            continue;
                        }
                    }
                }
                match stage.snapshot_covers(&snapshot.cursor) {
                    Ok(true) => {}
                    Ok(false) => {
                        record_recovery_failure(
                            IndexerError::Gap(
                                "recovery snapshot did not cover the failed source update".into(),
                            ),
                            &runtime.events,
                            &runtime.stats,
                        );
                        if sleep_or_cancel(config.reconnect_delay, cancel).await {
                            return false;
                        }
                        continue;
                    }
                    Err(error) => {
                        record_recovery_failure(error, &runtime.events, &runtime.stats);
                        if sleep_or_cancel(config.reconnect_delay, cancel).await {
                            return false;
                        }
                        continue;
                    }
                }
                let backfill_logs = if let Some(recovery_from) = recovery_from.as_ref() {
                    match recovery_backfill::load(
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
                stage.absorb(updates);
                let control = runtime.recovery.borrow().clone();
                stage.merge_control(&control);
                if stage.needs_canonical_head() {
                    continue;
                }
                match stage.snapshot_covers(&snapshot.cursor) {
                    Ok(true) => {}
                    Ok(false) => {
                        record_recovery_failure(
                            IndexerError::Gap(
                                "late recovery signal was not covered by the snapshot".into(),
                            ),
                            &runtime.events,
                            &runtime.stats,
                        );
                        continue;
                    }
                    Err(error) => {
                        record_recovery_failure(error, &runtime.events, &runtime.stats);
                        continue;
                    }
                }
                let buffered = stage.borrowed_updates();
                let result = recovery_install::install(shared, snapshot, buffered, backfill_logs);
                match result {
                    Ok(outcome) => {
                        runtime.stats.record_state_update();
                        if !shared.publish_available_if(source_lease) {
                            stage.require_all_updates();
                            record_recovery_failure(
                                IndexerError::Gap(
                                    "source disconnected while recovery was being installed".into(),
                                ),
                                &runtime.events,
                                &runtime.stats,
                            );
                            if sleep_or_cancel(config.reconnect_delay, cancel).await {
                                return false;
                            }
                            continue;
                        }
                        if !source_queue::finish_recovery(
                            &runtime.recovery,
                            stage.control_generation(),
                        ) {
                            stage.require_all_updates();
                            record_recovery_failure(
                                IndexerError::Gap(
                                    "source recovery requirement changed during publication".into(),
                                ),
                                &runtime.events,
                                &runtime.stats,
                            );
                            continue;
                        }
                        let staged = stage.into_owned_updates();
                        outcome.record_stats(runtime);
                        runtime.stats.recoveries.fetch_add(1, Ordering::Relaxed);
                        outcome.publish_after_ready(runtime, staged);
                        publish(&runtime.events, ClientRuntimeEvent::RecoveryCompleted);
                        return true;
                    }
                    Err(error) => {
                        stage.require_all_updates();
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

#[cfg(test)]
#[path = "tasks_tests.rs"]
mod tests;
