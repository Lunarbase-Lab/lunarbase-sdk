//! Bounded source-to-reducer queue admission.

use super::{ClientRuntimeStats, QueuedChainUpdate, cancellation_requested, recovery_stage};
use crate::indexer::client_types::{PendingCorrectionAdmission, unix_millis};
use crate::model::{ChainCursor, ChainUpdate};
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, watch};

#[derive(Clone, Debug, Default)]
pub(in crate::indexer) struct RecoverySignal {
    pub(super) active: bool,
    pub(super) required: Option<ChainCursor>,
    pub(super) conflict: bool,
    pub(super) needs_canonical_head: bool,
    pub(super) generation: u64,
}

pub(in crate::indexer) async fn send_update(
    sender: &mpsc::Sender<QueuedChainUpdate>,
    cancel: &mut watch::Receiver<bool>,
    recovery: &mut watch::Receiver<RecoverySignal>,
    recovery_signal: &watch::Sender<RecoverySignal>,
    update: ChainUpdate,
    stats: &ClientRuntimeStats,
    skip_if_recovering: bool,
) -> bool {
    send_update_with_admission(
        sender,
        cancel,
        recovery,
        recovery_signal,
        update,
        stats,
        skip_if_recovering,
        None,
    )
    .await
}

pub(super) async fn send_update_with_admission(
    sender: &mpsc::Sender<QueuedChainUpdate>,
    cancel: &mut watch::Receiver<bool>,
    recovery: &mut watch::Receiver<RecoverySignal>,
    recovery_signal: &watch::Sender<RecoverySignal>,
    update: ChainUpdate,
    stats: &ClientRuntimeStats,
    skip_if_recovering: bool,
    correction_admission: Option<PendingCorrectionAdmission>,
) -> bool {
    let update = normalize_update(update, stats.queue_byte_capacity);
    let recovery_cursor = recovery_stage::update_cursor(&update).cloned();
    if skip_if_recovering
        && recovery.borrow().active
        && publish_requirement_if_active(recovery_signal, recovery_cursor.clone())
    {
        return true;
    }
    let bytes = QueuedChainUpdate::retained_bytes(&update);
    let skip_if_recovering = skip_if_recovering || matches!(update, ChainUpdate::Gap { .. });
    // One weighted semaphore enforces both budgets. Every item consumes at
    // least 1/N of the byte pool, avoiding a second normal-path acquisition.
    let permit_bytes = bytes.max(stats.queue_item_byte_floor);
    let byte_permit = if skip_if_recovering {
        loop {
            tokio::select! {
                biased;
                () = cancellation_requested(cancel) => return false,
                () = recovery_started(recovery) => {
                    if publish_requirement_if_active(
                        recovery_signal,
                        recovery_cursor.clone(),
                    ) {
                        return true;
                    }
                },
                result = stats.queue_byte_budget.clone().acquire_many_owned(permit_bytes as u32) => break result,
            }
        }
    } else {
        tokio::select! {
            biased;
            () = cancellation_requested(cancel) => return false,
            result = stats.queue_byte_budget.clone().acquire_many_owned(permit_bytes as u32) => result,
        }
    };
    let Ok(byte_permit) = byte_permit else {
        return false;
    };
    let permit = if skip_if_recovering {
        loop {
            tokio::select! {
                biased;
                () = cancellation_requested(cancel) => return false,
                () = recovery_started(recovery) => {
                    if publish_requirement_if_active(
                        recovery_signal,
                        recovery_cursor.clone(),
                    ) {
                        return true;
                    }
                },
                result = sender.reserve() => break result,
            }
        }
    } else {
        tokio::select! {
            biased;
            () = cancellation_requested(cancel) => return false,
            result = sender.reserve() => result,
        }
    };
    let Ok(permit) = permit else {
        return false;
    };

    stats
        .last_source_update_unix_millis
        .store(unix_millis(), Ordering::Relaxed);
    #[cfg(feature = "perf-trace")]
    let trace_stage = crate::indexer::perf_trace::correction_hash(&update)
        .map(|new_tip_hash| (new_tip_hash, std::time::Instant::now()));
    permit.send(
        QueuedChainUpdate::new(update, bytes, byte_permit, stats.queue_accounting())
            .with_correction_admission(correction_admission),
    );
    #[cfg(feature = "perf-trace")]
    if let Some((new_tip_hash, recorded_at)) = trace_stage {
        stats.trace_correction_segment_at(
            new_tip_hash,
            crate::indexer::perf_trace::PerfTraceStage::QueueAdmission,
            recorded_at,
            None,
            None,
        );
    }
    true
}

pub(super) fn normalize_update(mut update: ChainUpdate, byte_capacity: usize) -> ChainUpdate {
    if QueuedChainUpdate::retained_bytes(&update) <= byte_capacity {
        update.normalize_for_retention();
        debug_assert!(QueuedChainUpdate::retained_bytes(&update) <= byte_capacity);
        return update;
    }
    ChainUpdate::Gap {
        cursor: recovery_stage::update_cursor(&update).cloned(),
        reason: "source update exceeded reducer queue byte budget; canonical recovery required"
            .into(),
    }
}

pub(super) fn is_terminal_after_normalization(update: &ChainUpdate, byte_capacity: usize) -> bool {
    matches!(update, ChainUpdate::Gap { .. })
        || QueuedChainUpdate::retained_bytes(update) > byte_capacity
}

pub(super) fn publish_requirement_if_active(
    signal: &watch::Sender<RecoverySignal>,
    cursor: Option<ChainCursor>,
) -> bool {
    let mut published = false;
    signal.send_if_modified(|signal| {
        if !signal.active {
            return false;
        }
        signal.generation = signal.generation.wrapping_add(1);
        if let Some(cursor) = cursor.as_ref() {
            signal.conflict |= recovery_stage::merge_required(&mut signal.required, cursor.clone());
        } else {
            signal.needs_canonical_head = true;
        }
        published = true;
        true
    });
    published
}

pub(super) fn finish_recovery(
    signal: &watch::Sender<RecoverySignal>,
    expected_generation: u64,
) -> bool {
    let mut cleared = false;
    signal.send_if_modified(|current| {
        if current.generation != expected_generation {
            return false;
        }
        let generation = current.generation;
        *current = RecoverySignal {
            generation,
            ..RecoverySignal::default()
        };
        cleared = true;
        true
    });
    cleared
}

async fn recovery_started(recovery: &mut watch::Receiver<RecoverySignal>) {
    loop {
        if recovery.borrow().active {
            return;
        }
        if recovery.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}
