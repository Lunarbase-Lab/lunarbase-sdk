//! Source-stream terminal handling kept out of reducer task wiring.

use super::{
    ClientRuntimeEvent, ClientRuntimeStats, QueuedChainUpdate, RecoverySignal, SharedQuoteState,
    cancellation_requested, mark_source_inactive, publish, send_update, source_queue,
};
use crate::indexer::client_types::PendingCorrectionAdmission;
use crate::model::ChainUpdate;
use crate::source::SourceStream;
use futures_util::StreamExt;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::timeout;

pub(super) struct Runtime<'a> {
    pub(super) sender: &'a mpsc::Sender<QueuedChainUpdate>,
    pub(super) stall_timeout: Duration,
    pub(super) source_active: &'a watch::Sender<bool>,
    pub(super) shared: &'a std::sync::Arc<SharedQuoteState>,
    pub(super) cancel: &'a mut watch::Receiver<bool>,
    pub(super) recovery: &'a mut watch::Receiver<RecoverySignal>,
    pub(super) recovery_signal: &'a watch::Sender<RecoverySignal>,
    pub(super) events: &'a broadcast::Sender<ClientRuntimeEvent>,
    pub(super) stats: &'a ClientRuntimeStats,
}

pub(super) async fn consume(mut stream: SourceStream, mut runtime: Runtime<'_>) -> bool {
    loop {
        let item = tokio::select! {
            biased;
            () = cancellation_requested(runtime.cancel) => return false,
            item = timeout(runtime.stall_timeout, stream.next()) => item,
        };
        let item = match item {
            Ok(item) => item,
            Err(_) => return stalled(&mut runtime).await,
        };
        let Some(item) = item else {
            mark_source_inactive(runtime.source_active, runtime.shared.as_ref());
            publish(runtime.events, ClientRuntimeEvent::SourceStreamClosed);
            return terminal_gap(
                &mut runtime,
                "source stream closed; canonical recovery required".into(),
            )
            .await;
        };
        let (update, synthetic_terminal) = match item {
            Ok(update) => (update, false),
            Err(error) => {
                let detail = error.to_string();
                publish(
                    runtime.events,
                    ClientRuntimeEvent::SourceStreamFailed {
                        detail: detail.clone(),
                    },
                );
                (
                    ChainUpdate::Gap {
                        cursor: None,
                        reason: format!("source stream failed: {detail}"),
                    },
                    true,
                )
            }
        };
        #[cfg(feature = "perf-trace")]
        if let Some(new_tip_hash) = crate::indexer::perf_trace::correction_hash(&update) {
            runtime.stats.trace_correction(
                new_tip_hash,
                crate::indexer::perf_trace::PerfTraceStage::SourceItem,
            );
        }
        // Budget conversion is terminal and must invalidate the lease before
        // either queue admission or out-of-band recovery publication. Inspect
        // the logical charge here; `send_update` owns the single compaction.
        let terminal = source_queue::is_terminal_after_normalization(
            &update,
            runtime.stats.queue_byte_capacity,
        );
        if terminal {
            mark_source_inactive(runtime.source_active, runtime.shared.as_ref());
        }
        let correction_admission = if matches!(update, ChainUpdate::Correction(_)) {
            PendingCorrectionAdmission::begin(std::sync::Arc::clone(runtime.shared))
        } else {
            None
        };
        if !source_queue::send_update_with_admission(
            runtime.sender,
            runtime.cancel,
            runtime.recovery,
            runtime.recovery_signal,
            update,
            runtime.stats,
            synthetic_terminal,
            correction_admission,
        )
        .await
        {
            return false;
        }
        if terminal {
            return true;
        }
    }
}

async fn stalled(runtime: &mut Runtime<'_>) -> bool {
    mark_source_inactive(runtime.source_active, runtime.shared.as_ref());
    publish(
        runtime.events,
        ClientRuntimeEvent::SourceStreamFailed {
            detail: format!(
                "source produced no updates for {} ms",
                runtime.stall_timeout.as_millis()
            ),
        },
    );
    terminal_gap(
        runtime,
        "realtime source stalled; canonical recovery required".into(),
    )
    .await
}

async fn terminal_gap(runtime: &mut Runtime<'_>, reason: String) -> bool {
    send_update(
        runtime.sender,
        runtime.cancel,
        runtime.recovery,
        runtime.recovery_signal,
        ChainUpdate::Gap {
            cursor: None,
            reason,
        },
        runtime.stats,
        true,
    )
    .await
}
