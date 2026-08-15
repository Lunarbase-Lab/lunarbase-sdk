//! Reconnecting, loss-intolerant handoff from a realtime source.

use crate::metrics::Metrics;
use futures_util::StreamExt;
use lunarbase_client::{
    model::{ChainUpdate, ContractFilter},
    source::{ChainDataSource, SourceStream},
};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

pub(crate) struct QueuedUpdate {
    update: ChainUpdate,
    _depth: SourceDepthGuard,
    _byte_permit: OwnedSemaphorePermit,
}

impl QueuedUpdate {
    pub(crate) fn into_inner(self) -> ChainUpdate {
        self.update
    }
}

struct SourceDepthGuard(Arc<Metrics>, usize);

impl Drop for SourceDepthGuard {
    fn drop(&mut self) {
        self.0.source_dequeued(self.1);
    }
}

pub(crate) struct PumpRuntime {
    pub reconnect_delay: Duration,
    pub stall_timeout: Duration,
    pub active: watch::Sender<bool>,
    pub shutdown: watch::Receiver<bool>,
    pub metrics: Arc<Metrics>,
    pub byte_budget: Arc<Semaphore>,
    pub byte_capacity: usize,
}

pub(crate) fn spawn<S>(
    source: Arc<S>,
    filter: ContractFilter,
    sender: mpsc::Sender<QueuedUpdate>,
    runtime: PumpRuntime,
) -> JoinHandle<()>
where
    S: ChainDataSource + 'static,
{
    tokio::spawn(run(source, filter, sender, runtime))
}

async fn run<S>(
    source: Arc<S>,
    filter: ContractFilter,
    sender: mpsc::Sender<QueuedUpdate>,
    mut runtime: PumpRuntime,
) where
    S: ChainDataSource + 'static,
{
    let mut ever_active = false;
    loop {
        if cancellation_requested(&mut runtime.shutdown).await {
            return;
        }
        let _ = runtime.active.send(false);
        let subscribed = tokio::select! {
            biased;
            () = wait_for_cancellation(&mut runtime.shutdown) => return,
            result = source.subscribe(filter.clone()) => result,
        };
        match subscribed {
            Ok(stream) => {
                ever_active = true;
                let _ = runtime.active.send(true);
                if !consume(stream, &sender, &mut runtime).await {
                    return;
                }
            }
            Err(error) if ever_active => {
                if !send(
                    &sender,
                    ChainUpdate::Gap {
                        cursor: None,
                        reason: format!("source subscribe failed: {error}"),
                    },
                    &mut runtime,
                )
                .await
                {
                    return;
                }
            }
            Err(error) => tracing::warn!(error = %error, "event source subscribe failed"),
        }
        let _ = runtime.active.send(false);
        runtime.metrics.source_reconnect();
        if sleep_or_cancel(runtime.reconnect_delay, &mut runtime.shutdown).await {
            return;
        }
    }
}

async fn consume(
    mut stream: SourceStream,
    sender: &mpsc::Sender<QueuedUpdate>,
    runtime: &mut PumpRuntime,
) -> bool {
    loop {
        let next = tokio::select! {
            biased;
            () = wait_for_cancellation(&mut runtime.shutdown) => return false,
            result = timeout(runtime.stall_timeout, stream.next()) => result,
        };
        let update = match next {
            Ok(Some(Ok(update))) => update,
            Ok(Some(Err(error))) => ChainUpdate::Gap {
                cursor: None,
                reason: format!("source stream failed: {error}"),
            },
            Ok(None) => ChainUpdate::Gap {
                cursor: None,
                reason: "source stream closed".into(),
            },
            Err(_) => ChainUpdate::Gap {
                cursor: None,
                reason: format!("source stalled for {:?}", runtime.stall_timeout),
            },
        };
        let terminal = matches!(update, ChainUpdate::Gap { .. });
        if !send(sender, update, runtime).await {
            return false;
        }
        if terminal {
            return true;
        }
    }
}

async fn send(
    sender: &mpsc::Sender<QueuedUpdate>,
    update: ChainUpdate,
    runtime: &mut PumpRuntime,
) -> bool {
    let mut update = update;
    let mut bytes = update.retained_bytes();
    if bytes > runtime.byte_capacity {
        update = ChainUpdate::Gap {
            cursor: None,
            reason: "source update exceeded event-worker queue byte budget".into(),
        };
        bytes = update.retained_bytes();
    }
    let byte_permit = match runtime
        .byte_budget
        .clone()
        .try_acquire_many_owned(bytes.max(1) as u32)
    {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            runtime.metrics.queue_saturated();
            tokio::select! {
                biased;
                () = wait_for_cancellation(&mut runtime.shutdown) => return false,
                result = runtime.byte_budget.clone().acquire_many_owned(bytes.max(1) as u32) => {
                    match result {
                        Ok(permit) => permit,
                        Err(_) => return false,
                    }
                },
            }
        }
        Err(tokio::sync::TryAcquireError::Closed) => return false,
    };
    let permit = match sender.try_reserve() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(_)) => {
            runtime.metrics.queue_saturated();
            tokio::select! {
                biased;
                () = wait_for_cancellation(&mut runtime.shutdown) => return false,
                result = sender.reserve() => match result {
                    Ok(permit) => permit,
                    Err(_) => return false,
                },
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => return false,
    };
    runtime.metrics.source_enqueued(bytes);
    permit.send(QueuedUpdate {
        update,
        _depth: SourceDepthGuard(runtime.metrics.clone(), bytes),
        _byte_permit: byte_permit,
    });
    true
}

async fn wait_for_cancellation(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
}

async fn cancellation_requested(shutdown: &mut watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

async fn sleep_or_cancel(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        biased;
        () = wait_for_cancellation(shutdown) => true,
        () = sleep(delay) => false,
    }
}

#[cfg(test)]
#[path = "pump_tests.rs"]
mod tests;
