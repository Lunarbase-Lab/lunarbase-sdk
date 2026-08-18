use lunarbase_client::model::SourceError;
use lunarbase_source_monad::execution::ExecutionEvent;
use std::sync::Arc;
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
};

pub(super) struct QueuedExecutionEvent {
    pub(super) result: Result<ExecutionEvent, SourceError>,
    pub(super) _byte_permit: OwnedSemaphorePermit,
}

pub(super) fn send_gap(
    sender: &mpsc::Sender<QueuedExecutionEvent>,
    byte_budget: &Arc<Semaphore>,
    runtime: &Handle,
    byte_bound: usize,
    reason: &str,
) {
    let _ = send_result(
        sender,
        byte_budget,
        runtime,
        byte_bound,
        Ok(ExecutionEvent::Gap {
            cursor: None,
            reason: reason.into(),
        }),
    );
}

pub(super) fn send_error(
    sender: &mpsc::Sender<QueuedExecutionEvent>,
    byte_budget: &Arc<Semaphore>,
    runtime: &Handle,
    byte_bound: usize,
    reason: String,
) {
    let _ = send_result(
        sender,
        byte_budget,
        runtime,
        byte_bound,
        Err(SourceError::Unavailable(reason)),
    );
}

pub(super) fn send_result(
    sender: &mpsc::Sender<QueuedExecutionEvent>,
    byte_budget: &Arc<Semaphore>,
    runtime: &Handle,
    byte_bound: usize,
    mut result: Result<ExecutionEvent, SourceError>,
) -> bool {
    let mut bytes = result.as_ref().map_or_else(
        |error| std::mem::size_of::<SourceError>().saturating_add(error.to_string().len()),
        ExecutionEvent::retained_bytes,
    );
    if bytes > byte_bound {
        result = Ok(ExecutionEvent::Gap {
            cursor: None,
            reason: "Monad native event exceeded the handoff byte budget".into(),
        });
        bytes = result.as_ref().map_or(
            std::mem::size_of::<SourceError>(),
            ExecutionEvent::retained_bytes,
        );
    }
    let Ok(permits) = u32::try_from(bytes.max(1)) else {
        return false;
    };
    let permit = match runtime.block_on(byte_budget.clone().acquire_many_owned(permits)) {
        Ok(permit) => permit,
        Err(_) => return false,
    };
    sender
        .blocking_send(QueuedExecutionEvent {
            result,
            _byte_permit: permit,
        })
        .is_ok()
}
