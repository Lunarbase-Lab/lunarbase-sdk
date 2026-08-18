//! Cancellation and source-activity waits shared by runtime submodules.

use super::RuntimeError;
use tokio::{
    sync::watch,
    time::{Duration, sleep},
};

pub(super) async fn wait_until_active(
    active: &mut watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool, RuntimeError> {
    while !*active.borrow() {
        tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return Ok(false),
            changed = active.changed() => {
                if changed.is_err() {
                    return Err(RuntimeError::PumpStopped);
                }
            }
        }
    }
    Ok(true)
}

pub(crate) async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
}

pub(crate) async fn sleep_or_shutdown(
    delay: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        biased;
        () = wait_for_shutdown(shutdown) => false,
        () = sleep(delay) => true,
    }
}
