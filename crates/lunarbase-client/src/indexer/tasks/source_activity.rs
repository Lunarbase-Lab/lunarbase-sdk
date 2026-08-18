//! Source-generation leases and panic-safe activity teardown.

use super::{SharedQuoteState, cancellation_requested};
use std::sync::Arc;
use tokio::sync::watch;

pub(super) fn mark_source_inactive(active: &watch::Sender<bool>, shared: &SharedQuoteState) {
    active.send_modify(|value| {
        shared.invalidate_source_lease();
        *value = false;
    });
}

pub(super) struct SourceInactiveGuard {
    active: watch::Sender<bool>,
    shared: Arc<SharedQuoteState>,
}

impl SourceInactiveGuard {
    pub(super) fn new(active: watch::Sender<bool>, shared: Arc<SharedQuoteState>) -> Self {
        Self { active, shared }
    }
}

impl Drop for SourceInactiveGuard {
    fn drop(&mut self) {
        mark_source_inactive(&self.active, self.shared.as_ref());
    }
}

pub(in crate::indexer) fn source_activity_lease(
    active: &watch::Receiver<bool>,
    shared: &SharedQuoteState,
) -> Option<u64> {
    source_activity_lease_after_observation(active, shared, || {})
}

pub(super) fn source_activity_lease_after_observation(
    active: &watch::Receiver<bool>,
    shared: &SharedQuoteState,
    after_observation: impl FnOnce(),
) -> Option<u64> {
    if active.has_changed().is_err() {
        return None;
    }
    let was_active = *active.borrow();
    if !was_active {
        return None;
    }
    after_observation();
    let lease = shared.availability_token();
    (active.has_changed().is_ok() && *active.borrow()).then_some(lease)
}

pub(in crate::indexer) async fn wait_for_source_active(
    active: &mut watch::Receiver<bool>,
    cancel: &mut watch::Receiver<bool>,
) -> bool {
    loop {
        if active.has_changed().is_err() {
            return false;
        }
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
