//! Cancellation-safe deadlines shared by bootstrap and recovery operations.

use crate::indexer::client_types::{ClientRuntimeStats, SharedQuoteState, unix_millis};
use crate::indexer::errors::IndexerError;
use crate::model::SourceError;
use std::{
    future::Future,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::watch;

/// Bounds one source operation without requiring source implementations to
/// cooperate with cancellation.
pub(crate) async fn source_operation<T, F>(
    operation: &'static str,
    deadline: Duration,
    future: F,
) -> Result<T, IndexerError>
where
    F: Future<Output = Result<T, SourceError>>,
{
    tokio::time::timeout(deadline, future)
        .await
        .map_err(|_| {
            SourceError::Unavailable(format!(
                "source {operation} exceeded its {} ms deadline",
                deadline.as_millis()
            ))
        })?
        .map_err(IndexerError::from)
}

/// Applies the fail-closed wall-clock freshness bound without underflow.
pub(crate) fn state_is_fresh(now: u64, last_update: u64, maximum_age: u64) -> bool {
    last_update != 0 && now.saturating_sub(last_update) <= maximum_age
}

/// Revokes readiness when reducer-published state stops advancing.
pub(super) async fn freshness_watchdog(
    shared: Arc<SharedQuoteState>,
    stats: Arc<ClientRuntimeStats>,
    maximum_age: Duration,
    mut cancel: watch::Receiver<bool>,
) {
    let maximum_age = duration_millis(maximum_age);
    loop {
        let generation = stats.state_update_generation.load(Ordering::Acquire);
        let last_update = stats.last_state_update_unix_millis.load(Ordering::Acquire);
        let now = unix_millis();
        let fresh = state_is_fresh(now, last_update, maximum_age);
        let delay = if fresh {
            maximum_age
                .saturating_sub(now.saturating_sub(last_update))
                .saturating_add(1)
        } else {
            expire_if_unchanged(&shared, &stats, generation);
            maximum_age.clamp(1, 250)
        };
        tokio::select! {
            biased;
            () = cancellation_requested(&mut cancel) => return,
            () = tokio::time::sleep(Duration::from_millis(delay)) => {}
        }
    }
}

fn expire_if_unchanged(shared: &SharedQuoteState, stats: &ClientRuntimeStats, generation: u64) {
    let was_available = shared.available.swap(false, Ordering::AcqRel);
    let unchanged = stats
        .state_update_generation
        .compare_exchange(generation, generation, Ordering::AcqRel, Ordering::Acquire)
        .is_ok();
    if unchanged {
        if was_available {
            shared.ready.notify_waiters();
        }
    } else {
        shared.publish_available();
    }
}

async fn cancellation_requested(cancel: &mut watch::Receiver<bool>) {
    while !*cancel.borrow() && cancel.changed().await.is_ok() {}
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().clamp(1, u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::{expire_if_unchanged, state_is_fresh};
    use crate::indexer::client_types::{ClientRuntimeStats, SharedQuoteState};
    use crate::indexer::engine::QuoteIndexer;
    use crate::model::{DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network};
    use lunarbase_math::{Address, B256, FeeClass, QuoteState};
    use std::sync::{Arc, atomic::Ordering};

    fn shared() -> (Arc<SharedQuoteState>, Arc<ClientRuntimeStats>) {
        let deployment = DeploymentConfig {
            network: Network::Base,
            chain_id: 8453,
            core: Address::new([1; 20]),
            fee_class: FeeClass::Whitelisted,
            verified_router: None,
            deployment_block: 1,
            expected_implementation: Address::new([2; 20]),
            expected_implementation_code_hash: B256::new([3; 32]),
            contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            explicit_lane_assets: Vec::new(),
        };
        (
            Arc::new(SharedQuoteState::new_not_ready(QuoteIndexer::new(
                QuoteState::default(),
                deployment,
            ))),
            Arc::new(ClientRuntimeStats::new(1, 1024)),
        )
    }

    #[test]
    fn freshness_is_fail_closed_and_saturating() {
        assert!(!state_is_fresh(100, 0, 10));
        assert!(state_is_fresh(100, 90, 10));
        assert!(!state_is_fresh(101, 90, 10));
        assert!(state_is_fresh(90, 100, 10));
    }

    #[test]
    fn watchdog_expiration_cannot_overwrite_concurrent_progress_or_shutdown() {
        let (shared, stats) = shared();
        stats.record_state_update();
        shared.publish_available();
        let generation = stats.state_update_generation.load(Ordering::Acquire);
        expire_if_unchanged(&shared, &stats, generation);
        assert!(!shared.available.load(Ordering::Acquire));

        let stale_generation = stats.state_update_generation.load(Ordering::Acquire);
        stats.record_state_update();
        shared.publish_available();
        expire_if_unchanged(&shared, &stats, stale_generation);
        assert!(shared.available.load(Ordering::Acquire));

        shared.stopping.store(true, Ordering::Release);
        shared.available.store(false, Ordering::Release);
        stats.record_state_update();
        shared.publish_available();
        assert!(!shared.available.load(Ordering::Acquire));
    }
}
