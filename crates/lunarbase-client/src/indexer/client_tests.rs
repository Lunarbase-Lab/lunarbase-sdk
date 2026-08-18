use super::*;
use crate::indexer::tasks::source_activity_lease;
use crate::model::{DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network};
use lunarbase_math::{Address, B256, FeeClass};

#[tokio::test]
async fn unexpected_completion_is_terminal_from_invalid_and_preserves_event() {
    let shared = shared_quote_state();
    let (active, active_rx) = watch::channel(true);
    let original = source_activity_lease(&active_rx, shared.as_ref()).unwrap();
    let (events, mut observed) = broadcast::channel(2);
    let (_cancel, cancel_rx) = watch::channel(false);

    supervise_task(
        "completed-test-task",
        async {},
        Arc::clone(&shared),
        events,
        cancel_rx,
    )
    .await;

    assert_terminal_leases(&shared, &active_rx, original);
    assert!(
        *active.borrow(),
        "supervisor does not own the activity watch"
    );
    assert!(matches!(
        observed.try_recv().unwrap(),
        ClientRuntimeEvent::BackgroundTaskStopped {
            task: "completed-test-task"
        }
    ));
}

#[tokio::test]
async fn panic_is_terminal_from_invalid_and_preserves_event_detail() {
    let shared = shared_quote_state();
    let (active, active_rx) = watch::channel(true);
    let original = source_activity_lease(&active_rx, shared.as_ref()).unwrap();
    let (events, mut observed) = broadcast::channel(2);
    let (_cancel, cancel_rx) = watch::channel(false);

    supervise_task(
        "panicking-test-task",
        async { panic!("expected supervisor panic") },
        Arc::clone(&shared),
        events,
        cancel_rx,
    )
    .await;

    assert_terminal_leases(&shared, &active_rx, original);
    assert!(
        *active.borrow(),
        "supervisor does not own the activity watch"
    );
    assert!(matches!(
        observed.try_recv().unwrap(),
        ClientRuntimeEvent::BackgroundTaskPanicked {
            task: "panicking-test-task",
            detail
        } if detail == "expected supervisor panic"
    ));
}

fn assert_terminal_leases(
    shared: &SharedQuoteState,
    active: &watch::Receiver<bool>,
    original: u64,
) {
    let newly_sampled =
        source_activity_lease(active, shared).expect("the simulated activity watch remains true");
    assert_ne!(newly_sampled, original);
    assert!(!shared.publish_available_if(original));
    assert!(!shared.publish_available_if(newly_sampled));
    assert!(!shared.is_available());
}

fn shared_quote_state() -> Arc<SharedQuoteState> {
    Arc::new(SharedQuoteState::new_not_ready(QuoteIndexer::new(
        QuoteState::default(),
        DeploymentConfig {
            network: Network::Evm,
            chain_id: 1,
            core: Address::new([1; 20]),
            fee_class: FeeClass::Whitelisted,
            verified_router: None,
            deployment_block: 0,
            expected_implementation: Address::new([2; 20]),
            expected_implementation_code_hash: B256::new([3; 32]),
            contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            explicit_lane_assets: Vec::new(),
        },
    )))
}
