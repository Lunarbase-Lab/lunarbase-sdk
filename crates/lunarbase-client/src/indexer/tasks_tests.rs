//! Queue-accounting tests for the source pump.

use super::{
    RecoverySignal, SourceInactiveGuard, mark_source_inactive, send_update, source_activity_lease,
    source_activity_lease_after_observation,
    source_queue::{finish_recovery, normalize_update, publish_requirement_if_active},
    wait_for_source_active,
};
use crate::indexer::client_types::{ClientRuntimeStats, SharedQuoteState};
use crate::indexer::engine::QuoteIndexer;
use crate::model::{
    BlockRef, ChainCorrection, ChainCursor, ChainUpdate, Commitment, ContractLog, DeploymentConfig,
    MATH_COMPATIBILITY_VERSION, Network,
};
use lunarbase_math::{Address, B256, Bytes, FeeClass, QuoteState};
use std::sync::{Arc, mpsc as std_mpsc};
use std::thread;
use tokio::sync::{mpsc, oneshot, watch};
#[tokio::test]
async fn queue_depth_is_incremented_before_delivery() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (_cancel_sender, mut cancel) = watch::channel(false);
    let (recovery_sender, mut recovery) = watch::channel(RecoverySignal::default());
    let stats = ClientRuntimeStats::new(1, 1024);
    let update = ChainUpdate::Gap {
        cursor: None,
        reason: "counter-order test".into(),
    };

    let (sent, observed_depth) = tokio::join!(
        send_update(
            &sender,
            &mut cancel,
            &mut recovery,
            &recovery_sender,
            update,
            &stats,
            false
        ),
        async {
            let queued = receiver.recv().await.expect("update is delivered");
            let depth = stats.queue_depth();
            let bytes = stats.queue_bytes();
            assert!(bytes > 0);
            queued.dequeue();
            depth
        },
    );

    assert!(sent);
    assert_eq!(observed_depth, 1);
    assert_eq!(stats.queue_depth(), 0);
    assert_eq!(stats.queue_bytes(), 0);
}

#[tokio::test]
async fn queued_drop_and_receiver_abort_restore_accounting_and_permits() {
    let (sender, mut receiver) = mpsc::channel(2);
    let (_cancel_sender, mut cancel) = watch::channel(false);
    let (recovery_sender, mut recovery) = watch::channel(RecoverySignal::default());
    let stats = Arc::new(ClientRuntimeStats::new(2, 2048));
    for reason in ["direct drop", "receiver abort"] {
        assert!(
            send_update(
                &sender,
                &mut cancel,
                &mut recovery,
                &recovery_sender,
                ChainUpdate::Gap {
                    cursor: None,
                    reason: reason.into(),
                },
                &stats,
                false,
            )
            .await
        );
    }
    assert_eq!(stats.queue_depth(), 2);
    assert!(stats.queue_bytes() > 0);
    assert_eq!(stats.queue_byte_budget.available_permits(), 0);

    drop(receiver.recv().await.unwrap());
    assert_eq!(stats.queue_depth(), 1);
    assert!(stats.queue_bytes() > 0);
    assert_eq!(stats.queue_byte_budget.available_permits(), 1024);

    let (owned, observed) = oneshot::channel();
    let receiver_task = tokio::spawn(async move {
        owned.send(()).unwrap();
        std::future::pending::<()>().await;
        drop(receiver);
    });
    observed.await.unwrap();
    receiver_task.abort();
    assert!(receiver_task.await.unwrap_err().is_cancelled());

    assert_eq!(stats.queue_depth(), 0);
    assert_eq!(stats.queue_bytes(), 0);
    assert_eq!(stats.queue_byte_budget.available_permits(), 2048);
}

#[tokio::test]
async fn oversized_update_preserves_its_recovery_cursor() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (_cancel_sender, mut cancel) = watch::channel(false);
    let (recovery_sender, mut recovery) = watch::channel(RecoverySignal::default());
    let stats = ClientRuntimeStats::new(1, 1024);
    let cursor = ChainCursor::block(1, 105, Some(B256::new([5; 32])), Commitment::Realtime);
    let update = ChainUpdate::Log(ContractLog {
        address: Address::new([1; 20]),
        transaction_hash: Some(B256::new([2; 32])),
        topics: vec![B256::new([3; 32])],
        data: Bytes::from(vec![4; 2_048]),
        removed: false,
        cursor: cursor.clone(),
    });

    assert!(matches!(
        normalize_update(update.clone(), stats.queue_byte_capacity),
        ChainUpdate::Gap { cursor: Some(actual), .. } if actual == cursor
    ));

    assert!(
        send_update(
            &sender,
            &mut cancel,
            &mut recovery,
            &recovery_sender,
            update,
            &stats,
            false,
        )
        .await
    );
    let queued = receiver.recv().await.unwrap();
    assert!(matches!(
        queued.dequeue(),
        ChainUpdate::Gap {
            cursor: Some(actual),
            ..
        } if actual == cursor
    ));
}

#[tokio::test]
async fn queue_budget_retains_only_the_visible_tail_slice() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (_cancel_sender, mut cancel) = watch::channel(false);
    let (recovery_sender, mut recovery) = watch::channel(RecoverySignal::default());
    let stats = ClientRuntimeStats::new(1, 1024);
    let backing = Bytes::from(vec![0x6b; 1 << 20]);
    let data = backing.slice(backing.len() - 1..);
    drop(backing);
    let update = ChainUpdate::Log(ContractLog {
        address: Address::new([1; 20]),
        transaction_hash: None,
        topics: Vec::new(),
        data,
        removed: false,
        cursor: ChainCursor::block(1, 105, Some(B256::new([5; 32])), Commitment::Realtime),
    });

    assert!(
        send_update(
            &sender,
            &mut cancel,
            &mut recovery,
            &recovery_sender,
            update,
            &stats,
            false,
        )
        .await
    );
    assert!(stats.queue_bytes() <= stats.queue_byte_capacity);
    let ChainUpdate::Log(log) = receiver.recv().await.unwrap().dequeue() else {
        panic!("small visible payload must remain a log");
    };
    assert_eq!(log.data.as_ref(), [0x6b]);
    let data: Vec<u8> = log.data.into();
    assert_eq!(data.capacity(), data.len());
}

#[test]
fn correction_budget_normalizes_every_replacement_payload() {
    let backing = Bytes::from(vec![0x7c; 1 << 20]);
    let data = backing.slice(backing.len() - 1..);
    drop(backing);
    let block = BlockRef::new(
        ChainCursor::block(1, 105, Some(B256::new([5; 32])), Commitment::Realtime),
        Some(B256::new([4; 32])),
    );
    let log = ContractLog {
        address: Address::new([1; 20]),
        transaction_hash: None,
        topics: Vec::new(),
        data,
        removed: false,
        cursor: ChainCursor::block(1, 105, Some(B256::new([5; 32])), Commitment::Realtime),
    };
    let update = ChainUpdate::Correction(Box::new(ChainCorrection {
        common_ancestor: block.clone(),
        old_tip: block.clone(),
        new_tip: block,
        old_branch: Vec::new(),
        new_branch: Vec::new(),
        replacement_logs: vec![log],
    }));

    let normalized = normalize_update(update, 4096);
    let ChainUpdate::Correction(correction) = normalized else {
        panic!("logically small correction must remain a correction");
    };
    assert_eq!(correction.replacement_logs[0].data.as_ref(), [0x7c]);
    let data: Vec<u8> = correction
        .replacement_logs
        .into_iter()
        .next()
        .unwrap()
        .data
        .into();
    assert_eq!(data.capacity(), data.len());
}

#[tokio::test]
async fn retained_recovery_permit_cannot_deadlock_a_synthetic_gap() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (_cancel_sender, mut cancel) = watch::channel(false);
    let (recovery_sender, mut recovery) = watch::channel(RecoverySignal::default());
    let stats = ClientRuntimeStats::new(1, 1024);
    let update = ChainUpdate::Gap {
        cursor: None,
        reason: "failed update retained by recovery".into(),
    };
    assert!(
        send_update(
            &sender,
            &mut cancel,
            &mut recovery,
            &recovery_sender,
            update,
            &stats,
            false,
        )
        .await
    );
    let retained = receiver.recv().await.unwrap();

    let required = ChainCursor::block(1, 105, Some(B256::new([9; 32])), Commitment::Realtime);
    let synthetic = ChainUpdate::Gap {
        cursor: Some(required.clone()),
        reason: "stream disconnected".into(),
    };
    let pending = send_update(
        &sender,
        &mut cancel,
        &mut recovery,
        &recovery_sender,
        synthetic,
        &stats,
        true,
    );
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut pending)
            .await
            .is_err()
    );
    recovery_sender.send_modify(|signal| signal.active = true);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut pending)
            .await
            .unwrap()
    );
    assert!(receiver.try_recv().is_err());
    assert_eq!(recovery_sender.borrow().required.as_ref(), Some(&required));
    drop(retained);
}

#[tokio::test]
async fn post_publication_recovery_signal_cannot_be_reset_or_lost() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (_cancel_sender, mut cancel) = watch::channel(false);
    let (recovery_sender, mut recovery) = watch::channel(RecoverySignal {
        active: true,
        ..RecoverySignal::default()
    });
    let expected_generation = recovery.borrow().generation;
    let stats = ClientRuntimeStats::new(1, 1024);
    let required = ChainCursor::block(1, 106, Some(B256::new([6; 32])), Commitment::Realtime);

    assert!(
        send_update(
            &sender,
            &mut cancel,
            &mut recovery,
            &recovery_sender,
            ChainUpdate::Gap {
                cursor: Some(required.clone()),
                reason: "late terminal gap".into(),
            },
            &stats,
            true,
        )
        .await
    );
    assert!(!finish_recovery(&recovery_sender, expected_generation));
    assert!(recovery_sender.borrow().active);
    assert_eq!(recovery_sender.borrow().required.as_ref(), Some(&required));

    let latest_generation = recovery_sender.borrow().generation;
    assert!(finish_recovery(&recovery_sender, latest_generation));
    assert!(!recovery_sender.borrow().active);
    assert_eq!(recovery_sender.borrow().generation, latest_generation);

    // Simulate a sender which observed `active=true` immediately before the
    // recovery commit. Its split publication must fail atomically, then the
    // normal send path must retain the Gap in the bounded queue.
    assert!(!publish_requirement_if_active(
        &recovery_sender,
        Some(required.clone())
    ));
    assert!(
        send_update(
            &sender,
            &mut cancel,
            &mut recovery,
            &recovery_sender,
            ChainUpdate::Gap {
                cursor: Some(required.clone()),
                reason: "post-finish terminal gap".into(),
            },
            &stats,
            true,
        )
        .await
    );
    assert!(matches!(
        receiver.recv().await.unwrap().dequeue(),
        ChainUpdate::Gap { cursor: Some(cursor), .. } if cursor == required
    ));
}
#[test]
fn inactive_observation_invalidates_an_earlier_recovery_lease() {
    let shared = shared_quote_state();
    let (active, active_rx) = watch::channel(true);
    let lease = source_activity_lease(&active_rx, &shared).expect("active source has a lease");

    mark_source_inactive(&active, &shared);

    assert!(!*active_rx.borrow());
    assert!(!shared.publish_available_if(lease));
    assert!(!shared.is_available());
}

#[test]
fn lease_capture_crossing_invalidation_cannot_return_the_new_token() {
    let shared = Arc::new(shared_quote_state());
    let (active, active_rx) = watch::channel(true);
    let token_before = shared.availability_token();
    let (observed, observation) = std_mpsc::sync_channel(0);
    let (resume, resumed) = std_mpsc::sync_channel(0);
    let capture_shared = Arc::clone(&shared);
    let capture = thread::spawn(move || {
        source_activity_lease_after_observation(&active_rx, &capture_shared, || {
            observed.send(()).unwrap();
            resumed.recv().unwrap();
        })
    });

    observation.recv().unwrap();
    mark_source_inactive(&active, &shared);
    let token_after = shared.availability_token();
    assert_ne!(token_after, token_before);
    assert!(!*active.borrow());
    resume.send(()).unwrap();

    assert_eq!(capture.join().unwrap(), None);
    assert!(!shared.is_available());
}

#[tokio::test]
async fn closed_true_activity_watch_has_no_lease_and_cannot_satisfy_wait() {
    let shared = shared_quote_state();
    let (active, mut active_rx) = watch::channel(true);
    drop(active);
    let (_cancel, mut cancel_rx) = watch::channel(false);

    assert_eq!(source_activity_lease(&active_rx, &shared), None);
    assert!(!wait_for_source_active(&mut active_rx, &mut cancel_rx).await);
    assert!(!shared.is_available());
}

#[test]
fn source_pump_panic_guard_invalidates_before_the_true_watch_closes() {
    let shared = Arc::new(shared_quote_state());
    let (active, active_rx) = watch::channel(true);
    let lease = source_activity_lease(&active_rx, &shared).unwrap();
    let guard = SourceInactiveGuard::new(active, Arc::clone(&shared));

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = guard;
        panic!("simulated source-pump panic");
    }));

    assert!(panic.is_err());
    assert!(
        !*active_rx.borrow(),
        "guard publishes inactive before close"
    );
    assert!(
        active_rx.has_changed().is_err(),
        "guard owned the last sender"
    );
    assert!(!shared.publish_available_if(lease));
    assert_eq!(source_activity_lease(&active_rx, &shared), None);
    assert!(!shared.is_available());
}

fn shared_quote_state() -> SharedQuoteState {
    SharedQuoteState::new_not_ready(QuoteIndexer::new(
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
    ))
}
