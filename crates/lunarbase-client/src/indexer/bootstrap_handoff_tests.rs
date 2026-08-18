use super::{BootstrapHandoff, captured_prefix_len, take_prefix};
use crate::indexer::client_types::{
    ClientRuntimeStats, CoreEventSink, QueuedChainUpdate, SharedQuoteState,
};
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::tasks::source_activity_lease;
use crate::model::{
    BlockRef, ChainCursor, ChainUpdate, Commitment, ContractLog, DeploymentConfig,
    MATH_COMPATIBILITY_VERSION, Network,
};
use lunarbase_math::{Address, B256, Bytes, FeeClass, QuoteState};
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::test]
async fn fixed_snapshot_does_not_absorb_refill_and_drop_releases_exactly() {
    let stats = Arc::new(ClientRuntimeStats::new(3, 3 * 1024));
    let (sender, mut receiver) = mpsc::channel(3);
    sender.send(queued(head(100), &stats).await).await.unwrap();
    sender.send(queued(head(101), &stats).await).await.unwrap();

    let captured = captured_prefix_len(&receiver, 3);
    assert_eq!(captured, 2);
    let refill_stats = Arc::clone(&stats);
    let refill_sender = sender.clone();
    let refill = tokio::spawn(async move {
        refill_sender
            .send(queued(head(102), &refill_stats).await)
            .await
            .unwrap();
    });
    refill.await.unwrap();

    let handoff = BootstrapHandoff {
        queued: take_prefix(&mut receiver, captured),
        observer_order: Vec::new(),
    };
    assert_eq!(handoff.queued.len(), 2);
    assert_eq!(receiver.len(), 1, "post-snapshot refill stays queued");
    assert_eq!(stats.queue_depth(), 3, "handoff retains count accounting");
    assert_eq!(stats.queue_byte_budget.available_permits(), 0);

    let blocked_stats = Arc::clone(&stats);
    let blocked = tokio::spawn(async move {
        sender
            .send(queued(head(103), &blocked_stats).await)
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    assert!(
        !blocked.is_finished(),
        "retained handoff permits must backpressure another refill"
    );
    drop(handoff);
    blocked.await.unwrap();
    assert_eq!(receiver.len(), 2);
    assert_eq!(stats.queue_depth(), 2);
    drop(receiver);
    assert_eq!(stats.queue_depth(), 0);
    assert_eq!(stats.queue_bytes(), 0);
    assert_eq!(stats.queue_byte_budget.available_permits(), 3 * 1024);
}

#[tokio::test]
async fn capture_never_exceeds_the_configured_prefix() {
    let stats = Arc::new(ClientRuntimeStats::new(4, 4 * 1024));
    let (sender, mut receiver) = mpsc::channel(4);
    for block in 100..104 {
        sender
            .send(queued(head(block), &stats).await)
            .await
            .unwrap();
    }

    let handoff = BootstrapHandoff::capture(&mut receiver, 2);

    assert_eq!(handoff.queued.len(), 2);
    assert_eq!(receiver.len(), 2);
    assert_eq!(stats.queue_depth(), 4);
    drop(handoff);
    drop(receiver);
    assert_eq!(stats.queue_depth(), 0);
    assert_eq!(stats.queue_byte_budget.available_permits(), 4 * 1024);
}

#[tokio::test]
async fn observer_delivery_moves_the_original_payload_and_releases_queue_budget() {
    let stats = Arc::new(ClientRuntimeStats::new(1, 64 * 1024));
    let (sender, mut receiver) = mpsc::channel(1);
    let update = log(101);
    let (topics_ptr, data_ptr) = match &update {
        ChainUpdate::Log(log) => (log.topics.as_ptr(), log.data.as_ptr()),
        _ => unreachable!(),
    };
    let queued = queued(update, &stats).await;
    let handoff = BootstrapHandoff {
        queued: vec![queued],
        observer_order: vec![0],
    };
    let sink = CoreEventSink::new(sender, Default::default());

    handoff.publish_events(Some(&sink), &stats);

    assert_eq!(stats.queue_depth(), 0);
    assert_eq!(stats.queue_bytes(), 0);
    assert_eq!(stats.queue_byte_budget.available_permits(), 64 * 1024);
    let delivered = receiver.try_recv().unwrap();
    assert_eq!(delivered.topics.as_ptr(), topics_ptr);
    assert_eq!(delivered.data.as_ptr(), data_ptr);
}

#[tokio::test]
async fn reconnect_cannot_publish_an_older_bootstrap_generation_or_observer_log() {
    let stats = Arc::new(ClientRuntimeStats::new(2, 128 * 1024));
    let initial = QuoteIndexer::new(QuoteState::default(), deployment());
    let shared = SharedQuoteState::new_not_ready(initial.clone());
    let (active, active_rx) = tokio::sync::watch::channel(true);
    let source_a_lease = source_activity_lease(&active_rx, &shared).unwrap();
    let (updates, mut receiver) = mpsc::channel(2);
    updates.send(queued(log(101), &stats).await).await.unwrap();
    let handoff = BootstrapHandoff::capture(&mut receiver, 2);
    let handoff = BootstrapHandoff {
        observer_order: vec![0],
        ..handoff
    };

    active.send_modify(|is_active| {
        shared.invalidate_source_lease();
        *is_active = false;
    });
    updates
        .send(
            queued(
                ChainUpdate::Gap {
                    cursor: None,
                    reason: "source A terminated during bootstrap".into(),
                },
                &stats,
            )
            .await,
        )
        .await
        .unwrap();
    active.send(true).unwrap();
    assert!(source_activity_lease(&active_rx, &shared).is_some());
    assert_ne!(
        source_activity_lease(&active_rx, &shared).unwrap(),
        source_a_lease
    );
    let (observer, mut observed) = mpsc::channel(1);
    let sink = CoreEventSink::new(observer, Default::default());

    let result = handoff.install_and_publish(&shared, initial, source_a_lease, Some(&sink), &stats);

    assert!(result.is_err());
    assert!(!shared.is_available());
    assert!(
        observed.try_recv().is_err(),
        "failed bootstrap emits nothing"
    );
    assert_eq!(receiver.len(), 1, "post-prefix Gap remains queued");
    assert_eq!(
        stats.queue_depth(),
        1,
        "failed handoff releases its wrapper"
    );
    assert!(matches!(
        receiver.try_recv().unwrap().dequeue(),
        ChainUpdate::Gap { .. }
    ));
    assert_eq!(stats.queue_depth(), 0);
    assert_eq!(stats.queue_bytes(), 0);
    assert_eq!(stats.queue_byte_budget.available_permits(), 128 * 1024);
}

async fn queued(update: ChainUpdate, stats: &Arc<ClientRuntimeStats>) -> QueuedChainUpdate {
    let bytes = QueuedChainUpdate::retained_bytes(&update);
    let charge = bytes.max(stats.queue_item_byte_floor);
    let permit = Arc::clone(&stats.queue_byte_budget)
        .acquire_many_owned(charge.try_into().unwrap())
        .await
        .unwrap();
    QueuedChainUpdate::new(update, bytes, permit, stats.queue_accounting())
}

fn head(block: u64) -> ChainUpdate {
    ChainUpdate::Head(BlockRef::new(
        ChainCursor {
            chain_id: 8453,
            block_number: block,
            execution_block_number: block,
            block_hash: Some(B256::new([block as u8; 32])),
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Realtime,
        },
        None,
    ))
}

fn log(block: u64) -> ChainUpdate {
    ChainUpdate::Log(ContractLog {
        address: Address::new([4; 20]),
        transaction_hash: Some(B256::new([5; 32])),
        topics: vec![B256::new([6; 32]); 64],
        data: Bytes::from(vec![7; 16 * 1024]),
        removed: false,
        cursor: ChainCursor {
            transaction_index: Some(0),
            log_index: Some(0),
            ..match head(block) {
                ChainUpdate::Head(head) => head.cursor,
                _ => unreachable!(),
            }
        },
    })
}

fn deployment() -> DeploymentConfig {
    DeploymentConfig {
        network: Network::Base,
        chain_id: 8453,
        core: Address::new([4; 20]),
        fee_class: FeeClass::Whitelisted,
        verified_router: None,
        deployment_block: 1,
        expected_implementation: Address::new([8; 20]),
        expected_implementation_code_hash: B256::new([7; 32]),
        contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        explicit_lane_assets: Vec::new(),
    }
}
