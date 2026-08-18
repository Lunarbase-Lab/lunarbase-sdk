use super::super::{RecoverySignal, recovery_stage::RecoveryStage};
use super::*;
use crate::indexer::client_types::{ClientRuntimeStats, CoreEventSink, CoreEventSinkPolicy};
use crate::indexer::engine::QuoteIndexer;
use crate::model::{
    BlockRef, ChainCorrection, Commitment, ContractLog, DeploymentConfig,
    MATH_COMPATIBILITY_VERSION, Network,
};
use crate::protocol::abi::core;
use alloy_sol_types::SolEvent;
use lunarbase_math::{Address, B256, FeeClass, QuoteState, U256};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{broadcast, mpsc, watch};

#[tokio::test]
async fn forty_same_block_logs_publish_once_and_clone_state_once() {
    let stats = Arc::new(ClientRuntimeStats::new(64, 1024 * 1024));
    let (observer, mut observed) = mpsc::channel(64);
    let runtime = runtime(
        Arc::clone(&stats),
        Some(CoreEventSink::new(observer, CoreEventSinkPolicy::default())),
    );
    let shared = shared_indexer();
    let mut segment = Vec::with_capacity(40);
    for log_index in 0..40 {
        segment.push(
            queued(
                ChainUpdate::Log(multiplier_log(100, log_index, log_index + 2)),
                &stats,
            )
            .await,
        );
    }

    #[cfg(feature = "perf-trace")]
    assert!(apply_live_segment(&shared, segment, None, &runtime, None).is_ok());
    #[cfg(not(feature = "perf-trace"))]
    assert!(apply_live_segment(&shared, segment, None, &runtime).is_ok());

    assert_eq!(shared.publication_generation(), 1);
    assert_eq!(stats.state_update_generation.load(Ordering::Acquire), 1);
    assert_eq!(stats.queue_depth(), 0);
    let published = shared.load_indexer().unwrap();
    assert_eq!(published.reducer.state_cow_clones(), 1);
    assert_eq!(published.reducer.cursor().unwrap().log_index, Some(39));
    drop(published);
    for log_index in 0..40 {
        assert_eq!(
            observed.try_recv().unwrap().cursor.log_index,
            Some(log_index)
        );
    }
    assert!(observed.try_recv().is_err());
}

#[tokio::test]
async fn every_nonjoining_update_is_retained_as_a_barrier() {
    let barriers = [
        ("subsequent head", ChainUpdate::Head(head(100))),
        (
            "different identity log",
            ChainUpdate::Log(multiplier_log(101, 1, 3)),
        ),
        (
            "reorg",
            ChainUpdate::Reorg {
                old_head: head(100),
                new_head: head(101),
            },
        ),
        (
            "gap",
            ChainUpdate::Gap {
                cursor: Some(block_cursor(101)),
                reason: "barrier test".into(),
            },
        ),
        (
            "correction",
            ChainUpdate::Correction(Box::new(ChainCorrection {
                common_ancestor: head(99),
                old_tip: head(100),
                new_tip: head(101),
                old_branch: Vec::new(),
                new_branch: Vec::new(),
                replacement_logs: Vec::new(),
            })),
        ),
    ];

    for (label, barrier) in barriers {
        let stats = Arc::new(ClientRuntimeStats::new(2, 64 * 1024));
        let (sender, mut receiver) = mpsc::channel(1);
        let first = queued(ChainUpdate::Log(multiplier_log(100, 0, 2)), &stats).await;
        sender
            .send(queued(barrier.clone(), &stats).await)
            .await
            .unwrap();
        let mut pending = VecDeque::new();

        let segment = collect_live_segment(first, &mut receiver, &mut pending);

        assert_eq!(segment.len(), 1, "{label}");
        assert_eq!(pending.len(), 1, "{label}");
        assert_eq!(pending.front().unwrap().update(), &barrier, "{label}");
        assert_eq!(stats.queue_depth(), 2, "{label}");
        for queued in segment.into_iter().chain(pending) {
            drop(queued.dequeue());
        }
        assert_eq!(stats.queue_depth(), 0, "{label}");
    }
}
#[tokio::test]
async fn first_head_starts_segment_but_next_head_is_pending() {
    let stats = Arc::new(ClientRuntimeStats::new(3, 96 * 1024));
    let (sender, mut receiver) = mpsc::channel(2);
    let first = queued(ChainUpdate::Head(head(100)), &stats).await;
    sender
        .send(queued(ChainUpdate::Log(multiplier_log(100, 0, 2)), &stats).await)
        .await
        .unwrap();
    sender
        .send(queued(ChainUpdate::Head(head(100)), &stats).await)
        .await
        .unwrap();
    let mut pending = VecDeque::new();

    let segment = collect_live_segment(first, &mut receiver, &mut pending);

    assert_eq!(segment.len(), 2);
    assert!(matches!(
        pending.front().unwrap().update(),
        ChainUpdate::Head(_)
    ));
    assert_eq!(stats.queue_depth(), 3);
    for queued in segment.into_iter().chain(pending) {
        drop(queued.dequeue());
    }
    assert_eq!(stats.queue_depth(), 0);
}

#[cfg(feature = "perf-trace")]
#[tokio::test]
async fn pending_barrier_preserves_its_first_receive_timestamp() {
    let stats = Arc::new(ClientRuntimeStats::new(2, 64 * 1024));
    let (sender, mut receiver) = mpsc::channel(1);
    let first = queued(ChainUpdate::Log(multiplier_log(100, 0, 2)), &stats).await;
    sender
        .send(queued(ChainUpdate::Head(head(101)), &stats).await)
        .await
        .unwrap();
    let mut pending = VecDeque::new();

    let segment = collect_live_segment(first, &mut receiver, &mut pending);
    let mut barrier = pending.pop_front().expect("head is retained as a barrier");
    let received_at = barrier
        .received_at()
        .expect("try_recv records the first receive timestamp");
    let later = received_at
        .checked_add(std::time::Duration::from_millis(1))
        .expect("test timestamp fits");

    assert_eq!(barrier.mark_received(later), received_at);
    assert_eq!(barrier.received_at(), Some(received_at));
    for queued in segment {
        drop(queued.dequeue());
    }
    drop(barrier.dequeue());
    assert_eq!(stats.queue_depth(), 0);
}

#[tokio::test]
async fn segment_cap_leaves_item_257_in_the_bounded_queue() {
    let stats = Arc::new(ClientRuntimeStats::new(300, 2 * 1024 * 1024));
    let (sender, mut receiver) = mpsc::channel(300);
    let first = queued(ChainUpdate::Log(multiplier_log(100, 0, 2)), &stats).await;
    for log_index in 1..257 {
        sender
            .send(
                queued(
                    ChainUpdate::Log(multiplier_log(100, log_index, log_index + 2)),
                    &stats,
                )
                .await,
            )
            .await
            .unwrap();
    }
    let mut pending = VecDeque::new();

    let segment = collect_live_segment(first, &mut receiver, &mut pending);

    assert_eq!(segment.len(), 256);
    assert!(pending.is_empty());
    assert_eq!(receiver.len(), 1);
    for queued in segment {
        drop(queued.dequeue());
    }
    drop(receiver.try_recv().unwrap().dequeue());
    assert_eq!(stats.queue_depth(), 0);
}

#[tokio::test]
async fn failed_private_prefix_and_consumed_barrier_enter_recovery_together() {
    let stats = Arc::new(ClientRuntimeStats::new(4, 64 * 1024));
    let runtime = runtime(Arc::clone(&stats), None);
    let shared = shared_indexer();
    let first = queued(ChainUpdate::Log(multiplier_log(100, 0, 2)), &stats).await;
    let mut malformed = multiplier_log(100, 1, 3);
    malformed.topics.clear();
    let second = queued(ChainUpdate::Log(malformed), &stats).await;
    let barrier = queued(
        ChainUpdate::Gap {
            cursor: Some(block_cursor(101)),
            reason: "post-segment barrier".into(),
        },
        &stats,
    )
    .await;

    #[cfg(feature = "perf-trace")]
    let failed = apply_live_segment(&shared, vec![first, second], None, &runtime, None);
    #[cfg(not(feature = "perf-trace"))]
    let failed = apply_live_segment(&shared, vec![first, second], None, &runtime);
    let mut failed = *failed.unwrap_err();
    assert_eq!(failed.failed_index, 1);
    assert_eq!(failed.queued.len(), 2);
    assert_eq!(shared.publication_generation(), 0);
    assert_eq!(
        shared.load_indexer().unwrap().reducer.cursor(),
        Some(&block_cursor(99))
    );
    assert_eq!(stats.queue_depth(), 3);

    failed.queued.push(barrier);
    {
        let stage =
            RecoveryStage::new_segment(failed.queued, failed.failed_index, failed.prior_cursor);
        assert_eq!(stage.updates().count(), 3);
        assert!(!stage.snapshot_covers(&block_cursor(99)).unwrap());
    }
    assert_eq!(stats.queue_depth(), 0);
}

#[test]
fn stale_generation_cannot_install_a_candidate() {
    let shared = shared_indexer();
    let (generation, candidate) = shared.indexer_candidate().unwrap();
    shared.mutate_indexer(|_| ()).unwrap();

    assert!(
        shared
            .publish_indexer_if_generation(generation, candidate)
            .unwrap()
            .is_none()
    );
    assert_eq!(shared.publication_generation(), 1);
}

#[cfg(feature = "perf-trace")]
#[test]
fn traced_publication_timestamps_bracket_the_arc_swap_store() {
    let shared = shared_indexer();
    let (generation, candidate) = shared.indexer_candidate().unwrap();

    let (retired, timing) = shared
        .publish_indexer_if_generation_traced(generation, candidate)
        .unwrap();

    drop(retired.expect("matching generation publishes the candidate"));
    let gate_acquired = timing
        .writer_gate_acquired_at
        .expect("writer gate is traced");
    let pre_store = timing.pre_store_at.expect("pre-store bound is traced");
    let store_returned = timing.store_returned_at.expect("store return is traced");
    let gate_released = timing
        .writer_gate_released_at
        .expect("gate release is traced");
    assert!(gate_acquired <= pre_store);
    assert!(pre_store <= store_returned);
    assert!(store_returned <= gate_released);
    assert_eq!(shared.publication_generation(), 1);
}

fn runtime(
    stats: Arc<ClientRuntimeStats>,
    core_event_sink: Option<CoreEventSink>,
) -> ReducerRuntime {
    let (events, _) = broadcast::channel(16);
    let (recovery, _) = watch::channel(RecoverySignal::default());
    ReducerRuntime {
        events,
        stats,
        core_event_sink,
        recovery,
    }
}

fn shared_indexer() -> SharedQuoteState {
    let mut indexer = QuoteIndexer::new(QuoteState::default(), deployment());
    indexer.reducer.bootstrap(block_cursor(99));
    SharedQuoteState::new_not_ready(indexer)
}

fn deployment() -> DeploymentConfig {
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
    }
}

fn head(block: u64) -> BlockRef {
    BlockRef::new(
        block_cursor(block),
        Some(B256::new([(block - 1) as u8; 32])),
    )
}

fn block_cursor(block: u64) -> ChainCursor {
    ChainCursor::block(
        1,
        block,
        Some(B256::new([block as u8; 32])),
        Commitment::Realtime,
    )
}

fn multiplier_log(block: u64, log_index: u32, multiplier: u32) -> ContractLog {
    let encoded = core::BlacklistFeeMultiplierSet {
        multiplier: U256::from(multiplier),
    }
    .encode_log_data();
    let mut cursor = block_cursor(block);
    cursor.transaction_index = Some(0);
    cursor.log_index = Some(log_index);
    ContractLog {
        address: Address::new([1; 20]),
        transaction_hash: Some(B256::new([4; 32])),
        topics: encoded.topics().to_vec(),
        data: encoded.data,
        removed: false,
        cursor,
    }
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
