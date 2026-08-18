//! Optimistic fork correction and publication invariants.

use super::*;
use crate::indexer::errors::ClientRuntimeEvent;
use crate::model::ChainCorrection;
use crate::protocol::abi::TOPIC_LANE_REMOVED;
use crate::state::reducer::ReducerError;
use std::sync::atomic::AtomicU64;

pub(super) const OLD_HASH: B256 = B256::new([0x22; 32]);
pub(super) const NEW_HASH: B256 = B256::new([0x33; 32]);
const ANCESTOR_HASH: B256 = B256::new([1; 32]);
const SHARED_HASH: B256 = B256::new([0x44; 32]);
const THIRD_HASH: B256 = B256::new([0x55; 32]);
const FOURTH_HASH: B256 = B256::new([0x66; 32]);

mod reducer_identity;

#[test]
fn handoff_sort_keeps_correction_between_abandoned_and_replacement_logs() {
    let mut indexer = ready_indexer();
    let replacement = lane_removed_log(101, NEW_HASH);
    indexer
        .apply_handoff(vec![
            ChainUpdate::Log(lane_removed_log(101, OLD_HASH)),
            ChainUpdate::Correction(Box::new(correction(vec![replacement.clone()]))),
            ChainUpdate::Log(replacement),
        ])
        .unwrap();

    assert!(indexer.reducer.is_ready());
    assert_eq!(indexer.reducer.cursor().unwrap().block_hash, Some(NEW_HASH));
    assert!(!indexer.state().unwrap().lanes.contains_key(&ASSET));
}

#[test]
fn direct_correction_restores_state_without_revoking_readiness() {
    let mut indexer = ready_indexer();
    let stable = indexer.checkpoint().unwrap();
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(101, OLD_HASH)))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip()))
        .unwrap();
    assert!(!indexer.state().unwrap().lanes.contains_key(&ASSET));

    let correction = correction(Vec::new());
    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(correction.clone())))
        .unwrap();

    assert!(indexer.reducer.is_ready());
    assert!(indexer.state().unwrap().lanes.contains_key(&ASSET));
    assert_eq!(indexer.reducer.cursor().unwrap().block_hash, Some(NEW_HASH));
    assert_eq!(indexer.correction_history_usage().0, 0);
    assert_eq!(indexer.checkpoint().unwrap(), stable);
    assert_eq!(
        indexer.checkpoint().unwrap().cursor,
        correction.common_ancestor.cursor
    );

    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(correction)))
        .unwrap();
    assert!(indexer.reducer.is_ready());
    assert!(indexer.state().unwrap().lanes.contains_key(&ASSET));
}

#[test]
fn optimistic_head_and_late_log_never_advance_durable_checkpoint() {
    let mut indexer = ready_indexer();
    let stable = indexer.checkpoint().unwrap();
    let mut head = cursor(102, Commitment::Realtime);
    head.block_hash = Some(B256::new([0x12; 32]));
    indexer
        .apply_core_update(ChainUpdate::Head(BlockRef::new(head, Some(OLD_HASH))))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(101, OLD_HASH)))
        .unwrap();

    let checkpoint = indexer.checkpoint().unwrap();
    assert_eq!(checkpoint.cursor, stable.cursor);
    assert_eq!(checkpoint.state, stable.state);
    assert_eq!(checkpoint.cursor.block_number, 100);
}

#[test]
fn shallow_correction_keeps_the_original_floor_for_a_later_deeper_fork() {
    let mut indexer = ready_indexer();
    let stable = indexer.checkpoint().unwrap();
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(101, SHARED_HASH)))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(102, OLD_HASH)))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Head(block(102, OLD_HASH, SHARED_HASH)))
        .unwrap();

    let ancestor = block(101, SHARED_HASH, ANCESTOR_HASH);
    let old = block(102, OLD_HASH, SHARED_HASH);
    let replacement = block(102, NEW_HASH, SHARED_HASH);
    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(ChainCorrection {
            common_ancestor: ancestor.clone(),
            old_tip: old.clone(),
            new_tip: replacement.clone(),
            old_branch: vec![old],
            new_branch: vec![replacement.clone()],
            replacement_logs: Vec::new(),
        })))
        .unwrap();

    assert_eq!(indexer.checkpoint().unwrap(), stable);

    let deeper_ancestor = BlockRef::new(
        cursor_with_hash(100, ANCESTOR_HASH, Commitment::Finalized),
        None,
    );
    let replacement_101 = block(101, THIRD_HASH, ANCESTOR_HASH);
    let replacement_102 = block(102, FOURTH_HASH, THIRD_HASH);
    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(ChainCorrection {
            common_ancestor: deeper_ancestor,
            old_tip: replacement.clone(),
            new_tip: replacement_102.clone(),
            old_branch: vec![ancestor, replacement],
            new_branch: vec![replacement_101, replacement_102],
            replacement_logs: Vec::new(),
        })))
        .unwrap();
    assert!(indexer.reducer.is_ready());
    assert_eq!(
        indexer.reducer.cursor().unwrap().block_hash,
        Some(FOURTH_HASH)
    );
    assert_eq!(indexer.checkpoint().unwrap(), stable);
    assert_eq!(indexer.correction_history_usage().0, 0);
}

#[test]
fn correction_requires_block_level_and_execution_consistent_identity() {
    let mut positioned = correction(Vec::new());
    positioned.common_ancestor.cursor.transaction_index = Some(0);
    assert!(matches!(positioned.validate(), Err(SourceError::Gap(_))));

    let mut inconsistent = correction(replacement_load_logs(1));
    inconsistent.replacement_logs[0]
        .cursor
        .execution_block_number = 102;
    assert!(matches!(inconsistent.validate(), Err(SourceError::Gap(_))));
}

#[test]
fn correction_old_tip_must_match_the_published_execution_context() {
    let mut indexer = ready_indexer();
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(101, OLD_HASH)))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip()))
        .unwrap();

    let mut conflicting = correction(Vec::new());
    conflicting.old_tip.cursor.execution_block_number = 102;
    conflicting.old_branch[0].cursor.execution_block_number = 102;
    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(conflicting))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
}

#[test]
fn duplicate_correction_still_validates_core_identity() {
    let mut indexer = ready_indexer();
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(101, OLD_HASH)))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip()))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(correction(Vec::new()))))
        .unwrap();

    let mut foreign = replacement_load_logs(1);
    foreign[0].address = Address::new([0x77; 20]);
    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(correction(foreign)))),
        Err(IndexerError::Reducer(ReducerError::ContractAddressMismatch))
    ));
    assert!(!indexer.reducer.is_ready());
}

#[test]
fn duplicate_correction_rejects_conflicting_execution_context() {
    let mut indexer = ready_indexer();
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(101, OLD_HASH)))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip()))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(correction(Vec::new()))))
        .unwrap();

    let mut conflicting = correction(Vec::new());
    conflicting.new_tip.cursor.execution_block_number = 102;
    conflicting.new_branch[0].cursor.execution_block_number = 102;
    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(conflicting))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connected_correction_is_one_swap_and_keeps_service_ready() {
    let source = Arc::new(MockSource::new(None));
    let client = Arc::new(
        ConnectedQuoteClient::connect(config(), source.clone(), None)
            .await
            .unwrap(),
    );
    let mut events = client.subscribe_runtime_events();
    source.publish(ChainUpdate::Log(lane_removed_log(101, OLD_HASH)));
    source.publish(ChainUpdate::Head(old_tip()));
    wait_until(|| client.runtime_stats().correction_history_blocks == 1).await;
    assert!(client.is_ready());
    assert!(matches!(
        client.quote(&request()).unwrap().outcome,
        lunarbase_math::QuoteOutcome::Unavailable(lunarbase_math::UnavailableReason::MissingLane(
            ASSET
        ))
    ));

    let stop = Arc::new(AtomicBool::new(false));
    let unexpected = Arc::new(AtomicU64::new(0));
    let first_unexpected = Arc::new(Mutex::new(None));
    let mut readers = Vec::new();
    for _ in 0..8 {
        let client = Arc::clone(&client);
        let stop = Arc::clone(&stop);
        let unexpected = Arc::clone(&unexpected);
        let first_unexpected = Arc::clone(&first_unexpected);
        readers.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                match client.quote(&request()) {
                    Ok(quote)
                        if matches!(
                            quote.outcome,
                            lunarbase_math::QuoteOutcome::Unavailable(
                                lunarbase_math::UnavailableReason::StaleLane(ASSET)
                            ) | lunarbase_math::QuoteOutcome::Unavailable(
                                lunarbase_math::UnavailableReason::MissingLane(ASSET)
                            )
                        ) => {}
                    Err(error) => {
                        unexpected.fetch_add(1, Ordering::Relaxed);
                        first_unexpected
                            .lock()
                            .unwrap()
                            .get_or_insert_with(|| format!("error: {error:?}"));
                    }
                    Ok(quote) => {
                        unexpected.fetch_add(1, Ordering::Relaxed);
                        first_unexpected
                            .lock()
                            .unwrap()
                            .get_or_insert_with(|| format!("outcome: {:?}", quote.outcome));
                    }
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    let correction = correction(replacement_load_logs(2_000));
    source.publish(ChainUpdate::Correction(Box::new(correction.clone())));
    wait_until(|| client.runtime_stats().corrections == 1).await;
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        reader.await.unwrap();
    }

    assert_eq!(
        unexpected.load(Ordering::Relaxed),
        0,
        "{:?}",
        first_unexpected.lock().unwrap()
    );
    assert!(client.is_ready());
    assert!(matches!(
        client.quote(&request()).unwrap().outcome,
        lunarbase_math::QuoteOutcome::Unavailable(lunarbase_math::UnavailableReason::StaleLane(
            ASSET
        ))
    ));
    let stats = client.runtime_stats();
    assert_eq!(stats.gaps, 0);
    assert_eq!(stats.recoveries, 0);
    assert_eq!(stats.correction_history_blocks, 0);
    assert_eq!(source.snapshot_calls.load(Ordering::Relaxed), 1);

    source.publish(ChainUpdate::Correction(Box::new(correction)));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(client.is_ready());
    assert_eq!(client.runtime_stats().corrections, 1);
    assert_eq!(client.runtime_stats().gaps, 0);
    assert_eq!(source.snapshot_calls.load(Ordering::Relaxed), 1);

    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        event,
        ClientRuntimeEvent::CorrectionApplied {
            common_ancestor: 100,
            old_tip_hash: OLD_HASH,
            new_tip_hash: NEW_HASH,
            replacement_logs: 2_000,
        }
    ));
    client.shutdown().await;
}

fn ready_indexer() -> QuoteIndexer {
    let mut indexer = QuoteIndexer::new(QuoteState::default(), config().deployment);
    indexer.bootstrap(snapshot(100)).unwrap();
    indexer
}

pub(super) fn correction(replacement_logs: Vec<ContractLog>) -> ChainCorrection {
    ChainCorrection {
        common_ancestor: BlockRef::new(
            cursor_with_hash(100, ANCESTOR_HASH, Commitment::Finalized),
            None,
        ),
        old_tip: old_tip(),
        new_tip: new_tip(),
        old_branch: vec![old_tip()],
        new_branch: vec![new_tip()],
        replacement_logs,
    }
}

pub(super) fn old_tip() -> BlockRef {
    BlockRef::new(
        cursor_with_hash(101, OLD_HASH, Commitment::Realtime),
        Some(ANCESTOR_HASH),
    )
}

fn new_tip() -> BlockRef {
    BlockRef::new(
        cursor_with_hash(101, NEW_HASH, Commitment::Realtime),
        Some(ANCESTOR_HASH),
    )
}

fn block(number: u64, hash: B256, parent: B256) -> BlockRef {
    BlockRef::new(
        cursor_with_hash(number, hash, Commitment::Realtime),
        Some(parent),
    )
}

pub(super) fn lane_removed_log(block: u64, hash: B256) -> ContractLog {
    let mut asset_topic = [0_u8; 32];
    asset_topic[12..].copy_from_slice(ASSET.as_slice());
    ContractLog {
        address: CORE,
        transaction_hash: Some(B256::new([0x41; 32])),
        topics: vec![TOPIC_LANE_REMOVED, B256::new(asset_topic)],
        data: Bytes::new(),
        removed: false,
        cursor: event_cursor(block, hash, 0),
    }
}

fn replacement_load_logs(count: u32) -> Vec<ContractLog> {
    (0..count)
        .map(|log_index| ContractLog {
            address: CORE,
            transaction_hash: Some(B256::new([0x51; 32])),
            topics: vec![B256::new([0x99; 32])],
            data: Bytes::new(),
            removed: false,
            cursor: event_cursor(101, NEW_HASH, log_index),
        })
        .collect()
}

fn event_cursor(block: u64, hash: B256, log_index: u32) -> ChainCursor {
    let mut cursor = cursor_with_hash(block, hash, Commitment::Realtime);
    cursor.transaction_index = Some(0);
    cursor.log_index = Some(log_index);
    cursor
}

fn cursor_with_hash(block: u64, hash: B256, commitment: Commitment) -> ChainCursor {
    let mut cursor = cursor(block, commitment);
    cursor.block_hash = Some(hash);
    cursor
}
