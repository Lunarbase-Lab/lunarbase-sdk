//! Focused correction regressions kept separate from the runtime stress harness.

use super::*;
use crate::model::{ChainCorrection, MAX_CORRECTION_RETAINED_BYTES, QuoteEvent};
use crate::protocol::abi::TOPIC_LANE_REMOVED;
use crate::state::reducer::ReducerError;

const OLD_HASH: B256 = B256::new([0x72; 32]);
const NEW_HASH: B256 = B256::new([0x73; 32]);
const ANCESTOR_HASH: B256 = B256::new([1; 32]);

#[test]
fn gap_memory_charge_includes_reserved_string_capacity() {
    let mut reason = String::with_capacity(1 << 20);
    reason.push('x');
    let reserved = reason.capacity();
    let update = ChainUpdate::Gap {
        cursor: None,
        reason,
    };
    assert!(update.retained_bytes() >= reserved);
}

#[test]
fn head_to_event_cannot_cross_a_nonzero_branch_hash() {
    let mut reducer = QuoteReducer::new(
        snapshot(100).state,
        lunarbase_math::FeeClass::Whitelisted,
        None,
    );
    reducer.bootstrap(cursor(100, Commitment::Finalized));
    let mut head = cursor_with_hash(101, OLD_HASH, Commitment::Realtime);
    head.source_sequence = Some(1);
    reducer.observe_head(head).unwrap();

    let mut replacement = event_cursor(101, NEW_HASH, 0);
    replacement.source_sequence = Some(2);
    assert_eq!(
        reducer.apply(replacement, QuoteEvent::LaneRemoved { asset: ASSET },),
        Err(ReducerError::BlockHashMismatch)
    );
    assert!(reducer.state().lanes.contains_key(&ASSET));
}

#[test]
fn same_hash_cannot_change_execution_context() {
    let mut reducer = QuoteReducer::new(
        snapshot(100).state,
        lunarbase_math::FeeClass::Whitelisted,
        None,
    );
    reducer.bootstrap(cursor(100, Commitment::Finalized));
    let mut head = cursor_with_hash(101, OLD_HASH, Commitment::Realtime);
    head.execution_block_number = 1_001;
    head.source_sequence = Some(1);
    reducer.observe_head(head.clone()).unwrap();

    head.execution_block_number = 1_002;
    head.source_sequence = Some(2);
    assert_eq!(
        reducer.observe_head(head),
        Err(ReducerError::BlockHashMismatch)
    );
    assert_eq!(reducer.cursor().unwrap().execution_block_number, 1_001);
}

#[test]
fn canonical_floor_requires_the_exact_published_block_identity() {
    let mut indexer = ready_indexer();
    let stable = indexer.checkpoint().unwrap();
    let mut conflicting = cursor_with_hash(100, OLD_HASH, Commitment::Finalized);
    conflicting.execution_block_number = 101;

    assert_eq!(
        indexer.set_canonical_floor(conflicting),
        Err(IndexerError::Reducer(ReducerError::BlockHashMismatch))
    );
    assert!(!indexer.reducer.is_ready());
    assert_eq!(indexer.checkpoint().unwrap(), stable);
}

#[test]
fn no_op_partner_activity_does_not_consume_correction_history() {
    let mut indexer = ready_indexer();
    for block in 101..=300 {
        let update = ChainUpdate::Log(bare_log(block, block_hash(block)));
        indexer
            .apply_update(update, &|_| {
                Some(QuoteEvent::PartnerInfoSet {
                    router: ROUTER,
                    asset: ASSET,
                    fee: 10,
                })
            })
            .unwrap();
    }
    let usage = indexer.correction_history_usage();
    assert_eq!(usage.0, 0);
    assert_eq!(usage.2, 0);
    assert!(indexer.state().unwrap().lanes.contains_key(&ASSET));

    indexer
        .apply_update(ChainUpdate::Log(bare_log(301, block_hash(301))), &|_| {
            Some(QuoteEvent::LaneRemoved { asset: ASSET })
        })
        .unwrap();
    let usage = indexer.correction_history_usage();
    assert_eq!(usage.0, 1);
    assert_eq!(usage.2, 0);
}

#[test]
fn correction_rewinds_ordering_after_an_old_branch_with_only_no_op_logs() {
    let mut indexer = ready_indexer();
    indexer
        .apply_update(ChainUpdate::Log(bare_log(101, OLD_HASH)), &|_| {
            Some(QuoteEvent::PartnerInfoSet {
                router: ROUTER,
                asset: ASSET,
                fee: 10,
            })
        })
        .unwrap();
    assert_eq!(indexer.correction_history_usage().0, 0);
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip()))
        .unwrap();

    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(correction(Vec::new()))))
        .unwrap();
    assert!(indexer.reducer.is_ready());
    assert_eq!(indexer.reducer.cursor().unwrap().block_hash, Some(NEW_HASH));
    assert!(indexer.state().unwrap().lanes.contains_key(&ASSET));
}

#[test]
fn an_observed_new_head_is_not_an_applied_correction() {
    let mut indexer = ready_indexer();
    indexer
        .apply_core_update(ChainUpdate::Head(new_tip()))
        .unwrap();

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(correction(vec![
            lane_removed_log(101, NEW_HASH),
        ])))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
    assert!(indexer.reducer.state().lanes.contains_key(&ASSET));
}

#[test]
fn altered_payload_with_the_same_tips_is_not_an_exact_duplicate() {
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

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(correction(vec![
            lane_removed_log(101, NEW_HASH),
        ])))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
    assert!(indexer.reducer.state().lanes.contains_key(&ASSET));
}

#[test]
fn correction_publishes_the_declared_block_level_new_tip() {
    let mut indexer = ready_indexer();
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(101, OLD_HASH)))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip()))
        .unwrap();
    let delta = correction(vec![lane_removed_log(101, NEW_HASH)]);
    let expected = delta.new_tip.cursor.clone();

    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(delta)))
        .unwrap();

    assert_eq!(indexer.reducer.cursor(), Some(&expected));
    assert!(
        indexer
            .reducer
            .cursor()
            .unwrap()
            .transaction_index
            .is_none()
    );
    assert!(indexer.reducer.cursor().unwrap().log_index.is_none());
}

#[test]
fn state_mutation_after_correction_invalidates_its_duplicate_marker() {
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
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(101, NEW_HASH)))
        .unwrap();

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(correction(Vec::new())))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
    assert!(!indexer.reducer.state().lanes.contains_key(&ASSET));
}

#[test]
fn exact_correction_retry_remains_a_no_op_after_later_heads() {
    let mut indexer = ready_indexer();
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip()))
        .unwrap();
    let delta = correction(Vec::new());
    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(delta.clone())))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Head(BlockRef::new(
            cursor_with_hash(102, block_hash(102), Commitment::Realtime),
            Some(NEW_HASH),
        )))
        .unwrap();

    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(delta)))
        .unwrap();
    assert!(indexer.reducer.is_ready());
    assert_eq!(indexer.reducer.cursor().unwrap().block_number, 102);
}

#[test]
fn authoritative_recovery_preserves_the_exact_correction_marker() {
    let mut indexer = ready_indexer();
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip()))
        .unwrap();
    let delta = correction(Vec::new());
    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(delta.clone())))
        .unwrap();
    indexer.bootstrap(snapshot(102)).unwrap();

    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(delta)))
        .unwrap();
    assert!(indexer.reducer.is_ready());
    assert_eq!(indexer.reducer.cursor().unwrap().block_number, 102);
}

#[test]
fn correction_cannot_rollback_a_finalized_published_tip() {
    let mut indexer = ready_indexer();
    indexer
        .apply_core_update(ChainUpdate::Log(lane_removed_log(101, OLD_HASH)))
        .unwrap();
    let mut finalized = old_tip();
    finalized.cursor.commitment = Commitment::Finalized;
    indexer
        .apply_core_update(ChainUpdate::Head(finalized))
        .unwrap();
    let mut late_realtime = bare_log(101, OLD_HASH);
    late_realtime.cursor.log_index = Some(1);
    indexer
        .apply_update(ChainUpdate::Log(late_realtime), &|_| {
            Some(QuoteEvent::PartnerInfoSet {
                router: ROUTER,
                asset: ASSET,
                fee: 10,
            })
        })
        .unwrap();
    assert_eq!(
        indexer.reducer.cursor().unwrap().commitment,
        Commitment::Finalized
    );

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(correction(Vec::new())))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
}

#[test]
fn replacement_log_commitment_must_match_its_branch() {
    let mut delta = correction(vec![lane_removed_log(101, NEW_HASH)]);
    delta.replacement_logs[0].cursor.commitment = Commitment::Canonical;
    assert!(matches!(delta.validate(), Err(SourceError::Gap(_))));
}

#[test]
fn correction_rejects_duplicate_canonical_position_with_new_transport_order() {
    let first = lane_removed_log(101, NEW_HASH);
    let mut duplicate = first.clone();
    duplicate.cursor.source_sequence = Some(2);
    duplicate.cursor.source_sub_index = Some(7);
    let delta = correction(vec![first, duplicate]);
    assert!(matches!(delta.validate(), Err(SourceError::Gap(_))));
}

#[test]
fn correction_cannot_replace_a_tip_with_the_same_block_identity() {
    let mut delta = correction(vec![lane_removed_log(101, NEW_HASH)]);
    delta.old_tip = delta.new_tip.clone();
    delta.old_branch = delta.new_branch.clone();

    assert!(matches!(delta.validate(), Err(SourceError::Gap(_))));
}

#[test]
fn correction_cannot_reuse_a_tip_hash_with_changed_execution_context() {
    let mut delta = correction(Vec::new());
    delta.new_tip.cursor.block_hash = delta.old_tip.cursor.block_hash;
    delta.new_tip.cursor.execution_block_number += 1;
    delta.new_branch[0] = delta.new_tip.clone();

    assert!(matches!(delta.validate(), Err(SourceError::Gap(_))));
}

#[test]
fn correction_rejects_a_repeated_hash_with_changed_block_identity() {
    let mut delta = correction(Vec::new());
    delta.new_tip.cursor.block_hash = delta.common_ancestor.cursor.block_hash;
    delta.new_branch[0] = delta.new_tip.clone();

    assert!(matches!(delta.validate(), Err(SourceError::Gap(_))));
}

#[test]
fn correction_byte_budget_charges_reserved_vector_capacity() {
    let mut delta = correction(Vec::new());
    let capacity = MAX_CORRECTION_RETAINED_BYTES
        .checked_div(std::mem::size_of::<BlockRef>())
        .unwrap()
        .saturating_add(1);
    let mut reserved = Vec::with_capacity(capacity);
    reserved.push(old_tip());
    delta.old_branch = reserved;

    assert!(delta.retained_bytes() > MAX_CORRECTION_RETAINED_BYTES);
    assert!(matches!(delta.validate(), Err(SourceError::Gap(_))));
}

#[test]
fn snapshot_covered_correction_is_validated_before_skip() {
    let mut valid = ready_indexer();
    valid
        .apply_handoff(vec![ChainUpdate::Correction(Box::new(covered_correction(
            Vec::new(),
        )))])
        .unwrap();
    assert!(valid.reducer.is_ready());

    let mut recovered = QuoteIndexer::new(QuoteState::default(), config().deployment);
    let notices = recovered
        .bootstrap_normalized_with_notices(
            snapshot(100),
            vec![ChainUpdate::Correction(Box::new(covered_correction(
                Vec::new(),
            )))],
        )
        .unwrap();
    assert!(notices.is_empty());

    let mut foreign_log = bare_log(100, ANCESTOR_HASH);
    foreign_log.address = Address::new([0x99; 20]);
    foreign_log.cursor.commitment = Commitment::Finalized;
    let mut foreign = ready_indexer();
    assert!(matches!(
        foreign.apply_handoff(vec![ChainUpdate::Correction(Box::new(covered_correction(
            vec![foreign_log],
        )))]),
        Err(IndexerError::Reducer(ReducerError::ContractAddressMismatch))
    ));
    assert!(!foreign.reducer.is_ready());

    let mut malformed_delta = covered_correction(Vec::new());
    malformed_delta.new_branch[0].parent_hash = Some(B256::new([0xfe; 32]));
    let mut malformed = ready_indexer();
    assert!(matches!(
        malformed.apply_handoff(vec![ChainUpdate::Correction(Box::new(malformed_delta))]),
        Err(IndexerError::Source(SourceError::Gap(_)))
    ));
    assert!(!malformed.reducer.is_ready());
}

#[test]
fn covered_correction_cannot_replace_an_exact_retry_marker() {
    let mut indexer = ready_indexer();
    let original = covered_correction(Vec::new());
    indexer
        .apply_handoff(vec![ChainUpdate::Correction(Box::new(original.clone()))])
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(original)))
        .unwrap();

    let mut altered_log = bare_log(100, ANCESTOR_HASH);
    altered_log.cursor.commitment = Commitment::Finalized;
    let altered = covered_correction(vec![altered_log]);
    assert!(matches!(
        indexer.apply_handoff(vec![ChainUpdate::Correction(Box::new(altered))]),
        Err(IndexerError::Gap(_))
    ));
}

#[test]
fn finalized_snapshot_preflight_is_atomic_and_cannot_weaken_the_floor() {
    let mut indexer = ready_indexer();
    let finalized = cursor_with_hash(101, OLD_HASH, Commitment::Finalized);
    indexer
        .apply_core_update(ChainUpdate::Head(BlockRef::new(finalized.clone(), None)))
        .unwrap();
    let before_state = indexer.state().unwrap().clone();
    let before_cursor = indexer.reducer.cursor().cloned();

    let mut weaker = snapshot(101);
    weaker.cursor = ChainCursor {
        commitment: Commitment::Canonical,
        ..finalized.clone()
    };
    assert!(matches!(
        indexer.bootstrap(weaker),
        Err(IndexerError::Gap(_))
    ));
    assert_eq!(indexer.state().unwrap(), &before_state);
    assert_eq!(indexer.reducer.cursor(), before_cursor.as_ref());
    assert!(indexer.reducer.is_ready());

    let mut missing_hash = snapshot(102);
    missing_hash.cursor.block_hash = None;
    assert!(matches!(
        indexer.bootstrap(missing_hash),
        Err(IndexerError::Gap(_))
    ));
    assert_eq!(indexer.state().unwrap(), &before_state);
    assert_eq!(indexer.reducer.cursor(), before_cursor.as_ref());
    assert!(indexer.reducer.is_ready());
}

fn ready_indexer() -> QuoteIndexer {
    let mut indexer = QuoteIndexer::new(QuoteState::default(), config().deployment);
    indexer.bootstrap(snapshot(100)).unwrap();
    indexer
}

fn correction(replacement_logs: Vec<ContractLog>) -> ChainCorrection {
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

fn covered_correction(replacement_logs: Vec<ContractLog>) -> ChainCorrection {
    let parent_hash = B256::new([0x80; 32]);
    let ancestor = BlockRef::new(
        cursor_with_hash(99, parent_hash, Commitment::Finalized),
        None,
    );
    let old = BlockRef::new(
        cursor_with_hash(100, OLD_HASH, Commitment::Realtime),
        Some(parent_hash),
    );
    let new = BlockRef::new(
        cursor_with_hash(100, ANCESTOR_HASH, Commitment::Finalized),
        Some(parent_hash),
    );
    ChainCorrection {
        common_ancestor: ancestor,
        old_tip: old.clone(),
        new_tip: new.clone(),
        old_branch: vec![old],
        new_branch: vec![new],
        replacement_logs,
    }
}

fn old_tip() -> BlockRef {
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

fn lane_removed_log(block: u64, hash: B256) -> ContractLog {
    let mut asset_topic = [0_u8; 32];
    asset_topic[12..].copy_from_slice(ASSET.as_slice());
    let mut log = bare_log(block, hash);
    log.topics = vec![TOPIC_LANE_REMOVED, B256::new(asset_topic)];
    log
}

fn bare_log(block: u64, hash: B256) -> ContractLog {
    ContractLog {
        address: CORE,
        transaction_hash: Some(B256::new([0x74; 32])),
        topics: Vec::new(),
        data: Bytes::new(),
        removed: false,
        cursor: event_cursor(block, hash, 0),
    }
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

fn block_hash(block: u64) -> B256 {
    B256::new([(block as u8).wrapping_add(1); 32])
}
