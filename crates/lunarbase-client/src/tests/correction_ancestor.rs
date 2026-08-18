//! Proof requirements for optimistic common ancestors above the stable floor.

use super::*;
use crate::model::{ChainCorrection, QuoteEvent};

const FLOOR_HASH: B256 = B256::new([1; 32]);
const ACTUAL_ANCESTOR_HASH: B256 = B256::new([0x72; 32]);
const FORGED_ANCESTOR_HASH: B256 = B256::new([0x73; 32]);
const TIP_HASH: B256 = B256::new([0x74; 32]);
const REPLACEMENT_TIP_HASH: B256 = B256::new([0x75; 32]);
const HEAD_ONLY_HASH: B256 = B256::new([0x76; 32]);

#[test]
fn correction_ancestor_must_match_retained_optimistic_identity() {
    let mut indexer = ready_indexer();
    apply_multiplier(&mut indexer, 101, ACTUAL_ANCESTOR_HASH, 2);
    apply_multiplier(&mut indexer, 102, TIP_HASH, 3);
    let published_tip = block(102, TIP_HASH, ACTUAL_ANCESTOR_HASH);
    indexer
        .apply_core_update(ChainUpdate::Head(published_tip.clone()))
        .unwrap();
    let old_tip = BlockRef::new(published_tip.cursor, Some(FORGED_ANCESTOR_HASH));
    let new_tip = block(102, REPLACEMENT_TIP_HASH, FORGED_ANCESTOR_HASH);
    let delta = ChainCorrection {
        common_ancestor: block(101, FORGED_ANCESTOR_HASH, FLOOR_HASH),
        old_tip: old_tip.clone(),
        new_tip: new_tip.clone(),
        old_branch: vec![old_tip],
        new_branch: vec![new_tip],
        replacement_logs: Vec::new(),
    };

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(delta))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
    assert_eq!(
        indexer.reducer.state().blacklist_fee_multiplier,
        U256::from(3)
    );
}

#[test]
fn correction_rejects_an_unproven_ancestor_above_the_stable_floor() {
    let mut indexer = ready_indexer();
    apply_multiplier(&mut indexer, 102, TIP_HASH, 3);
    let old_tip = block(102, TIP_HASH, ACTUAL_ANCESTOR_HASH);
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip.clone()))
        .unwrap();
    let new_tip = block(102, REPLACEMENT_TIP_HASH, ACTUAL_ANCESTOR_HASH);
    let delta = ChainCorrection {
        common_ancestor: block(101, ACTUAL_ANCESTOR_HASH, FLOOR_HASH),
        old_tip: old_tip.clone(),
        new_tip: new_tip.clone(),
        old_branch: vec![old_tip],
        new_branch: vec![new_tip],
        replacement_logs: Vec::new(),
    };

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(delta))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
}

#[test]
fn a_retained_head_proves_an_uneventful_correction_ancestor() {
    let mut indexer = ready_indexer();
    let ancestor = block(101, ACTUAL_ANCESTOR_HASH, FLOOR_HASH);
    indexer
        .apply_core_update(ChainUpdate::Head(ancestor.clone()))
        .unwrap();
    apply_multiplier(&mut indexer, 102, TIP_HASH, 3);
    let old_tip = block(102, TIP_HASH, ACTUAL_ANCESTOR_HASH);
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip.clone()))
        .unwrap();
    let new_tip = block(102, REPLACEMENT_TIP_HASH, ACTUAL_ANCESTOR_HASH);

    indexer
        .apply_core_update(ChainUpdate::Correction(Box::new(ChainCorrection {
            common_ancestor: ancestor,
            old_tip: old_tip.clone(),
            new_tip: new_tip.clone(),
            old_branch: vec![old_tip],
            new_branch: vec![new_tip],
            replacement_logs: Vec::new(),
        })))
        .unwrap();
    assert!(indexer.reducer.is_ready());
}

#[test]
fn a_late_stale_head_cannot_relabel_a_retained_eventful_block() {
    let mut indexer = ready_indexer();
    apply_multiplier(&mut indexer, 101, ACTUAL_ANCESTOR_HASH, 2);
    apply_multiplier(&mut indexer, 102, TIP_HASH, 3);
    let old_tip = block(102, TIP_HASH, ACTUAL_ANCESTOR_HASH);
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip.clone()))
        .unwrap();

    // The reducer ignores this stale head because block 102 is already
    // published. The identity proof must ignore it as well.
    indexer
        .apply_core_update(ChainUpdate::Head(block(
            101,
            FORGED_ANCESTOR_HASH,
            FLOOR_HASH,
        )))
        .unwrap();
    let valid_base = indexer.clone();

    let forged_tip = block(102, REPLACEMENT_TIP_HASH, FORGED_ANCESTOR_HASH);
    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(ChainCorrection {
            common_ancestor: block(101, FORGED_ANCESTOR_HASH, FLOOR_HASH),
            old_tip: BlockRef::new(old_tip.cursor.clone(), Some(FORGED_ANCESTOR_HASH)),
            new_tip: forged_tip.clone(),
            old_branch: vec![BlockRef::new(
                old_tip.cursor.clone(),
                Some(FORGED_ANCESTOR_HASH),
            )],
            new_branch: vec![forged_tip],
            replacement_logs: Vec::new(),
        }))),
        Err(IndexerError::Gap(_))
    ));

    let mut valid = valid_base;
    let replacement = block(102, REPLACEMENT_TIP_HASH, ACTUAL_ANCESTOR_HASH);
    valid
        .apply_core_update(ChainUpdate::Correction(Box::new(ChainCorrection {
            common_ancestor: block(101, ACTUAL_ANCESTOR_HASH, FLOOR_HASH),
            old_tip: old_tip.clone(),
            new_tip: replacement.clone(),
            old_branch: vec![old_tip],
            new_branch: vec![replacement],
            replacement_logs: Vec::new(),
        })))
        .unwrap();
    assert!(valid.reducer.is_ready());
    assert_eq!(
        valid.reducer.state().blacklist_fee_multiplier,
        U256::from(2)
    );
}

#[test]
fn correction_proves_head_only_blocks_across_the_abandoned_branch() {
    let mut indexer = ready_indexer();
    let ancestor = block(101, ACTUAL_ANCESTOR_HASH, FLOOR_HASH);
    let head_only = block(102, HEAD_ONLY_HASH, ACTUAL_ANCESTOR_HASH);
    let old_tip = block(103, TIP_HASH, HEAD_ONLY_HASH);
    indexer
        .apply_core_update(ChainUpdate::Head(ancestor.clone()))
        .unwrap();
    indexer
        .apply_core_update(ChainUpdate::Head(head_only.clone()))
        .unwrap();
    apply_multiplier(&mut indexer, 103, TIP_HASH, 3);
    indexer
        .apply_core_update(ChainUpdate::Head(old_tip.clone()))
        .unwrap();

    let forged_middle = block(102, FORGED_ANCESTOR_HASH, ACTUAL_ANCESTOR_HASH);
    let forged_old_tip = BlockRef::new(old_tip.cursor.clone(), Some(FORGED_ANCESTOR_HASH));
    let replacement = block(103, REPLACEMENT_TIP_HASH, FORGED_ANCESTOR_HASH);
    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(ChainCorrection {
            common_ancestor: ancestor,
            old_tip: forged_old_tip.clone(),
            new_tip: replacement.clone(),
            old_branch: vec![forged_middle, forged_old_tip],
            new_branch: vec![
                block(102, FORGED_ANCESTOR_HASH, ACTUAL_ANCESTOR_HASH),
                replacement,
            ],
            replacement_logs: Vec::new(),
        }))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
}

#[test]
fn correction_cannot_cross_a_finalized_floor_below_the_realtime_tip() {
    let mut indexer = ready_indexer();
    let h101 = block(101, B256::new([0x81; 32]), FLOOR_HASH);
    let h102 = block(102, B256::new([0x82; 32]), B256::new([0x81; 32]));
    let ancestor = block(103, B256::new([0x83; 32]), B256::new([0x82; 32]));
    let h104 = block(104, B256::new([0x84; 32]), B256::new([0x83; 32]));
    let mut finalized = block(105, B256::new([0x85; 32]), B256::new([0x84; 32]));
    finalized.cursor.commitment = Commitment::Finalized;
    let old_tip = block(106, B256::new([0x86; 32]), B256::new([0x85; 32]));
    for head in [
        h101,
        h102,
        ancestor.clone(),
        h104.clone(),
        finalized,
        old_tip.clone(),
    ] {
        indexer.apply_core_update(ChainUpdate::Head(head)).unwrap();
    }

    // The supplied branch deliberately downgrades block 105 to Realtime. Its
    // hash/execution identity still matches retained history, so only the
    // independent finalized floor can reject this deeper rollback.
    let abandoned_105 = block(105, B256::new([0x85; 32]), B256::new([0x84; 32]));
    let new_104 = block(104, B256::new([0x94; 32]), B256::new([0x83; 32]));
    let new_105 = block(105, B256::new([0x95; 32]), B256::new([0x94; 32]));
    let new_tip = block(106, B256::new([0x96; 32]), B256::new([0x95; 32]));
    let delta = ChainCorrection {
        common_ancestor: ancestor,
        old_tip: old_tip.clone(),
        new_tip: new_tip.clone(),
        old_branch: vec![h104, abandoned_105, old_tip],
        new_branch: vec![new_104, new_105, new_tip],
        replacement_logs: Vec::new(),
    };

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Correction(Box::new(delta))),
        Err(IndexerError::Gap(_))
    ));
    assert!(!indexer.reducer.is_ready());
}

fn ready_indexer() -> QuoteIndexer {
    let mut indexer = QuoteIndexer::new(QuoteState::default(), config().deployment);
    indexer.bootstrap(snapshot(100)).unwrap();
    indexer
}

fn apply_multiplier(indexer: &mut QuoteIndexer, height: u64, hash: B256, multiplier: u64) {
    indexer
        .apply_update(ChainUpdate::Log(bare_log(height, hash)), &|_| {
            Some(QuoteEvent::BlacklistFeeMultiplierSet {
                multiplier: U256::from(multiplier),
            })
        })
        .unwrap();
}

fn block(height: u64, hash: B256, parent: B256) -> BlockRef {
    BlockRef::new(cursor_with_hash(height, hash), Some(parent))
}

fn bare_log(height: u64, hash: B256) -> ContractLog {
    ContractLog {
        address: CORE,
        transaction_hash: Some(B256::new([0x74; 32])),
        topics: Vec::new(),
        data: Bytes::new(),
        removed: false,
        cursor: {
            let mut cursor = cursor_with_hash(height, hash);
            cursor.transaction_index = Some(0);
            cursor.log_index = Some(0);
            cursor
        },
    }
}

fn cursor_with_hash(height: u64, hash: B256) -> ChainCursor {
    let mut cursor = cursor(height, Commitment::Realtime);
    cursor.block_hash = Some(hash);
    cursor
}
