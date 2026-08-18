use super::*;
use alloy_primitives::Bytes;
use lunarbase_client::model::{ChainCursor, Commitment};

#[test]
fn semantic_id_detects_payload_changes_and_ignores_commitment_promotion() {
    let core = Address::new([0x13; 20]);
    let resolution = resolution();
    let finalized = block(39, 9, 8, Commitment::Finalized);
    let replacement = log(core, 41, 3, Commitment::Canonical, 7);
    let original = ReorgCorrection::new(
        &resolution,
        finalized.clone(),
        vec![replacement.clone()],
        core,
    )
    .unwrap();

    let mut altered = replacement.clone();
    altered.data = Bytes::from(vec![0x99; 64]);
    let altered =
        ReorgCorrection::new(&resolution, finalized.clone(), vec![altered], core).unwrap();
    assert_ne!(original.reorg_id, altered.reorg_id);

    let advanced_finalized = block(40, 1, 0, Commitment::Finalized);
    let advanced = ReorgCorrection::new(
        &resolution,
        advanced_finalized,
        vec![replacement.clone()],
        core,
    )
    .unwrap();
    assert_eq!(original.reorg_id, advanced.reorg_id);

    let mut promoted = resolution.clone();
    promoted.common_ancestor.cursor.commitment = Commitment::Realtime;
    promoted.old_tip.cursor.commitment = Commitment::Realtime;
    promoted.new_tip.cursor.commitment = Commitment::Realtime;
    for block in promoted
        .old_branch
        .iter_mut()
        .chain(promoted.new_branch.iter_mut())
    {
        block.cursor.commitment = Commitment::Realtime;
    }
    let mut promoted_log = replacement;
    promoted_log.cursor.commitment = Commitment::Realtime;
    let promoted = ReorgCorrection::new(&promoted, finalized, vec![promoted_log], core).unwrap();
    assert_eq!(original.reorg_id, promoted.reorg_id);
}

fn resolution() -> ForkResolution {
    let ancestor = block(40, 1, 0, Commitment::Canonical);
    let old_tip = block(41, 2, 1, Commitment::Canonical);
    let new_tip = block(41, 3, 1, Commitment::Canonical);
    ForkResolution {
        common_ancestor: ancestor,
        old_tip: old_tip.clone(),
        new_tip: new_tip.clone(),
        old_branch: vec![old_tip],
        new_branch: vec![new_tip],
    }
}

fn block(number: u64, hash: u8, parent: u8, commitment: Commitment) -> BlockRef {
    BlockRef::new(
        ChainCursor::block(8453, number, Some(B256::new([hash; 32])), commitment),
        Some(B256::new([parent; 32])),
    )
}

fn log(
    core: Address,
    block_number: u64,
    block_hash: u8,
    commitment: Commitment,
    payload: u8,
) -> ContractLog {
    ContractLog {
        address: core,
        transaction_hash: Some(B256::new([payload; 32])),
        topics: vec![B256::new([payload.saturating_add(1); 32])],
        data: Bytes::from(vec![payload; 64]),
        removed: false,
        cursor: ChainCursor {
            chain_id: 8453,
            block_number,
            execution_block_number: block_number,
            block_hash: Some(B256::new([block_hash; 32])),
            transaction_index: Some(0),
            log_index: Some(0),
            source_sequence: Some(99),
            source_sub_index: Some(7),
            commitment,
        },
    }
}
