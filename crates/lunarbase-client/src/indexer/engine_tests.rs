use super::{canonical_floor_covers_log, snapshot_covers};
use crate::model::{ChainCursor, Commitment};
use lunarbase_math::B256;

#[test]
fn handoff_never_covers_an_update_from_another_chain() {
    let snapshot = cursor_at(B256::new([1; 32]), 2, 3);
    let mut foreign = cursor_at(B256::new([1; 32]), 2, 2);
    foreign.chain_id = 1;
    foreign.block_number -= 1;

    assert!(matches!(
        snapshot_covers(&foreign, &snapshot),
        Err(crate::indexer::errors::IndexerError::Reducer(
            crate::state::reducer::ReducerError::ChainIdMismatch
        ))
    ));
}

#[test]
fn event_level_checkpoint_covers_only_events_through_its_cursor() {
    let floor = cursor_at(B256::new([1; 32]), 2, 3);
    let covered = cursor_at(B256::new([1; 32]), 2, 2);
    let later = cursor_at(B256::new([1; 32]), 2, 4);

    assert!(canonical_floor_covers_log(&covered, &floor).unwrap());
    assert!(!canonical_floor_covers_log(&later, &floor).unwrap());
}

fn cursor(block_hash: B256, source_sequence: Option<u64>) -> ChainCursor {
    ChainCursor {
        chain_id: 8453,
        block_number: 100,
        execution_block_number: 100,
        block_hash: Some(block_hash),
        transaction_index: Some(2),
        log_index: Some(3),
        source_sequence,
        source_sub_index: None,
        commitment: Commitment::Realtime,
    }
}

fn cursor_at(block_hash: B256, transaction_index: u32, log_index: u32) -> ChainCursor {
    ChainCursor {
        transaction_index: Some(transaction_index),
        log_index: Some(log_index),
        commitment: Commitment::Canonical,
        ..cursor(block_hash, None)
    }
}
