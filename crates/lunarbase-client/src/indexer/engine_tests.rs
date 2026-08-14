use super::{QuoteIndexer, canonical_floor_covers_log, snapshot_covers};
use crate::model::{
    ChainCursor, Commitment, ContractLog, DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network,
};
use lunarbase_math::{Address, B256, Bytes, FeeClass, QuoteState};

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

#[test]
fn retained_event_log_reuses_its_payload_allocations() {
    let core = Address::new([4; 20]);
    let mut indexer = QuoteIndexer::new(QuoteState::default(), deployment(core));
    let log = ContractLog {
        address: core,
        transaction_hash: Some(B256::new([2; 32])),
        topics: vec![B256::new([0xff; 32]), B256::new([3; 32])],
        data: Bytes::from(vec![4; 128]),
        removed: false,
        cursor: cursor_at(B256::new([1; 32]), 2, 3),
    };
    let topics = log.topics.as_ptr();
    let data = log.data.as_ptr();

    let retained = indexer.apply_core_log_for_delivery(log).unwrap().unwrap();

    assert_eq!(retained.topics.as_ptr(), topics);
    assert_eq!(retained.data.as_ptr(), data);
}

fn deployment(core: Address) -> DeploymentConfig {
    DeploymentConfig {
        network: Network::Base,
        chain_id: 8453,
        core,
        fee_class: FeeClass::Whitelisted,
        verified_router: None,
        deployment_block: 1,
        expected_implementation: Address::new([5; 20]),
        expected_implementation_code_hash: B256::new([6; 32]),
        contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        explicit_lane_assets: Vec::new(),
    }
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
