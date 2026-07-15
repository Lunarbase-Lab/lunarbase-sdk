use super::*;
use lunarbase_math::{Address, QuoteState, U256};

fn address(value: u8) -> Address {
    let mut bytes = [0u8; 20];
    bytes[19] = value;
    Address(bytes)
}
fn cursor(index: u32) -> ChainCursor {
    ChainCursor {
        chain_id: 8453,
        block_number: 10,
        block_hash: None,
        transaction_index: Some(0),
        log_index: Some(index),
        source_sequence: None,
        source_sub_index: None,
        commitment: Commitment::Canonical,
    }
}

#[test]
fn namespace_uses_one_cluster_hash_tag() {
    let namespace = RedisNamespace::new(8453, address(7));
    assert!(namespace.meta.contains("{8453:0x"));
    assert!(namespace.state.contains("{8453:0x"));
}

#[test]
fn reducer_preserves_raw_slot_and_principal_lifecycle() {
    let asset = address(2);
    let mut reducer = QuoteReducer::new(QuoteState {
        cash: address(1),
        ..Default::default()
    });
    reducer
        .apply(
            cursor(0),
            QuoteEvent::LaneUpdated {
                asset,
                slot0: U256::MAX,
            },
        )
        .unwrap();
    reducer
        .apply(cursor(1), QuoteEvent::LaneAdded { asset })
        .unwrap();
    reducer
        .apply(
            cursor(2),
            QuoteEvent::DepositExecuted {
                asset,
                principal: U256::from(10u8),
            },
        )
        .unwrap();
    reducer
        .apply(
            cursor(3),
            QuoteEvent::WithdrawalExecuted {
                asset,
                principal: U256::from(3u8),
            },
        )
        .unwrap();
    assert_eq!(reducer.state().lanes[&asset].slot0, U256::MAX);
    assert_eq!(
        reducer.state().total_principal_amount[&asset],
        U256::from(7u8)
    );
}

#[test]
fn bounded_handoff_queue_poison_is_sticky() {
    let mut queue = BufferedUpdateQueue::new(1).unwrap();
    queue.push(ChainUpdate::Head(cursor(0))).unwrap();
    assert!(queue.push(ChainUpdate::Head(cursor(1))).is_err());
    assert!(queue.is_poisoned());
    assert!(queue.drain().is_err());
}

#[test]
fn block_head_does_not_hide_first_log_in_same_block() {
    let asset = address(2);
    let mut reducer = QuoteReducer::new(QuoteState {
        cash: address(1),
        ..Default::default()
    });
    reducer
        .observe_head(ChainCursor::block(8453, 10, None, Commitment::Realtime))
        .unwrap();
    reducer
        .apply(
            ChainCursor {
                chain_id: 8453,
                block_number: 10,
                block_hash: None,
                transaction_index: Some(0),
                log_index: Some(0),
                source_sequence: None,
                source_sub_index: None,
                commitment: Commitment::Realtime,
            },
            QuoteEvent::LaneAdded { asset },
        )
        .unwrap();
    assert!(reducer.state().lanes[&asset].exists);
}

#[test]
fn binary_codec_round_trips_checkpoint_and_update() {
    let asset = address(2);
    let cursor = cursor(7);
    let checkpoint = Checkpoint {
        schema_version: SCHEMA_VERSION,
        math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        expected_runtime_code_hash: [9u8; 32],
        cursor: cursor.clone(),
        state: QuoteState {
            cash: address(1),
            lanes: [(
                asset,
                lunarbase_math::LaneState {
                    slot0: U256::MAX,
                    exists: true,
                    paused: false,
                    block_delay: 3,
                    slippage_k_bps: 42,
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        },
    };
    let encoded = encode_checkpoint(&checkpoint).unwrap();
    assert_eq!(decode_checkpoint(&encoded).unwrap(), checkpoint);

    let update = ChainUpdate::Head(cursor);
    assert_eq!(decode_update(&encode_update(&update)).unwrap(), update);
}

#[test]
fn monad_filtered_logs_allow_sparse_global_sequences_but_reject_regression() {
    let mut tracker = MonadRingTracker::default();
    assert!(tracker.observe_sparse(100, 0).unwrap());
    assert!(tracker.observe_sparse(104, 0).unwrap());
    assert!(!tracker.observe_sparse(104, 0).unwrap());
    assert!(tracker.observe_sparse(104, 1).unwrap());
    assert!(matches!(
        tracker.observe_sparse(103, 0),
        Err(SourceError::Gap(_))
    ));
}

#[test]
fn base_flashblocks_accept_multiple_logs_in_one_index() {
    let mut normalizer = BaseFlashblocksNormalizer::new(8453);
    let header = FlashblockHeader {
        payload_id: [1u8; 32],
        block_number: 42,
        block_hash: Some([2u8; 32]),
        index: 0,
    };
    let make_log = |address: Address| FlashblockLog {
        header: header.clone(),
        transaction_index: 0,
        log_index: 0,
        address,
        topics: Vec::new(),
        data: Vec::new(),
        removed: false,
    };
    assert_eq!(
        normalizer
            .normalize_log(make_log(address(1)))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        normalizer
            .normalize_log(make_log(address(2)))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn heads_promote_commitment_without_regressing_event_cursor() {
    let mut reducer = QuoteReducer::new(QuoteState::default());
    let mut event_cursor = cursor(3);
    event_cursor.block_hash = Some([7u8; 32]);
    reducer.bootstrap(event_cursor.clone());
    reducer
        .observe_head(ChainCursor {
            chain_id: 8453,
            block_number: event_cursor.block_number,
            block_hash: Some([7u8; 32]),
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Finalized,
        })
        .unwrap();
    assert_eq!(reducer.cursor().unwrap().commitment, Commitment::Finalized);
    assert_eq!(reducer.cursor().unwrap().log_index, event_cursor.log_index);

    reducer
        .observe_head(ChainCursor::block(
            8453,
            9,
            Some([8u8; 32]),
            Commitment::Realtime,
        ))
        .unwrap();
    assert_eq!(reducer.cursor().unwrap().block_number, 10);
    assert_eq!(reducer.cursor().unwrap().commitment, Commitment::Finalized);
}
