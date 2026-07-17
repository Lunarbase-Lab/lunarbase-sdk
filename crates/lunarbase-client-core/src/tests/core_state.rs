use super::*;
// Core runtime tests deliberately avoid provider-specific transport setup.
use crate::protocol::codec::decode_fixed_hex32;
use async_trait::async_trait;
use futures_util::stream;
use lunarbase_math::{Address, QuoteState, U256};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

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

