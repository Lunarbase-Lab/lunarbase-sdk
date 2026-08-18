//! Defense-in-depth tests for logs returned by an incorrectly filtered source.

use super::*;
use crate::protocol::abi::lane_discovery_topics;
use crate::state::reducer::ReducerError;

#[test]
fn foreign_valid_core_log_is_rejected_without_state_or_cursor_mutation() {
    let mut indexer = ready_indexer();
    let before = indexer.checkpoint().unwrap();
    let foreign = foreign_lane_removed_log(false);

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Log(foreign)),
        Err(IndexerError::Reducer(ReducerError::ContractAddressMismatch))
    ));
    let after = indexer.checkpoint().unwrap();
    assert_eq!(after.cursor, before.cursor);
    assert_eq!(after.state, before.state);
    assert!(!indexer.reducer.is_ready());
}

#[test]
fn foreign_removed_or_malformed_log_is_rejected_by_address_first() {
    for mut log in [
        foreign_lane_removed_log(true),
        foreign_lane_removed_log(false),
    ] {
        if !log.removed {
            log.cursor.block_number = 100;
            log.cursor.execution_block_number = 100;
        }
        if !log.removed {
            log.topics.truncate(1);
        }
        let mut indexer = ready_indexer();
        assert!(matches!(
            indexer.apply_core_update(ChainUpdate::Log(log)),
            Err(IndexerError::Reducer(ReducerError::ContractAddressMismatch))
        ));
        assert!(!indexer.reducer.is_ready());
    }
}

#[tokio::test]
async fn connected_reducer_does_not_publish_a_foreign_log_to_the_sink() {
    let source = Arc::new(MockSource::new(None));
    let (event_sender, mut event_receiver) = mpsc::channel(1);
    let client =
        ConnectedQuoteClient::connect_with_event_sink(config(), source.clone(), None, event_sender)
            .await
            .unwrap();
    let initial_snapshots = source.snapshot_calls.load(Ordering::Relaxed);

    source.publish(ChainUpdate::Log(foreign_lane_removed_log(false)));
    wait_until(|| source.snapshot_calls.load(Ordering::Relaxed) > initial_snapshots).await;

    assert!(event_receiver.try_recv().is_err());
    client.shutdown().await;
}

#[tokio::test]
async fn foreign_chain_removed_log_is_rejected_before_live_sink_delivery() {
    let source = Arc::new(MockSource::new(None));
    let (event_sender, mut event_receiver) = mpsc::channel(1);
    let client =
        ConnectedQuoteClient::connect_with_event_sink(config(), source.clone(), None, event_sender)
            .await
            .unwrap();
    let initial_snapshots = source.snapshot_calls.load(Ordering::Relaxed);
    let mut log = foreign_lane_removed_log(true);
    log.address = CORE;
    log.cursor.chain_id = 1;

    source.publish(ChainUpdate::Log(log));
    wait_until(|| source.snapshot_calls.load(Ordering::Relaxed) > initial_snapshots).await;

    assert!(event_receiver.try_recv().is_err());
    client.shutdown().await;
}

#[tokio::test]
async fn foreign_chain_backfill_log_is_validated_before_checkpoint_skip() {
    let mut checkpoint = ready_indexer().checkpoint().unwrap();
    checkpoint.cursor.transaction_index = Some(2);
    checkpoint.cursor.log_index = Some(3);
    checkpoint.cursor.commitment = Commitment::Canonical;
    let source = Arc::new(MockSource::new(None));
    let mut skipped = foreign_lane_removed_log(false);
    skipped.address = CORE;
    skipped.cursor.chain_id = 1;
    skipped.cursor.block_number = 100;
    skipped.cursor.execution_block_number = 100;
    source.set_backfill_logs(vec![skipped]);

    let client = ConnectedQuoteClient::connect(config(), source.clone(), Some(checkpoint))
        .await
        .unwrap();

    assert_eq!(source.backfill_calls.load(Ordering::Relaxed), 1);
    assert_eq!(source.snapshot_calls.load(Ordering::Relaxed), 1);
    client.shutdown().await;
}

fn ready_indexer() -> QuoteIndexer {
    let mut indexer = QuoteIndexer::new(QuoteState::default(), config().deployment);
    indexer.bootstrap(snapshot(100)).unwrap();
    indexer
}

fn foreign_lane_removed_log(removed: bool) -> ContractLog {
    let mut event_cursor = cursor(101, Commitment::Realtime);
    event_cursor.transaction_index = Some(0);
    event_cursor.log_index = Some(0);
    ContractLog {
        address: Address::new([0x55; 20]),
        transaction_hash: None,
        topics: vec![
            lane_discovery_topics()[1],
            B256::left_padding_from(ASSET.as_slice()),
        ],
        data: Bytes::new(),
        removed,
        cursor: event_cursor,
    }
}
