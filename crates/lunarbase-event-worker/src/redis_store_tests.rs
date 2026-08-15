use super::{RedisEventStore, RedisKeys, RedisQueueLimits, StoreError};
use crate::{event::DurableEvent, metrics::Metrics};
use alloy_primitives::{Address, B256, Bytes};
use lunarbase_client::model::{ChainCursor, Commitment, ContractLog};
use std::{sync::Arc, time::Duration};

#[test]
fn deployment_keys_share_one_cluster_hash_slot() {
    let keys = RedisKeys::new("lunarbase", 8453, Address::new([3; 20]));
    let tag = "{8453:0x0303030303030303030303030303030303030303}";
    assert!(keys.stream.contains(tag));
    assert!(keys.cursor.contains(tag));
    assert!(keys.cursor_order.contains(tag));
    assert!(keys.event_ids.contains(tag));
    assert!(keys.metadata.contains(tag));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LUNARBASE_TEST_REDIS_URL with AOF fsync-always"]
async fn durable_redis_append_is_atomic_and_idempotent() {
    let url = std::env::var("LUNARBASE_TEST_REDIS_URL").expect("durable Redis URL");
    let core = Address::new([3; 20]);
    let metrics = Arc::new(Metrics::new(8, 1024 * 1024, 8, 1024 * 1024));
    let namespace = format!("lunarbase-test-{}", std::process::id());
    let (store, writer) = RedisEventStore::start(
        url.clone(),
        &namespace,
        "integration-consumers".into(),
        8453,
        core,
        Duration::from_secs(2),
        queue_limits(),
        metrics,
    )
    .unwrap();
    store.initialize().await.unwrap();

    let applied_log = log(core, false);
    let applied = Arc::new(DurableEvent::from_log(&applied_log).unwrap());
    let first = store.append(applied.clone()).await.unwrap();
    let duplicate = store.append(applied).await.unwrap();
    assert!(first.appended);
    assert!(!duplicate.appended);
    assert_eq!(first.stream_id, duplicate.stream_id);

    let removed = Arc::new(DurableEvent::from_log(&log(core, true)).unwrap());
    assert!(store.append(removed).await.unwrap().appended);
    assert_eq!(
        store.load_cursor(8453, core).await.unwrap(),
        Some(applied_log.cursor.clone())
    );

    let client = redis::Client::open(url.clone()).unwrap();
    let mut connection = client.get_connection().unwrap();
    let stream_length = redis::cmd("XLEN")
        .arg(&store.keys().stream)
        .query::<usize>(&mut connection)
        .unwrap();
    assert_eq!(stream_length, 2);

    drop(store);
    writer.join().unwrap();

    let restarted_metrics = Arc::new(Metrics::new(8, 1024 * 1024, 8, 1024 * 1024));
    let (restarted, restarted_writer) = RedisEventStore::start(
        url,
        &namespace,
        "integration-consumers".into(),
        8453,
        core,
        Duration::from_secs(2),
        queue_limits(),
        restarted_metrics,
    )
    .unwrap();
    restarted.initialize().await.unwrap();
    assert_eq!(
        restarted.load_cursor(8453, core).await.unwrap(),
        Some(applied_log.cursor.clone())
    );
    let replay = Arc::new(DurableEvent::from_log(&applied_log).unwrap());
    let replayed = restarted.append(replay).await.unwrap();
    assert!(!replayed.appended);
    assert_eq!(replayed.stream_id, first.stream_id);

    drop(restarted);
    restarted_writer.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LUNARBASE_TEST_UNSAFE_REDIS_URL without AOF fsync-always"]
async fn redis_without_fsync_always_is_rejected() {
    let url = std::env::var("LUNARBASE_TEST_UNSAFE_REDIS_URL").expect("unsafe Redis URL");
    let metrics = Arc::new(Metrics::new(8, 1024 * 1024, 8, 1024 * 1024));
    let (store, writer) = RedisEventStore::start(
        url,
        "lunarbase-unsafe-test",
        "integration-consumers".into(),
        8453,
        Address::new([4; 20]),
        Duration::from_secs(2),
        queue_limits(),
        metrics,
    )
    .unwrap();
    assert!(matches!(
        store.initialize().await.unwrap_err(),
        StoreError::Durability(_)
    ));
    drop(store);
    writer.join().unwrap();
}

fn log(core: Address, removed: bool) -> ContractLog {
    ContractLog {
        address: core,
        transaction_hash: Some(B256::new([7; 32])),
        topics: vec![B256::new([8; 32])],
        data: Bytes::from_static(&[9; 64]),
        removed,
        cursor: ChainCursor {
            chain_id: 8453,
            block_number: 41,
            execution_block_number: 41,
            block_hash: Some(B256::new([6; 32])),
            transaction_index: Some(2),
            log_index: Some(3),
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Canonical,
        },
    }
}

fn queue_limits() -> RedisQueueLimits {
    RedisQueueLimits {
        capacity: 8,
        byte_capacity: 1024 * 1024,
    }
}
