use super::{RedisDeployment, RedisEventStore, RedisKeys, RedisQueueLimits, StoreError};
use crate::{
    event::{DurableEvent, DurableHead},
    metrics::Metrics,
};
use alloy_primitives::{Address, B256, Bytes};
use lunarbase_client::model::{BlockRef, ChainCursor, Commitment, ContractLog};
use std::{sync::Arc, time::Duration};

#[test]
fn deployment_keys_share_one_cluster_hash_slot() {
    let keys = RedisKeys::new("lunarbase", 8453, Address::new([3; 20]));
    let tag = "{8453:0x0303030303030303030303030303030303030303}";
    assert!(keys.stream.contains(tag));
    assert!(keys.cursor.contains(tag));
    assert!(keys.cursor_order.contains(tag));
    assert!(keys.resume.contains(tag));
    assert!(keys.record_ids.contains(tag));
    assert!(keys.log_state.contains(tag));
    assert!(keys.headers.contains(tag));
    assert!(keys.canonical_height.contains(tag));
    assert!(keys.canonical_head.contains(tag));
    assert!(keys.finalized_head.contains(tag));
    assert!(keys.reorg_manifest.contains(tag));
    assert!(keys.journal_usage.contains(tag));
    assert!(keys.metadata.contains(tag));
    assert!(keys.block_logs("0x01").contains(tag));
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
        deployment(core, Commitment::Canonical),
        Duration::from_secs(2),
        queue_limits(),
        metrics,
    )
    .unwrap();
    store.initialize().await.unwrap();

    let head = Arc::new(DurableHead::from_block(&block(), core).unwrap());
    let first_head = store.append_head(head.clone()).await.unwrap();
    let duplicate_head = store.append_head(head).await.unwrap();
    assert!(first_head.appended);
    assert!(!duplicate_head.appended);

    assert_eq!(
        store.load_cursor(8453, core).await.unwrap(),
        None,
        "a header alone must not skip unpersisted logs during recovery"
    );
    let mut competing = block();
    competing.cursor.block_hash = Some(B256::new([4; 32]));
    let competing = Arc::new(DurableHead::from_block(&competing, core).unwrap());
    assert!(matches!(
        store.append_head(competing).await.unwrap_err(),
        StoreError::Journal(_)
    ));

    let applied_log = log(core);
    let applied = Arc::new(DurableEvent::from_log(&applied_log).unwrap());
    let first = store.append_event(applied.clone()).await.unwrap();
    let duplicate = store.append_event(applied).await.unwrap();
    assert!(first.appended);
    assert!(!duplicate.appended);
    assert_eq!(first.stream_id, duplicate.stream_id);

    let mut altered_log = applied_log.clone();
    altered_log.cursor.execution_block_number = 99;
    altered_log.topics[0] = B256::new([0x42; 32]);
    altered_log.data = Bytes::from_static(&[0x43; 64]);
    let altered = Arc::new(DurableEvent::from_log(&altered_log).unwrap());
    let error = store.append_event(altered).await.unwrap_err();
    assert!(matches!(
        error,
        StoreError::Journal(detail) if detail.contains("LUNARBASE_LOG_ALREADY_ACTIVE")
    ));

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
    assert_eq!(stream_length, 1);
    let entries = redis::cmd("XRANGE")
        .arg(&store.keys().stream)
        .arg("-")
        .arg("+")
        .query::<redis::streams::StreamRangeReply>(&mut connection)
        .unwrap();
    let entry = entries.ids.first().unwrap();
    assert_eq!(entry.get::<String>("schemaVersion").as_deref(), Some("2"));
    assert_eq!(
        entry.get::<String>("lifecycleRevision").as_deref(),
        Some("1")
    );
    assert!(!entry.contains_key("rawLog"));
    assert!(!entry.contains_key("eventName"));
    let stored_header = redis::cmd("HGET")
        .arg(&store.keys().headers)
        .arg(format!("{:#x}", B256::new([6; 32])))
        .query::<Option<String>>(&mut connection)
        .unwrap();
    assert!(stored_header.is_some());
    let header_count = redis::cmd("HLEN")
        .arg(&store.keys().headers)
        .query::<usize>(&mut connection)
        .unwrap();
    assert_eq!(header_count, 1);
    let canonical_hash = redis::cmd("HGET")
        .arg(&store.keys().canonical_height)
        .arg(41)
        .query::<String>(&mut connection)
        .unwrap();
    assert_eq!(canonical_hash, format!("{:#x}", B256::new([6; 32])));
    let block_log_count = redis::cmd("LLEN")
        .arg(store.keys().block_logs(&canonical_hash))
        .query::<usize>(&mut connection)
        .unwrap();
    assert_eq!(block_log_count, 1);

    drop(store);
    writer.join().unwrap();

    let restarted_metrics = Arc::new(Metrics::new(8, 1024 * 1024, 8, 1024 * 1024));
    let (restarted, restarted_writer) = RedisEventStore::start(
        url,
        &namespace,
        "integration-consumers".into(),
        deployment(core, Commitment::Canonical),
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
    let replayed = restarted.append_event(replay).await.unwrap();
    assert!(!replayed.appended);
    assert_eq!(replayed.stream_id, first.stream_id);

    drop(restarted);
    restarted_writer.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LUNARBASE_TEST_REDIS_URL with AOF fsync-always"]
async fn finalized_watermark_can_skip_empty_intermediate_blocks() {
    let url = std::env::var("LUNARBASE_TEST_REDIS_URL").expect("durable Redis URL");
    let core = Address::new([9; 20]);
    let metrics = Arc::new(Metrics::new(8, 1024 * 1024, 8, 1024 * 1024));
    let namespace = format!("lunarbase-finalized-test-{}", std::process::id());
    let (store, writer) = RedisEventStore::start(
        url,
        &namespace,
        "integration-consumers".into(),
        deployment(core, Commitment::Finalized),
        Duration::from_secs(2),
        queue_limits(),
        metrics,
    )
    .unwrap();
    store.initialize().await.unwrap();

    for block in [
        block_at(41, 6, 5, Commitment::Finalized),
        block_at(48, 9, 8, Commitment::Finalized),
    ] {
        let head = Arc::new(DurableHead::from_block(&block, core).unwrap());
        assert!(store.append_head(head).await.unwrap().appended);
    }

    let client =
        redis::Client::open(std::env::var("LUNARBASE_TEST_REDIS_URL").expect("durable Redis URL"))
            .unwrap();
    let canonical = redis::cmd("GET")
        .arg(&store.keys().canonical_head)
        .query::<String>(&mut client.get_connection().unwrap())
        .unwrap();
    assert_eq!(canonical, format!("{:#x}", B256::new([9; 32])));

    drop(store);
    writer.join().unwrap();
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
        deployment(Address::new([4; 20]), Commitment::Canonical),
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

fn block() -> BlockRef {
    block_at(41, 6, 5, Commitment::Canonical)
}

fn block_at(number: u64, hash: u8, parent: u8, commitment: Commitment) -> BlockRef {
    BlockRef::new(
        ChainCursor::block(8453, number, Some(B256::new([hash; 32])), commitment),
        Some(B256::new([parent; 32])),
    )
}

fn log(core: Address) -> ContractLog {
    ContractLog {
        address: core,
        transaction_hash: Some(B256::new([7; 32])),
        topics: vec![B256::new([8; 32])],
        data: Bytes::from_static(&[9; 64]),
        removed: false,
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

fn deployment(core: Address, delivery_mode: Commitment) -> RedisDeployment {
    RedisDeployment {
        chain_id: 8453,
        core,
        delivery_mode,
    }
}

fn queue_limits() -> RedisQueueLimits {
    RedisQueueLimits {
        capacity: 8,
        byte_capacity: 1024 * 1024,
    }
}
