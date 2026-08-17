//! Redis-backed lifecycle correction integration coverage.

use super::{CorrectionLimits, RedisDeployment, RedisEventStore, RedisQueueLimits};
use crate::{
    event::{DurableEvent, DurableHead, ReorgCorrection},
    metrics::Metrics,
};
use alloy_primitives::{Address, B256, Bytes};
use lunarbase_client::model::{BlockRef, ChainCursor, Commitment, ContractLog};
use lunarbase_source_evm::fork::ForkResolution;
use std::{sync::Arc, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LUNARBASE_TEST_REDIS_URL with AOF fsync-always"]
async fn correction_is_atomic_ordered_and_idempotent() {
    let url = std::env::var("LUNARBASE_TEST_REDIS_URL").expect("durable Redis URL");
    let core = Address::new([13; 20]);
    let namespace = format!("lunarbase-reorg-test-{}", std::process::id());
    let metrics = Arc::new(Metrics::new(32, 4 * 1024 * 1024, 8, 4 * 1024 * 1024));
    let (store, writer) = RedisEventStore::start(
        url.clone(),
        &namespace,
        "reorg-consumers".into(),
        RedisDeployment {
            chain_id: 8453,
            core,
            delivery_mode: Commitment::Canonical,
        },
        Duration::from_secs(2),
        RedisQueueLimits {
            capacity: 8,
            byte_capacity: 4 * 1024 * 1024,
        },
        metrics,
    )
    .unwrap();
    store.initialize().await.unwrap();

    let finalized = block(40, 5, 4, Commitment::Finalized);
    let ancestor = block(41, 6, 5, Commitment::Canonical);
    let old_42 = block(42, 7, 6, Commitment::Canonical);
    let old_43 = block(43, 8, 7, Commitment::Canonical);
    for head in [&finalized, &ancestor, &old_42, &old_43] {
        store
            .append_head(Arc::new(DurableHead::from_block(head, core).unwrap()))
            .await
            .unwrap();
    }

    let old_logs = [
        log(core, 42, 7, 0, 0, 20),
        log(core, 42, 7, 0, 1, 21),
        log(core, 43, 8, 1, 0, 22),
    ];
    let mut logical = Vec::with_capacity(old_logs.len());
    for log in &old_logs {
        let event = Arc::new(DurableEvent::from_log(log).unwrap());
        logical.push(event.logical_log_id.clone());
        store.append_event(event).await.unwrap();
    }

    let new_42 = block(42, 9, 6, Commitment::Canonical);
    let new_43 = block(43, 10, 9, Commitment::Canonical);
    let mut correction_finalized = ancestor.clone();
    correction_finalized.cursor.commitment = Commitment::Finalized;
    store
        .append_head(Arc::new(
            DurableHead::from_block(&correction_finalized, core).unwrap(),
        ))
        .await
        .unwrap();
    let resolution = ForkResolution {
        common_ancestor: ancestor,
        old_tip: old_43.clone(),
        new_tip: new_43.clone(),
        old_branch: vec![old_42, old_43],
        new_branch: vec![new_42, new_43],
    };
    let correction = Arc::new(
        ReorgCorrection::new(
            &resolution,
            correction_finalized,
            vec![log(core, 42, 9, 0, 0, 30), log(core, 42, 9, 0, 1, 31)],
            core,
        )
        .unwrap(),
    );
    let limits = CorrectionLimits {
        max_events: 16,
        max_bytes: 4 * 1024 * 1024,
    };
    let first = store.correct(correction.clone(), limits).await.unwrap();
    assert!(first.appended);
    assert_eq!((first.reverted, first.applied), (3, 2));
    let replay = store.correct(correction, limits).await.unwrap();
    assert!(!replay.appended);
    assert_eq!(replay.stream_id, first.stream_id);

    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_connection().unwrap();
    let records = redis::cmd("XRANGE")
        .arg(&store.keys().stream)
        .arg("-")
        .arg("+")
        .query::<redis::streams::StreamRangeReply>(&mut connection)
        .unwrap();
    let operations = records
        .ids
        .iter()
        .map(|entry| entry.get::<String>("operation").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        operations,
        [
            "applied", "applied", "applied", "begin", "reverted", "reverted", "reverted",
            "applied", "applied", "commit",
        ]
    );
    let reverted = &records.ids[4..7];
    assert_eq!(
        reverted
            .iter()
            .map(|entry| entry.get::<String>("logicalLogId").unwrap())
            .collect::<Vec<_>>(),
        vec![logical[2].clone(), logical[1].clone(), logical[0].clone()]
    );
    assert!(reverted.iter().all(|entry| {
        entry.get::<String>("lifecycleRevision").as_deref() == Some("2")
            && entry.get::<String>("reorgId").is_some()
    }));
    assert_eq!(
        redis::cmd("GET")
            .arg(&store.keys().canonical_head)
            .query::<String>(&mut connection)
            .unwrap(),
        format!("{:#x}", B256::new([10; 32]))
    );
    assert_eq!(
        redis::cmd("EXISTS")
            .arg(&store.keys().reorg_manifest)
            .query::<usize>(&mut connection)
            .unwrap(),
        0
    );
    for hash in [7_u8, 8] {
        assert_eq!(
            redis::cmd("EXISTS")
                .arg(
                    store
                        .keys()
                        .block_logs(&format!("{:#x}", B256::new([hash; 32])))
                )
                .query::<usize>(&mut connection)
                .unwrap(),
            0
        );
    }
    let restored = store.load_window(8453, 16, 1024 * 1024).await.unwrap();
    assert_eq!(
        restored
            .blocks
            .iter()
            .map(|block| block.cursor.block_hash.unwrap())
            .collect::<Vec<_>>(),
        vec![
            B256::new([5; 32]),
            B256::new([6; 32]),
            B256::new([9; 32]),
            B256::new([10; 32]),
        ]
    );
    assert_eq!(
        restored.finalized.unwrap().cursor.block_hash,
        Some(B256::new([6; 32]))
    );
    let cursor = store.load_cursor(8453, core).await.unwrap().unwrap();
    assert_eq!(cursor.block_hash, Some(B256::new([10; 32])));
    assert_eq!(cursor.transaction_index, Some(u32::MAX));
    assert_eq!(cursor.log_index, Some(u32::MAX));

    drop(store);
    writer.join().unwrap();
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
    transaction_index: u32,
    log_index: u32,
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
            transaction_index: Some(transaction_index),
            log_index: Some(log_index),
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Canonical,
        },
    }
}
