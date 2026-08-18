use super::*;
use crate::{
    config::Config,
    metrics::Metrics,
    redis_store::{RedisDeployment, RedisQueueLimits},
};
use alloy_primitives::{Address, B256, Bytes};
use lunarbase_client::model::{BlockRef, ChainCursor, Commitment, ContractLog, Network};
use lunarbase_source_evm::{
    fork::{CanonicalWindow, ForkResolver, ForkWindowLimits},
    rpc::{backend::RpcHttpBackend, client::RpcHttpClient},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};

#[test]
fn correction_outside_durable_window_recovers_without_stopping_worker() {
    let mut window = CanonicalWindow::new(ForkWindowLimits {
        max_blocks: 8,
        max_bytes: 1 << 20,
    })
    .unwrap();
    window
        .push_head(block(50, 5, 4, Commitment::Canonical))
        .unwrap();
    let ancestor = block(40, 1, 0, Commitment::Canonical);
    let old_tip = block(41, 2, 1, Commitment::Canonical);
    let new_tip = block(41, 3, 1, Commitment::Canonical);
    let correction = ChainCorrection {
        common_ancestor: ancestor.clone(),
        old_tip: old_tip.clone(),
        new_tip: new_tip.clone(),
        old_branch: vec![old_tip],
        new_branch: vec![new_tip.clone()],
        replacement_logs: Vec::new(),
    };
    let error = validate_durable_branch(&window, &correction).unwrap_err();
    let metrics = Metrics::new(8, 1 << 20, 8, 1 << 20);
    metrics.set_ready(true);

    assert_eq!(
        recovery_transition(&metrics, &new_tip, error),
        Transition::Recover(Some(new_tip))
    );
    assert!(!metrics.is_ready());
    assert!(
        metrics
            .render()
            .contains("lunarbase_event_worker_source_gaps_total 1\n")
    );
}

#[test]
fn exact_duplicate_candidate_can_retry_after_finality_advances() {
    let core = Address::new([0x13; 20]);
    let config = config(
        "redis://127.0.0.1:1".into(),
        "duplicate-finality".into(),
        core,
    );
    let backend = RpcHttpBackend::new(
        RpcHttpClient::new("http://127.0.0.1:1").unwrap(),
        Network::Base,
        config.chain_id,
        "latest",
    );
    let mut runtime = ForkRuntime::new(
        ForkResolver::new(backend, config.fork_max_depth).unwrap(),
        &config,
    )
    .unwrap();
    let ancestor = block(40, 1, 0, Commitment::Canonical);
    let old_tip = block(41, 2, 1, Commitment::Canonical);
    let new_tip = block(41, 3, 1, Commitment::Canonical);
    runtime.window.push_head(ancestor.clone()).unwrap();
    runtime.window.push_head(new_tip.clone()).unwrap();
    let mut finalized = new_tip.clone();
    finalized.cursor.commitment = Commitment::Finalized;
    runtime.window.advance_finalized(finalized.clone()).unwrap();
    runtime.finalized = Some(finalized);
    let correction = ChainCorrection {
        common_ancestor: ancestor,
        old_tip: old_tip.clone(),
        new_tip: new_tip.clone(),
        old_branch: vec![old_tip],
        new_branch: vec![new_tip],
        replacement_logs: Vec::new(),
    };

    assert!(validate_durable_branch(&runtime.window, &correction).unwrap());
    assert!(validate_finality(&runtime, &correction, true).is_ok());
    assert!(validate_finality(&runtime, &correction, false).is_err());
}

#[test]
fn altered_self_correction_is_rejected_before_durable_resolution() {
    let core = Address::new([0x13; 20]);
    let config = config("redis://127.0.0.1:1".into(), "self-correction".into(), core);
    let ancestor = block(40, 1, 0, Commitment::Canonical);
    let current = block(41, 3, 1, Commitment::Canonical);
    let correction = ChainCorrection {
        common_ancestor: ancestor,
        old_tip: current.clone(),
        new_tip: current.clone(),
        old_branch: vec![current.clone()],
        new_branch: vec![current],
        replacement_logs: vec![log(core, 41, 3, 0, 0, 99)],
    };

    assert!(validate_resolution_envelope(&correction, &config).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LUNARBASE_TEST_REDIS_URL with AOF fsync-always"]
async fn resolved_correction_is_durable_idempotent_and_keeps_readiness() {
    let url = std::env::var("LUNARBASE_TEST_REDIS_URL").expect("durable Redis URL");
    let core = Address::new([0x13; 20]);
    let namespace = format!("lunarbase-upstream-correction-{}", std::process::id());
    let config = config(url.clone(), namespace.clone(), core);
    let metrics = Arc::new(Metrics::new(32, 4 << 20, 8, 4 << 20));
    let (store, writer) = RedisEventStore::start(
        url,
        &namespace,
        "upstream-correction".into(),
        RedisDeployment {
            chain_id: config.chain_id,
            core,
            delivery_mode: config.minimum_commitment,
        },
        Duration::from_secs(2),
        RedisQueueLimits {
            capacity: 8,
            byte_capacity: 4 << 20,
        },
        metrics.clone(),
    )
    .unwrap();
    store.initialize().await.unwrap();

    let finalized = block(40, 5, 4, Commitment::Finalized);
    let ancestor = block(41, 6, 5, Commitment::Canonical);
    let old_42 = block(42, 7, 6, Commitment::Canonical);
    let old_43 = block(43, 8, 7, Commitment::Canonical);
    for head in [&finalized, &ancestor, &old_42, &old_43] {
        store
            .append_head(Arc::new(
                crate::event::DurableHead::from_block(head, core).unwrap(),
            ))
            .await
            .unwrap();
    }
    store
        .append_event(Arc::new(
            crate::event::DurableEvent::from_log(&log(core, 42, 7, 0, 0, 20)).unwrap(),
        ))
        .await
        .unwrap();

    let new_42 = block(42, 9, 6, Commitment::Canonical);
    let new_43 = block(43, 10, 9, Commitment::Canonical);
    let correction = ChainCorrection {
        common_ancestor: ancestor,
        old_tip: old_43.clone(),
        new_tip: new_43.clone(),
        old_branch: vec![old_42, old_43],
        new_branch: vec![new_42.clone(), new_43.clone()],
        replacement_logs: vec![log(core, 42, 9, 0, 0, 30)],
    };
    let backend = RpcHttpBackend::new(
        RpcHttpClient::new("http://127.0.0.1:1").unwrap(),
        Network::Base,
        config.chain_id,
        "latest",
    );
    let mut runtime = ForkRuntime::new(
        ForkResolver::new(backend, config.fork_max_depth).unwrap(),
        &config,
    )
    .unwrap();
    runtime.ensure_loaded(&config, &store).await.unwrap();
    metrics.set_ready(true);
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    assert_eq!(
        runtime
            .apply_upstream_correction(
                correction.clone(),
                &config,
                &store,
                &metrics,
                &mut shutdown,
            )
            .await
            .unwrap(),
        Transition::Continue
    );
    assert!(metrics.is_ready());
    assert!(same_block(runtime.window.tip().unwrap(), &new_43));

    let mut promoted = new_42;
    promoted.cursor.commitment = Commitment::Finalized;
    store
        .append_head(Arc::new(
            crate::event::DurableHead::from_block(&promoted, core).unwrap(),
        ))
        .await
        .unwrap();
    runtime.window.advance_finalized(promoted.clone()).unwrap();
    runtime.finalized = Some(promoted);

    assert_eq!(
        runtime
            .apply_upstream_correction(
                correction.clone(),
                &config,
                &store,
                &metrics,
                &mut shutdown,
            )
            .await
            .unwrap(),
        Transition::Continue
    );
    assert!(metrics.is_ready());
    let rendered = metrics.render();
    assert!(rendered.contains("lunarbase_event_worker_source_gaps_total 0\n"));
    assert!(rendered.contains("lunarbase_event_worker_reorg_corrections_total 1\n"));
    assert!(rendered.contains("lunarbase_event_worker_duplicates_total 1\n"));

    let client = redis::Client::open(config.redis_url.clone()).unwrap();
    let stream_length = {
        let mut connection = client.get_connection().unwrap();
        redis::cmd("XLEN")
            .arg(&store.keys().stream)
            .query::<usize>(&mut connection)
            .unwrap()
    };
    let mut altered = correction;
    altered.replacement_logs[0].data = Bytes::from(vec![31; 64]);
    assert_eq!(
        runtime
            .apply_upstream_correction(altered, &config, &store, &metrics, &mut shutdown)
            .await
            .unwrap(),
        Transition::Recover(Some(new_43.clone()))
    );
    assert!(!metrics.is_ready());
    let mut connection = client.get_connection().unwrap();
    let after_altered = redis::cmd("XLEN")
        .arg(&store.keys().stream)
        .query::<usize>(&mut connection)
        .unwrap();
    assert_eq!(after_altered, stream_length);

    let persisted = store
        .load_window(
            config.chain_id,
            config.fork_window_blocks,
            config.fork_window_bytes,
        )
        .await
        .unwrap();
    assert!(same_block(persisted.blocks.last().unwrap(), &new_43));
    let rendered = metrics.render();
    assert!(rendered.contains("lunarbase_event_worker_source_gaps_total 1\n"));
    assert!(rendered.contains("lunarbase_event_worker_reorg_corrections_total 1\n"));
    assert!(rendered.contains("lunarbase_event_worker_duplicates_total 1\n"));
    drop(store);
    writer.join().unwrap();
}

fn config(redis_url: String, redis_namespace: String, core: Address) -> Config {
    Config {
        network: Network::Base,
        chain_id: 8453,
        core,
        deployment_block: 40,
        http_rpc_url: "http://127.0.0.1:1".into(),
        realtime_url: "ws://127.0.0.1:1".into(),
        redis_url,
        redis_namespace,
        consumer_group: "upstream-correction".into(),
        minimum_commitment: Commitment::Canonical,
        bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        source_queue_bound: 32,
        source_queue_byte_bound: 4 << 20,
        backfill_page_blocks: 100,
        fork_window_blocks: 128,
        fork_window_bytes: 1 << 20,
        fork_max_depth: 128,
        correction_event_bound: 128,
        correction_byte_bound: 4 << 20,
        redis_queue_bound: 8,
        redis_queue_byte_bound: 4 << 20,
        reconnect_delay: Duration::from_millis(10),
        source_stall_timeout: Duration::from_secs(1),
        redis_timeout: Duration::from_secs(2),
        #[cfg(all(feature = "monad-native", target_os = "linux"))]
        native_poll_interval: Duration::from_micros(100),
        shutdown_timeout: Duration::from_secs(2),
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
