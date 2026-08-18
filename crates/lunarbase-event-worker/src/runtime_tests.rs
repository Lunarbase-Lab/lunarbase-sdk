use super::recovery_coverage::recovery_log_is_covered;
use alloy_primitives::{Address, B256, Bytes};
use lunarbase_client::{
    bootstrap::BootstrapSnapshot,
    model::{
        BackfillRequest, BlockRef, ChainCursor, Checkpoint, Commitment, ContractFilter,
        ContractLog, DeploymentConfig, Network, SourceError,
    },
    source::{ChainDataSource, SourceStream},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};

#[test]
fn no_fork_runtime_replays_the_durable_boundary_to_verify_payload_identity() {
    let mut durable = ChainCursor::block(8453, 41, Some(B256::new([6; 32])), Commitment::Canonical);
    durable.transaction_index = Some(2);
    durable.log_index = Some(3);
    let mut earlier = durable.clone();
    earlier.log_index = Some(2);
    let mut previous_block = earlier.clone();
    previous_block.block_number = 40;

    assert!(!recovery_log_is_covered(false, &durable, &durable));
    assert!(!recovery_log_is_covered(false, &earlier, &durable));
    assert!(recovery_log_is_covered(false, &previous_block, &durable));
    assert!(recovery_log_is_covered(true, &durable, &durable));
    assert!(recovery_log_is_covered(true, &earlier, &durable));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LUNARBASE_TEST_REDIS_URL with AOF fsync-always"]
async fn no_fork_recovery_progresses_on_exact_replay_but_not_altered_payload() {
    use crate::{
        event::{DurableEvent, DurableHead},
        metrics::Metrics,
        redis_store::{RedisDeployment, RedisEventStore, RedisQueueLimits},
        runtime::{RecoveryAction, RecoveryState, Transition, recover},
    };
    use tokio::sync::{mpsc, watch};

    let url = std::env::var("LUNARBASE_TEST_REDIS_URL").expect("durable Redis URL");
    let core = Address::new([0x31; 20]);
    let namespace = format!("lunarbase-no-fork-recovery-{}", std::process::id());
    let config = test_config(url.clone(), namespace.clone(), core);
    let metrics = Arc::new(Metrics::new(8, 1 << 20, 8, 1 << 20));
    let (store, writer) = RedisEventStore::start(
        url.clone(),
        &namespace,
        "no-fork-recovery".into(),
        RedisDeployment {
            chain_id: config.chain_id,
            core,
            delivery_mode: Commitment::Canonical,
        },
        Duration::from_secs(2),
        RedisQueueLimits {
            capacity: 8,
            byte_capacity: 1 << 20,
        },
        metrics.clone(),
    )
    .unwrap();
    store.initialize().await.unwrap();

    let head = test_block();
    let exact = test_log(core, 9);
    store
        .append_head(Arc::new(DurableHead::from_block(&head, core).unwrap()))
        .await
        .unwrap();
    store
        .append_event(Arc::new(DurableEvent::from_log(&exact).unwrap()))
        .await
        .unwrap();

    let filter = ContractFilter {
        address: core,
        topics: Vec::new(),
    };
    let (_sender, mut receiver) = mpsc::channel(1);
    let (_active_sender, active) = watch::channel(true);
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    metrics.set_ready(true);
    assert_eq!(
        recover(
            &BoundarySource { log: exact.clone() },
            None,
            None,
            None,
            &config,
            &filter,
            &store,
            &metrics,
            &mut receiver,
            &active,
            &mut shutdown,
        )
        .await
        .unwrap(),
        Transition::Continue
    );
    assert!(metrics.is_ready());

    let altered = test_log(core, 10);
    let altered_transition = recover(
        &BoundarySource { log: altered },
        None,
        None,
        None,
        &config,
        &filter,
        &store,
        &metrics,
        &mut receiver,
        &active,
        &mut shutdown,
    )
    .await
    .unwrap();
    let Transition::Recover(Some(target)) = &altered_transition else {
        panic!("altered durable-boundary payload must retain its coverage target");
    };
    assert_eq!(target.cursor, test_block().cursor);

    let mut recovery = RecoveryState::default();
    assert_eq!(
        recovery.apply(altered_transition, false),
        RecoveryAction::Retry
    );
    assert_eq!(recovery.target(), None);
    assert_eq!(recovery.required(), Some(&test_block().cursor));
    assert!(!metrics.is_ready());
    assert_eq!(
        store.load_cursor(config.chain_id, core).await.unwrap(),
        Some(exact.cursor)
    );
    let client = redis::Client::open(url).unwrap();
    assert_eq!(
        redis::cmd("XLEN")
            .arg(&store.keys().stream)
            .query::<usize>(&mut client.get_connection().unwrap())
            .unwrap(),
        1
    );
    drop(store);
    writer.join().unwrap();
}

struct BoundarySource {
    log: ContractLog,
}

impl ChainDataSource for BoundarySource {
    fn network(&self) -> Network {
        Network::Base
    }

    async fn snapshot(
        &self,
        _deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        Err(SourceError::Unavailable("unused test snapshot".into()))
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        Ok(vec![self.log.clone()])
    }

    async fn subscribe(&self, _filter: ContractFilter) -> Result<SourceStream, SourceError> {
        Err(SourceError::Unavailable("unused test subscription".into()))
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        Ok(test_block().cursor)
    }

    async fn validate_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        Ok(true)
    }
}

fn test_block() -> BlockRef {
    BlockRef::new(
        ChainCursor::block(8453, 41, Some(B256::new([6; 32])), Commitment::Canonical),
        Some(B256::new([5; 32])),
    )
}

fn test_log(core: Address, payload: u8) -> ContractLog {
    ContractLog {
        address: core,
        transaction_hash: Some(B256::new([7; 32])),
        topics: vec![B256::new([8; 32])],
        data: Bytes::from(vec![payload; 64]),
        removed: false,
        cursor: ChainCursor {
            transaction_index: Some(2),
            log_index: Some(3),
            ..test_block().cursor
        },
    }
}

fn test_config(redis_url: String, redis_namespace: String, core: Address) -> crate::config::Config {
    crate::config::Config {
        network: Network::Base,
        chain_id: 8453,
        core,
        deployment_block: 41,
        http_rpc_url: "http://127.0.0.1:1".into(),
        realtime_url: "ws://127.0.0.1:1".into(),
        redis_url,
        redis_namespace,
        consumer_group: "no-fork-recovery".into(),
        minimum_commitment: Commitment::Canonical,
        bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        source_queue_bound: 8,
        source_queue_byte_bound: 1 << 20,
        backfill_page_blocks: 100,
        fork_window_blocks: 128,
        fork_window_bytes: 1 << 20,
        fork_max_depth: 128,
        correction_event_bound: 128,
        correction_byte_bound: 1 << 20,
        redis_queue_bound: 8,
        redis_queue_byte_bound: 1 << 20,
        reconnect_delay: Duration::from_millis(10),
        source_stall_timeout: Duration::from_secs(1),
        redis_timeout: Duration::from_secs(2),
        #[cfg(all(feature = "monad-native", target_os = "linux"))]
        native_poll_interval: Duration::from_micros(100),
        shutdown_timeout: Duration::from_secs(2),
    }
}
