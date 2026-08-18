use super::*;
use alloy_primitives::{Address, B256, Bytes};
use lunarbase_client::{
    bootstrap::BootstrapSnapshot,
    model::{
        BackfillRequest, ChainCursor, Checkpoint, ContractLog, DeploymentConfig, Network,
        SourceError,
    },
    source::{ChainDataSource, SourceStream},
};
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[test]
fn oversized_resolution_fails_before_durable_commit_and_revokes_readiness() {
    let limits = ForkWindowLimits {
        max_blocks: 2,
        max_bytes: 1 << 20,
    };
    let mut window = CanonicalWindow::new(limits).unwrap();
    let ancestor = block(40, 1, 0);
    let old_tip = block(41, 2, 1);
    window.push_head(ancestor.clone()).unwrap();
    window.push_head(old_tip.clone()).unwrap();
    let new_41 = block(41, 3, 1);
    let new_42 = block(42, 4, 3);
    let resolution = ForkResolution {
        common_ancestor: ancestor,
        old_tip: old_tip.clone(),
        new_tip: new_42.clone(),
        old_branch: vec![old_tip.clone()],
        new_branch: vec![new_41, new_42],
    };
    let metrics = Metrics::new(8, 1 << 20, 8, 1 << 20);
    metrics.set_ready(true);

    let error = resolved_window_for_recovery(&window, &resolution, &metrics).unwrap_err();
    assert!(matches!(&error, RuntimeError::Fork(ForkError::BlockBudget)));
    assert!(error.retryable_recovery());
    assert!(same_block(window.tip().unwrap(), &old_tip));
    assert!(!metrics.is_ready());
    assert!(
        metrics
            .render()
            .contains("lunarbase_event_worker_source_gaps_total 1\n")
    );
}

#[tokio::test]
async fn replacement_backfill_stops_at_the_first_over_budget_page() {
    let core = Address::new([0x13; 20]);
    let mut config = config(core);
    config.backfill_page_blocks = 1;
    config.correction_event_bound = 4;
    let source = PagedSource {
        core,
        calls: AtomicUsize::new(0),
        backing_bytes: 64,
        visible_bytes: 64,
    };
    let ancestor = block(40, 1, 0);
    let old_tip = block(41, 0x80, 1);
    let new_branch = (41..=49)
        .map(|number| block(number, number as u8, number.saturating_sub(1) as u8))
        .collect::<Vec<_>>();
    let resolution = ForkResolution {
        common_ancestor: ancestor,
        old_tip: old_tip.clone(),
        new_tip: new_branch.last().unwrap().clone(),
        old_branch: vec![old_tip],
        new_branch,
    };
    let filter = ContractFilter {
        address: core,
        topics: Vec::new(),
    };

    let error = replacement_logs(&source, &resolution, &config, &filter, 0)
        .await
        .unwrap_err();
    assert!(matches!(
        &error,
        RuntimeError::Store(StoreError::CorrectionBudget(_))
    ));
    assert_eq!(source.calls.load(Ordering::Relaxed), 3);

    let metrics = Metrics::new(8, 1 << 20, 8, 1 << 20);
    metrics.set_ready(true);
    let classified = classify_correction_error(&metrics, error);
    assert!(matches!(classified, RuntimeError::RecoveryLog(_)));
    assert!(classified.retryable_recovery());
    assert!(!metrics.is_ready());
}

#[tokio::test]
async fn replacement_backfill_retains_only_the_visible_tail_slice() {
    let core = Address::new([0x13; 20]);
    let mut config = config(core);
    config.backfill_page_blocks = 1;
    config.correction_event_bound = 8;
    config.correction_byte_bound = 4096;
    let source = PagedSource {
        core,
        calls: AtomicUsize::new(0),
        backing_bytes: 1 << 20,
        visible_bytes: 1,
    };
    let ancestor = block(40, 1, 0);
    let replacement = block(41, 41, 1);
    let resolution = ForkResolution {
        common_ancestor: ancestor,
        old_tip: block(41, 2, 1),
        new_tip: replacement.clone(),
        old_branch: vec![block(41, 2, 1)],
        new_branch: vec![replacement],
    };
    let filter = ContractFilter {
        address: core,
        topics: Vec::new(),
    };

    let logs = replacement_logs(&source, &resolution, &config, &filter, 0)
        .await
        .unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].data.as_ref(), [0x42]);
    let data: Vec<u8> = logs.into_iter().next().unwrap().data.into();
    assert_eq!(data.capacity(), data.len());
}

struct PagedSource {
    core: Address,
    calls: AtomicUsize,
    backing_bytes: usize,
    visible_bytes: usize,
}

impl ChainDataSource for PagedSource {
    fn network(&self) -> Network {
        Network::Base
    }

    async fn snapshot(
        &self,
        _deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        Err(unused())
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let number = request.from_block;
        let backing = Bytes::from(vec![0x42; self.backing_bytes]);
        let data = backing.slice(self.backing_bytes - self.visible_bytes..);
        Ok(vec![ContractLog {
            address: self.core,
            transaction_hash: Some(hash(number.saturating_add(100))),
            topics: vec![hash(number.saturating_add(200))],
            data,
            removed: false,
            cursor: ChainCursor {
                transaction_index: Some(0),
                log_index: Some(0),
                ..ChainCursor::block(
                    8453,
                    number,
                    Some(B256::new([number as u8; 32])),
                    Commitment::Canonical,
                )
            },
        }])
    }

    async fn subscribe(&self, _filter: ContractFilter) -> Result<SourceStream, SourceError> {
        Err(unused())
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        Err(unused())
    }

    async fn validate_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        Ok(false)
    }
}

fn config(core: Address) -> Config {
    Config {
        network: Network::Base,
        chain_id: 8453,
        core,
        deployment_block: 40,
        http_rpc_url: "http://127.0.0.1:1".into(),
        realtime_url: "ws://127.0.0.1:1".into(),
        redis_url: "redis://127.0.0.1:1".into(),
        redis_namespace: "local-atomicity".into(),
        consumer_group: "local-atomicity".into(),
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

fn block(number: u64, block_hash: u8, parent_hash: u8) -> BlockRef {
    BlockRef::new(
        ChainCursor::block(
            8453,
            number,
            Some(B256::new([block_hash; 32])),
            Commitment::Canonical,
        ),
        Some(B256::new([parent_hash; 32])),
    )
}

fn hash(value: u64) -> B256 {
    B256::left_padding_from(&value.to_be_bytes())
}

fn unused() -> SourceError {
    SourceError::Unavailable("unused test source operation".into())
}
