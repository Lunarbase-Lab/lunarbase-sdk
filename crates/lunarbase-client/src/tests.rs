//! Cross-module embeddable-runtime acceptance tests.

use crate::bootstrap::BootstrapSnapshot;
use crate::indexer::client::ConnectedQuoteClient;
use crate::indexer::client_types::{ClientConnectConfig, CoreEventSinkPolicy};
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::IndexerError;
use crate::model::{
    BackfillRequest, BlockRef, ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractFilter,
    ContractLog, DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network, SourceError,
};
use crate::protocol::abi::{TOPIC_LANE_ADDED, quote_critical_topics};
use crate::source::{ChainDataSource, SourceStream};
use crate::state::reducer::QuoteReducer;
use lunarbase_math::arithmetic::WAD;
use lunarbase_math::slot0::{LaneSlot0, encode_lane_slot0};
use lunarbase_math::{Address, B256, Bytes, LaneState, QuoteMode, QuoteRequest, QuoteState, U256};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, broadcast, mpsc};

const CASH: Address = Address::new([1; 20]);
const ASSET: Address = Address::new([2; 20]);
const ROUTER: Address = Address::new([3; 20]);
const CORE: Address = Address::new([4; 20]);

struct MockSource {
    snapshot_calls: AtomicUsize,
    backfill_calls: AtomicUsize,
    subscribe_calls: AtomicUsize,
    canonical_calls: AtomicUsize,
    validate_calls: AtomicUsize,
    checkpoint_valid: AtomicBool,
    snapshot_block: AtomicUsize,
    snapshot_gate: Mutex<Option<Arc<Notify>>>,
    backfill_logs: Mutex<Vec<ContractLog>>,
    events: broadcast::Sender<ChainUpdate>,
    snapshot: BootstrapSnapshot,
}

impl MockSource {
    fn new(snapshot_gate: Option<Arc<Notify>>) -> Self {
        let (events, _) = broadcast::channel(32);
        Self {
            snapshot_calls: AtomicUsize::new(0),
            backfill_calls: AtomicUsize::new(0),
            subscribe_calls: AtomicUsize::new(0),
            canonical_calls: AtomicUsize::new(0),
            validate_calls: AtomicUsize::new(0),
            checkpoint_valid: AtomicBool::new(true),
            snapshot_block: AtomicUsize::new(100),
            snapshot_gate: Mutex::new(snapshot_gate),
            backfill_logs: Mutex::new(Vec::new()),
            events,
            snapshot: snapshot(100),
        }
    }

    fn publish(&self, update: ChainUpdate) {
        let _ = self.events.send(update);
    }

    fn set_backfill_logs(&self, logs: Vec<ContractLog>) {
        *self.backfill_logs.lock().unwrap() = logs;
    }

    fn set_snapshot_block(&self, block: usize) {
        self.snapshot_block.store(block, Ordering::Relaxed);
    }

    fn set_snapshot_gate(&self, gate: Arc<Notify>) {
        *self.snapshot_gate.lock().unwrap() = Some(gate);
    }
}

impl ChainDataSource for MockSource {
    fn network(&self) -> Network {
        Network::Base
    }

    async fn snapshot(
        &self,
        _deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        self.snapshot_calls.fetch_add(1, Ordering::Relaxed);
        let snapshot_gate = self.snapshot_gate.lock().unwrap().clone();
        if let Some(gate) = snapshot_gate {
            gate.notified().await;
        }
        let mut snapshot = self.snapshot.clone();
        let block = self.snapshot_block.load(Ordering::Relaxed) as u64;
        snapshot.cursor.block_number = block;
        snapshot.cursor.execution_block_number = block;
        Ok(snapshot)
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.backfill_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.backfill_logs.lock().unwrap().clone())
    }

    async fn subscribe(&self, _filter: ContractFilter) -> Result<SourceStream, SourceError> {
        self.subscribe_calls.fetch_add(1, Ordering::Relaxed);
        let mut receiver = self.events.subscribe();
        Ok(Box::pin(async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok(update) => yield Ok(update),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        yield Ok(ChainUpdate::Gap {
                            cursor: None,
                            reason: format!("mock stream lagged by {skipped}"),
                        });
                        return;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }))
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        self.canonical_calls.fetch_add(1, Ordering::Relaxed);
        Ok(cursor(
            self.snapshot_block.load(Ordering::Relaxed) as u64,
            Commitment::Finalized,
        ))
    }

    async fn validate_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        self.validate_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.checkpoint_valid.load(Ordering::Relaxed))
    }
}

#[tokio::test]
async fn valid_checkpoint_skips_snapshot_and_invalid_checkpoint_does_not() {
    let first_source = Arc::new(MockSource::new(None));
    let first = ConnectedQuoteClient::connect(config(), first_source, None)
        .await
        .unwrap();
    let checkpoint = first.checkpoint().unwrap().unwrap();
    first.shutdown().await;

    let valid_source = Arc::new(MockSource::new(None));
    let valid =
        ConnectedQuoteClient::connect(config(), valid_source.clone(), Some(checkpoint.clone()))
            .await
            .unwrap();
    assert_eq!(valid_source.snapshot_calls.load(Ordering::Relaxed), 0);
    assert_eq!(valid_source.validate_calls.load(Ordering::Relaxed), 1);
    valid.shutdown().await;

    let invalid_source = Arc::new(MockSource::new(None));
    invalid_source
        .checkpoint_valid
        .store(false, Ordering::Relaxed);
    let invalid = ConnectedQuoteClient::connect(config(), invalid_source.clone(), Some(checkpoint))
        .await
        .unwrap();
    assert_eq!(invalid_source.snapshot_calls.load(Ordering::Relaxed), 1);
    invalid.shutdown().await;
}

#[tokio::test]
async fn checkpoint_recovery_delivers_each_backfill_log_once() {
    let checkpoint = checkpoint_before_recovery().await;
    let source = Arc::new(MockSource::new(None));
    let recovered_log = unknown_log(100);
    source.set_backfill_logs(vec![recovered_log.clone()]);
    let (event_sender, mut event_receiver) = mpsc::channel(2);

    let client = ConnectedQuoteClient::connect_with_event_sink(
        config(),
        source.clone(),
        Some(checkpoint),
        event_sender,
    )
    .await
    .unwrap();

    let delivered = tokio::time::timeout(Duration::from_secs(1), event_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered, recovered_log);
    assert!(event_receiver.try_recv().is_err());
    assert_eq!(source.backfill_calls.load(Ordering::Relaxed), 1);
    assert_eq!(source.snapshot_calls.load(Ordering::Relaxed), 0);
    client.shutdown().await;
}

#[tokio::test]
async fn closed_event_observer_does_not_block_checkpoint_recovery() {
    let checkpoint = checkpoint_before_recovery().await;
    let source = Arc::new(MockSource::new(None));
    source.set_backfill_logs(vec![unknown_log(100)]);
    let (event_sender, event_receiver) = mpsc::channel(1);
    drop(event_receiver);

    let client = ConnectedQuoteClient::connect_with_event_sink(
        config(),
        source.clone(),
        Some(checkpoint),
        event_sender,
    )
    .await
    .unwrap();

    assert_eq!(client.runtime_stats().event_observer_drops, 1);
    assert!(client.is_ready());
    assert_eq!(source.backfill_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        source.snapshot_calls.load(Ordering::Relaxed),
        0,
        "observer closure must not fall back to a snapshot"
    );
    client.shutdown().await;
}

async fn checkpoint_before_recovery() -> Checkpoint {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source, None)
        .await
        .unwrap();
    let mut checkpoint = client.checkpoint().unwrap().unwrap();
    checkpoint.cursor.block_number = 99;
    checkpoint.cursor.execution_block_number = 99;
    client.shutdown().await;
    checkpoint
}

#[tokio::test]
async fn subscription_event_during_snapshot_is_applied_after_handoff() {
    let gate = Arc::new(Notify::new());
    let source = Arc::new(MockSource::new(Some(gate.clone())));
    let (event_sender, mut event_receiver) = mpsc::channel(4);
    let task = tokio::spawn(ConnectedQuoteClient::connect_with_event_sink(
        config(),
        source.clone(),
        None,
        event_sender,
    ));
    wait_until(|| source.subscribe_calls.load(Ordering::Relaxed) == 1).await;
    let log = unknown_log(101);
    source.publish(ChainUpdate::Log(log.clone()));
    source.publish(ChainUpdate::Head(BlockRef::new(
        cursor(101, Commitment::Realtime),
        None,
    )));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), event_receiver.recv())
            .await
            .is_err(),
        "single-writer sink must wait for bootstrap ordering"
    );
    wait_until(|| source.events.receiver_count() > 0).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    gate.notify_waiters();
    let client = task.await.unwrap().unwrap();
    let delivered = tokio::time::timeout(Duration::from_secs(1), event_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered, log);
    assert_eq!(client.health().unwrap().cursor.unwrap().block_number, 101);
    client.shutdown().await;
}

#[tokio::test]
async fn closed_event_observer_does_not_abort_bootstrap() {
    let gate = Arc::new(Notify::new());
    let source = Arc::new(MockSource::new(Some(gate.clone())));
    let (event_sender, event_receiver) = mpsc::channel(1);
    drop(event_receiver);
    let task = tokio::spawn(ConnectedQuoteClient::connect_with_event_sink(
        config(),
        source.clone(),
        None,
        event_sender,
    ));
    wait_until(|| source.subscribe_calls.load(Ordering::Relaxed) == 1).await;
    source.publish(ChainUpdate::Log(unknown_log(101)));
    gate.notify_waiters();

    let client = task.await.unwrap().unwrap();
    assert_eq!(client.runtime_stats().event_observer_drops, 1);
    assert!(client.is_ready());
    client.shutdown().await;
}

#[tokio::test]
async fn gap_revokes_readiness_until_snapshot_recovery_completes() {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source.clone(), None)
        .await
        .unwrap();
    wait_until(|| source.subscribe_calls.load(Ordering::Relaxed) >= 1).await;
    wait_until(|| source.events.receiver_count() >= 1).await;
    source.publish(ChainUpdate::Gap {
        cursor: None,
        reason: "intentional test gap".into(),
    });
    wait_until(|| source.snapshot_calls.load(Ordering::Relaxed) >= 2).await;
    wait_until(|| client.is_ready()).await;
    assert!(client.quote(&request()).is_ok());
    client.shutdown().await;
}

#[tokio::test]
async fn live_gap_recovery_replays_the_bounded_canonical_event_range_once() {
    let source = Arc::new(MockSource::new(None));
    let (event_sender, mut event_receiver) = mpsc::channel(4);
    let client =
        ConnectedQuoteClient::connect_with_event_sink(config(), source.clone(), None, event_sender)
            .await
            .unwrap();
    source.set_snapshot_block(101);
    let recovered_log = unknown_log(101);
    source.set_backfill_logs(vec![recovered_log.clone()]);
    wait_until(|| source.events.receiver_count() >= 1).await;

    source.publish(ChainUpdate::Gap {
        cursor: None,
        reason: "intentional event replay gap".into(),
    });

    let delivered = tokio::time::timeout(Duration::from_secs(1), event_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered, recovered_log);
    wait_until(|| client.runtime_stats().recoveries >= 1).await;
    assert!(event_receiver.try_recv().is_err());
    assert_eq!(source.backfill_calls.load(Ordering::Relaxed), 1);
    assert!(client.is_ready());
    client.shutdown().await;
}

#[tokio::test]
async fn recovery_emits_backfill_before_buffered_live_logs_once() {
    let source = Arc::new(MockSource::new(None));
    let (event_sender, mut event_receiver) = mpsc::channel(8);
    let client =
        ConnectedQuoteClient::connect_with_event_sink(config(), source.clone(), None, event_sender)
            .await
            .unwrap();
    let snapshot_gate = Arc::new(Notify::new());
    source.set_snapshot_gate(snapshot_gate.clone());
    source.set_snapshot_block(101);
    let mut recovered_log = unknown_log(101);
    recovered_log.cursor.commitment = Commitment::Canonical;
    let live_duplicate = unknown_log(101);
    let live_log = unknown_log(102);
    source.set_backfill_logs(vec![recovered_log.clone()]);
    wait_until(|| source.events.receiver_count() >= 1).await;

    source.publish(ChainUpdate::Gap {
        cursor: None,
        reason: "intentional ordered replay gap".into(),
    });
    wait_until(|| source.subscribe_calls.load(Ordering::Relaxed) >= 2).await;
    wait_until(|| source.snapshot_calls.load(Ordering::Relaxed) >= 2).await;
    source.publish(ChainUpdate::Log(live_duplicate));
    source.publish(ChainUpdate::Log(live_log.clone()));
    tokio::time::sleep(Duration::from_millis(10)).await;
    snapshot_gate.notify_waiters();

    let first = tokio::time::timeout(Duration::from_secs(1), event_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(1), event_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, recovered_log);
    assert_eq!(second, live_log);
    wait_until(|| client.runtime_stats().recoveries >= 1).await;
    assert!(event_receiver.try_recv().is_err());
    client.shutdown().await;
}

#[tokio::test]
async fn closed_event_observer_does_not_block_live_gap_recovery() {
    let source = Arc::new(MockSource::new(None));
    let (event_sender, event_receiver) = mpsc::channel(1);
    let client =
        ConnectedQuoteClient::connect_with_event_sink(config(), source.clone(), None, event_sender)
            .await
            .unwrap();
    source.set_snapshot_block(101);
    source.set_backfill_logs(vec![unknown_log(101)]);
    drop(event_receiver);
    wait_until(|| source.events.receiver_count() >= 1).await;

    source.publish(ChainUpdate::Gap {
        cursor: None,
        reason: "intentional event replay gap".into(),
    });

    wait_until(|| client.runtime_stats().recoveries >= 1).await;
    assert!(client.is_ready());
    assert_eq!(client.runtime_stats().recovery_failures, 0);
    assert_eq!(client.runtime_stats().event_observer_drops, 1);
    client.shutdown().await;
}

#[tokio::test]
async fn unknown_core_log_is_accepted_and_published_without_changing_quote_state() {
    let source = Arc::new(MockSource::new(None));
    let (event_sender, mut event_receiver) = mpsc::channel(1);
    let client =
        ConnectedQuoteClient::connect_with_event_sink(config(), source.clone(), None, event_sender)
            .await
            .unwrap();
    let before = client.quote(&request()).unwrap();
    let log = unknown_log(101);
    source.publish(ChainUpdate::Log(log.clone()));

    let accepted = tokio::time::timeout(Duration::from_secs(1), event_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(accepted, log);
    assert_eq!(client.quote(&request()).unwrap().outcome, before.outcome);
    client.shutdown().await;
}

fn unknown_log(block: u64) -> ContractLog {
    let mut log_cursor = cursor(block, Commitment::Realtime);
    log_cursor.transaction_index = Some(2);
    log_cursor.log_index = Some(3);
    ContractLog {
        address: CORE,
        transaction_hash: Some(B256::new([0x44; 32])),
        topics: vec![B256::new([0x99; 32])],
        data: Bytes::new(),
        removed: false,
        cursor: log_cursor,
    }
}

#[test]
fn realtime_source_sequence_allows_progressive_hashes_at_one_height() {
    let mut reducer = QuoteReducer::new(
        QuoteState::default(),
        lunarbase_math::FeeClass::Whitelisted,
        None,
    );
    let mut first = cursor(101, Commitment::Realtime);
    first.source_sequence = Some(1);
    reducer.bootstrap(first);
    let mut second = cursor(101, Commitment::Realtime);
    second.block_hash = Some(B256::new([2; 32]));
    second.source_sequence = Some(2);

    reducer.observe_head(second.clone()).unwrap();
    assert_eq!(reducer.cursor(), Some(&second));
}

#[test]
fn same_height_handoff_with_another_hash_fails_closed() {
    let deployment = config().deployment;
    let mut indexer = QuoteIndexer::new(QuoteState::default(), deployment);
    indexer.bootstrap(snapshot(100)).unwrap();
    let mut conflicting = cursor(100, Commitment::Realtime);
    conflicting.block_hash = Some(B256::new([2; 32]));
    conflicting.source_sequence = Some(1);

    assert!(matches!(
        indexer.apply_handoff(vec![ChainUpdate::Head(BlockRef::new(conflicting, None))]),
        Err(IndexerError::Reducer(
            crate::state::reducer::ReducerError::BlockHashMismatch
        ))
    ));
}

#[test]
fn stale_source_buffer_log_covered_by_snapshot_is_ignored_before_decode() {
    let deployment = config().deployment;
    let mut indexer = QuoteIndexer::new(QuoteState::default(), deployment);
    indexer.bootstrap(snapshot(100)).unwrap();
    let mut stale_cursor = cursor(99, Commitment::Realtime);
    stale_cursor.transaction_index = Some(2);
    stale_cursor.log_index = Some(3);
    let malformed_stale_log = ContractLog {
        address: CORE,
        transaction_hash: None,
        topics: vec![quote_critical_topics()[0]],
        data: Bytes::new(),
        removed: false,
        cursor: stale_cursor,
    };

    indexer
        .apply_core_update(ChainUpdate::Log(malformed_stale_log))
        .unwrap();
    assert_eq!(indexer.reducer.cursor().unwrap().block_number, 100);
}

#[test]
fn canonical_floor_never_hides_a_removed_log() {
    let deployment = config().deployment;
    let mut indexer = QuoteIndexer::new(QuoteState::default(), deployment);
    indexer.bootstrap(snapshot(100)).unwrap();
    let mut stale_cursor = cursor(99, Commitment::Realtime);
    stale_cursor.transaction_index = Some(2);
    stale_cursor.log_index = Some(3);
    let removed_log = ContractLog {
        address: CORE,
        transaction_hash: None,
        topics: Vec::new(),
        data: Bytes::new(),
        removed: true,
        cursor: stale_cursor,
    };

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Log(removed_log)),
        Err(IndexerError::Reducer(
            crate::state::reducer::ReducerError::RemovedLog
        ))
    ));
}

#[test]
fn same_block_source_buffer_log_conflicting_with_snapshot_fails_closed() {
    let deployment = config().deployment;
    let mut indexer = QuoteIndexer::new(QuoteState::default(), deployment);
    indexer.bootstrap(snapshot(100)).unwrap();
    let mut conflicting_cursor = cursor(100, Commitment::Realtime);
    conflicting_cursor.block_hash = Some(B256::new([2; 32]));
    conflicting_cursor.transaction_index = Some(2);
    conflicting_cursor.log_index = Some(3);
    let conflicting_log = ContractLog {
        address: CORE,
        transaction_hash: None,
        topics: Vec::new(),
        data: Bytes::new(),
        removed: false,
        cursor: conflicting_cursor,
    };

    assert!(matches!(
        indexer.apply_core_update(ChainUpdate::Log(conflicting_log)),
        Err(IndexerError::Reducer(
            crate::state::reducer::ReducerError::BlockHashMismatch
        ))
    ));
}

mod event_sink_policy;

mod checkpoint_validation;

mod fee_policy;

mod source_identity;

mod lifecycle;

mod correction_ancestor;
mod correction_regressions;
mod optimistic_correction;
mod quote_path;
mod recovery_correction;
mod recovery_watermark;

fn config() -> ClientConnectConfig {
    ClientConnectConfig {
        deployment: DeploymentConfig {
            network: Network::Base,
            chain_id: 8453,
            core: CORE,
            fee_class: lunarbase_math::FeeClass::Whitelisted,
            verified_router: None,
            deployment_block: 1,
            expected_implementation: Address::new([8; 20]),
            expected_implementation_code_hash: B256::new([7; 32]),
            contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            explicit_lane_assets: vec![ASSET],
        },
        filter: ContractFilter {
            address: CORE,
            topics: quote_critical_topics().to_vec(),
        },
        buffer_capacity: 16,
        buffer_byte_capacity: 1024 * 1024,
        reconnect_delay: Duration::from_millis(10),
        source_stall_timeout: Duration::from_secs(1),
        source_operation_timeout: Duration::from_secs(1),
    }
}

fn snapshot(block: u64) -> BootstrapSnapshot {
    let slot0 = encode_lane_slot0(&LaneSlot0 {
        price: u128::try_from(WAD).unwrap(),
        exists: true,
        ..Default::default()
    })
    .unwrap();
    let mut state = QuoteState {
        cash: CASH,
        ..Default::default()
    };
    state
        .lanes
        .insert(ASSET, LaneState::new(slot0, 1_000_000, 0));
    BootstrapSnapshot {
        state,
        cursor: cursor(block, Commitment::Finalized),
        implementation: Address::new([8; 20]),
        implementation_code_hash: B256::new([7; 32]),
        verified_router: None,
    }
}

fn cursor(block: u64, commitment: Commitment) -> ChainCursor {
    ChainCursor {
        chain_id: 8453,
        block_number: block,
        execution_block_number: block,
        block_hash: Some(B256::new([1; 32])),
        transaction_index: None,
        log_index: None,
        source_sequence: None,
        source_sub_index: None,
        commitment,
    }
}

fn request() -> QuoteRequest {
    QuoteRequest {
        asset_in: CASH,
        asset_out: ASSET,
        amount: U256::from(1_000),
        mode: QuoteMode::ExactIn,
    }
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    let started = Instant::now();
    while !predicate() {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "condition timed out"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
