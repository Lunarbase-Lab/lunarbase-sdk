//! Cross-module embeddable-runtime acceptance tests.

use crate::bootstrap::BootstrapSnapshot;
use crate::indexer::client::ConnectedQuoteClient;
use crate::indexer::client_types::ClientConnectConfig;
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::IndexerError;
use crate::model::{
    BackfillRequest, ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractFilter, ContractLog,
    DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network, SourceError,
};
use crate::protocol::abi::quote_critical_topics;
use crate::source::{ChainDataSource, SourceStream};
use crate::state::reducer::QuoteReducer;
use lunarbase_math::arithmetic::WAD;
use lunarbase_math::slot0::{LaneSlot0, encode_lane_slot0};
use lunarbase_math::state::{LaneState, QuoteMode, QuoteRequest, QuoteState};
use lunarbase_math::types::{Address, B256, U256};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, broadcast};

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
    snapshot_gate: Option<Arc<Notify>>,
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
            snapshot_gate,
            events,
            snapshot: snapshot(100),
        }
    }

    fn publish(&self, update: ChainUpdate) {
        let _ = self.events.send(update);
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
        if let Some(gate) = &self.snapshot_gate {
            gate.notified().await;
        }
        Ok(self.snapshot.clone())
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.backfill_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Vec::new())
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
        Ok(cursor(100, Commitment::Finalized))
    }

    async fn validate_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        self.validate_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.checkpoint_valid.load(Ordering::Relaxed))
    }
}

#[tokio::test]
async fn quote_and_batch_never_call_the_source() {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source.clone(), None)
        .await
        .unwrap();
    let calls = source_calls(&source);
    let single = client.quote(&request()).unwrap();
    let batch = client
        .quote_many(&[request(), request(), request()])
        .unwrap();
    assert_eq!(batch.cursor, single.cursor);
    assert!(
        batch
            .outcomes
            .iter()
            .all(|outcome| outcome == &single.outcome)
    );
    assert_eq!(source_calls(&source), calls);
    let oversized = vec![request(); 257];
    assert!(matches!(
        client.quote_many(&oversized),
        Err(IndexerError::InvalidRequest(_))
    ));
    client.shutdown().await;
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
async fn subscription_event_during_snapshot_is_applied_after_handoff() {
    let gate = Arc::new(Notify::new());
    let source = Arc::new(MockSource::new(Some(gate.clone())));
    let task = tokio::spawn(ConnectedQuoteClient::connect(
        config(),
        source.clone(),
        None,
    ));
    wait_until(|| source.subscribe_calls.load(Ordering::Relaxed) == 1).await;
    source.publish(ChainUpdate::Head(cursor(101, Commitment::Realtime)));
    wait_until(|| source.events.receiver_count() > 0).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    gate.notify_waiters();
    let client = task.await.unwrap().unwrap();
    assert_eq!(client.health().unwrap().cursor.unwrap().block_number, 101);
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

#[test]
fn realtime_source_sequence_allows_progressive_hashes_at_one_height() {
    let mut reducer = QuoteReducer::new(QuoteState::default(), ROUTER);
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
        indexer.apply_handoff(vec![ChainUpdate::Head(conflicting)]),
        Err(IndexerError::Reducer(
            crate::state::reducer::ReducerError::BlockHashMismatch
        ))
    ));
}

#[test]
fn deployment_rejects_duplicate_or_zero_explicit_lanes() {
    let mut deployment = config().deployment;
    deployment.explicit_lane_assets = vec![ASSET, ASSET];
    assert!(deployment.validate().is_err());
    deployment.explicit_lane_assets = vec![Address::ZERO];
    assert!(deployment.validate().is_err());
}

fn config() -> ClientConnectConfig {
    ClientConnectConfig {
        deployment: DeploymentConfig {
            network: Network::Base,
            chain_id: 8453,
            core: CORE,
            router: ROUTER,
            expect_whitelisted: true,
            deployment_block: 1,
            expected_implementation: Address::new([8; 20]),
            expected_implementation_code_hash: B256::new([7; 32]),
            contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            http_rpc_url: "http://unused".into(),
            realtime_source: "ws://unused".into(),
            explicit_lane_assets: vec![ASSET],
        },
        filter: ContractFilter {
            address: CORE,
            topics: quote_critical_topics().to_vec(),
        },
        buffer_capacity: 16,
        reconnect_delay: Duration::from_millis(10),
        source_stall_timeout: Duration::from_secs(1),
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

fn source_calls(source: &MockSource) -> [usize; 5] {
    [
        source.snapshot_calls.load(Ordering::Relaxed),
        source.backfill_calls.load(Ordering::Relaxed),
        source.subscribe_calls.load(Ordering::Relaxed),
        source.canonical_calls.load(Ordering::Relaxed),
        source.validate_calls.load(Ordering::Relaxed),
    ]
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
