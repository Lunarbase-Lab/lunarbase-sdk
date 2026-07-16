use super::*;
use crate::codec::decode_fixed_hex32;
use async_trait::async_trait;
use futures_util::stream;
use lunarbase_math::{Address, QuoteState, U256};
use std::sync::Arc;
use std::time::Duration;

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

struct TestProvider;

#[async_trait]
impl SnapshotProvider for TestProvider {
    async fn snapshot(
        &self,
        config: &DeploymentConfig,
        _lane_assets: &[Address],
        _routers: &[Address],
    ) -> Result<BootstrapSnapshot, SourceError> {
        Ok(BootstrapSnapshot {
            state: QuoteState {
                cash: config.core,
                ..Default::default()
            },
            cursor: ChainCursor::block(config.chain_id, 10, None, Commitment::Finalized),
            runtime_code_hash: config.expected_runtime_code_hash,
        })
    }
}

struct PendingSource {
    core: Address,
}

#[async_trait]
impl ChainEventSource for PendingSource {
    fn network(&self) -> Network {
        Network::Base
    }

    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        Ok(ChainCursor::block(8453, 10, None, Commitment::Finalized))
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        Ok(Vec::new())
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        if filter.address != self.core {
            return Err(SourceError::NetworkMismatch);
        }
        Ok(Box::pin(stream::pending()))
    }
}

#[tokio::test]
async fn connected_client_bootstraps_with_source_started_first() {
    let core = address(1);
    let source = Arc::new(PendingSource { core });
    let config = ClientConnectConfig {
        deployment: DeploymentConfig {
            network: Network::Base,
            chain_id: 8453,
            core,
            deployment_block: 1,
            expected_runtime_code_hash: [0; 32],
            contract_compatibility_version: "test".into(),
            http_rpc_url: "http://127.0.0.1:8545".into(),
            realtime_source: "test".into(),
            redis: RedisConfig::default(),
            explicit_lane_assets: Vec::new(),
            eager_routers: Vec::new(),
        },
        filter: ContractFilter {
            address: core,
            topics: Vec::new(),
        },
        lane_assets: Vec::new(),
        routers: Vec::new(),
        buffer_capacity: 8,
        reconnect_delay: Duration::from_millis(10),
    };
    let mut client = ConnectedQuoteClient::connect(&TestProvider, source, config)
        .await
        .unwrap();
    client.await_ready(Commitment::Finalized).await.unwrap();
    assert!(client.health().await.ready);
    client.shutdown().await;
    assert!(!client.health().await.ready);
}

#[tokio::test]
async fn connected_client_publishes_initial_checkpoint_to_store() {
    let core = address(1);
    let source = Arc::new(PendingSource { core });
    let config = ClientConnectConfig {
        deployment: DeploymentConfig {
            network: Network::Base,
            chain_id: 8453,
            core,
            deployment_block: 1,
            expected_runtime_code_hash: [0; 32],
            contract_compatibility_version: "test".into(),
            http_rpc_url: "http://127.0.0.1:8545".into(),
            realtime_source: "test".into(),
            redis: RedisConfig::default(),
            explicit_lane_assets: Vec::new(),
            eager_routers: Vec::new(),
        },
        filter: ContractFilter {
            address: core,
            topics: Vec::new(),
        },
        lane_assets: Vec::new(),
        routers: Vec::new(),
        buffer_capacity: 8,
        reconnect_delay: Duration::from_millis(10),
    };
    let store: SharedCheckpointStore = Arc::new(tokio::sync::Mutex::new(Box::new(
        InMemoryRedisStore::new(8),
    )));
    let mut client = ConnectedQuoteClient::connect_with_store(
        &TestProvider,
        source,
        config,
        Some(store.clone()),
    )
    .await
    .unwrap();
    assert!(store.lock().await.load().is_some());
    client.shutdown().await;
}

#[test]
fn binary_codec_round_trips_checkpoint_and_update() {
    let asset = address(2);
    let cursor = cursor(7);
    let checkpoint = Checkpoint {
        schema_version: SCHEMA_VERSION,
        math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        expected_runtime_code_hash: [9u8; 32],
        cursor: cursor.clone(),
        state: QuoteState {
            cash: address(1),
            lanes: [(
                asset,
                lunarbase_math::LaneState {
                    slot0: U256::MAX,
                    exists: true,
                    paused: false,
                    block_delay: 3,
                    slippage_k_bps: 42,
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        },
    };
    let encoded = encode_checkpoint(&checkpoint).unwrap();
    assert_eq!(decode_checkpoint(&encoded).unwrap(), checkpoint);

    let update = ChainUpdate::Head(cursor);
    assert_eq!(decode_update(&encode_update(&update)).unwrap(), update);
}

#[test]
fn in_memory_checkpoint_store_deduplicates_replayed_updates() {
    let checkpoint = Checkpoint {
        schema_version: SCHEMA_VERSION,
        math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        expected_runtime_code_hash: [0; 32],
        cursor: cursor(0),
        state: QuoteState {
            cash: address(1),
            ..Default::default()
        },
    };
    let update = ChainUpdate::Head(cursor(1));
    let mut store = InMemoryRedisStore::new(8);
    store
        .commit(checkpoint.clone(), vec![update.clone()])
        .unwrap();
    store.commit(checkpoint, vec![update]).unwrap();
    assert_eq!(store.updates(), vec![ChainUpdate::Head(cursor(1))]);
}

#[test]
fn redis_dedup_keys_share_the_checkpoint_hash_tag() {
    let namespace = RedisNamespace::new(8453, address(7));
    let key = crate::persistence::update_dedup_key(&namespace, &ChainUpdate::Head(cursor(1)));
    assert!(key.starts_with("lb:{8453:0x"));
}

#[test]
fn versioned_normalized_replay_fixture_reaches_reducer_boundary() {
    fn decimal(value: &serde_json::Value, field: &str) -> u64 {
        value[field]
            .as_str()
            .unwrap_or_else(|| panic!("fixture field {field} is not a decimal string"))
            .parse()
            .unwrap_or_else(|_| panic!("fixture field {field} is not a u64"))
    }
    fn hash(value: &serde_json::Value, field: &str) -> Option<[u8; 32]> {
        value[field]
            .as_str()
            .map(|encoded| decode_fixed_hex32(encoded).unwrap())
    }
    fn fixture_cursor(value: &serde_json::Value) -> ChainCursor {
        ChainCursor {
            chain_id: decimal(value, "chainId"),
            block_number: decimal(value, "blockNumber"),
            block_hash: hash(value, "blockHash"),
            transaction_index: value["transactionIndex"]
                .as_str()
                .map(|_| decimal(value, "transactionIndex") as u32),
            log_index: value["logIndex"]
                .as_str()
                .map(|_| decimal(value, "logIndex") as u32),
            source_sequence: value["sourceSequence"]
                .as_str()
                .map(|_| decimal(value, "sourceSequence")),
            source_sub_index: value["sourceSubIndex"]
                .as_str()
                .map(|_| decimal(value, "sourceSubIndex") as u32),
            commitment: match value["commitment"].as_str().unwrap() {
                "Realtime" => Commitment::Realtime,
                "Canonical" => Commitment::Canonical,
                "Finalized" => Commitment::Finalized,
                other => panic!("unknown fixture commitment {other}"),
            },
        }
    }

    let mut reducer = QuoteReducer::new(QuoteState {
        cash: address(1),
        ..Default::default()
    });
    let mut updates = 0;
    for line in
        include_str!("../../../fixtures/event-replay/monad-exec-events/normalized-updates.jsonl")
            .lines()
    {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        match value["kind"].as_str().unwrap() {
            "Head" => {
                let cursor = fixture_cursor(&value["cursor"]);
                if reducer.cursor().is_none() {
                    reducer.bootstrap(cursor);
                } else {
                    reducer.observe_head(cursor).unwrap();
                }
            }
            "Log" => {
                let cursor = fixture_cursor(&value["cursor"]);
                let update = ChainUpdate::Log(ContractLog {
                    address: Address::ZERO,
                    topics: vec![U256::ONE],
                    data: Vec::new(),
                    removed: false,
                    cursor,
                });
                if let ChainUpdate::Log(log) = update {
                    assert!(decode_core_event(&log).unwrap().is_none());
                }
            }
            "Gap" => {
                assert_eq!(
                    value["reason"].as_str(),
                    Some("Monad parser subscription gap; skipped=3")
                );
                reducer.mark_not_ready();
            }
            other => panic!("unknown normalized fixture kind {other}"),
        }
        updates += 1;
    }
    assert_eq!(updates, 4);
    assert_eq!(reducer.cursor().unwrap().commitment, Commitment::Canonical);
    assert!(!reducer.is_ready());

    let checkpoint = Checkpoint {
        schema_version: SCHEMA_VERSION,
        math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        expected_runtime_code_hash: [0; 32],
        cursor: ChainCursor {
            chain_id: 143,
            block_number: 700,
            block_hash: Some([0xaa; 32]),
            transaction_index: None,
            log_index: None,
            source_sequence: Some(1000),
            source_sub_index: None,
            commitment: Commitment::Realtime,
        },
        state: QuoteState {
            cash: address(1),
            ..Default::default()
        },
    };
    let encoded = encode_checkpoint(&checkpoint).unwrap();
    let encoded_hex = encoded
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        format!("0x{encoded_hex}"),
        "0x4c4251310002000000446c756e6172626173652d636f6e74726163747340323464623437623836366538313530613064393163666664383065666534396466383531373962353a6d6174682d76310000000000000000000000000000000000000000000000000000000000000000000000000000008f00000000000002bc01aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000100000000000003e8000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    );
}

#[test]
fn provisional_overlay_commits_only_after_canonical_match() {
    let asset = address(2);
    let mut overlay = ProvisionalOverlay::default();
    overlay.begin(cursor(0));
    let event = QuoteEvent::LaneAdded { asset };
    overlay.push(cursor(1), event.clone());
    let canonical = vec![(cursor(1), event)];
    assert_eq!(
        overlay.commit_canonical(&canonical).unwrap(),
        Some(cursor(1))
    );
    assert!(overlay.updates().is_empty());

    overlay.begin(cursor(2));
    overlay.push(cursor(3), QuoteEvent::SwapExecuted);
    overlay.discard();
    assert!(overlay.updates().is_empty());
}

#[test]
fn monad_filtered_logs_allow_sparse_global_sequences_but_reject_regression() {
    let mut tracker = MonadRingTracker::default();
    assert!(tracker.observe_sparse(100, 0).unwrap());
    assert!(tracker.observe_sparse(104, 0).unwrap());
    assert!(!tracker.observe_sparse(104, 0).unwrap());
    assert!(tracker.observe_sparse(104, 1).unwrap());
    assert!(matches!(
        tracker.observe_sparse(103, 0),
        Err(SourceError::Gap(_))
    ));
}

#[test]
fn base_flashblocks_accept_multiple_logs_in_one_index() {
    let mut normalizer = BaseFlashblocksNormalizer::new(8453);
    let header = FlashblockHeader {
        payload_id: [1u8; 32],
        block_number: 42,
        block_hash: Some([2u8; 32]),
        index: 0,
    };
    let make_log = |address: Address| FlashblockLog {
        header: header.clone(),
        transaction_index: 0,
        log_index: 0,
        address,
        topics: Vec::new(),
        data: Vec::new(),
        removed: false,
    };
    assert_eq!(
        normalizer
            .normalize_log(make_log(address(1)))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        normalizer
            .normalize_log(make_log(address(2)))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn heads_promote_commitment_without_regressing_event_cursor() {
    let mut reducer = QuoteReducer::new(QuoteState::default());
    let mut event_cursor = cursor(3);
    event_cursor.block_hash = Some([7u8; 32]);
    reducer.bootstrap(event_cursor.clone());
    reducer
        .observe_head(ChainCursor {
            chain_id: 8453,
            block_number: event_cursor.block_number,
            block_hash: Some([7u8; 32]),
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Finalized,
        })
        .unwrap();
    assert_eq!(reducer.cursor().unwrap().commitment, Commitment::Finalized);
    assert_eq!(reducer.cursor().unwrap().log_index, event_cursor.log_index);

    reducer
        .observe_head(ChainCursor::block(
            8453,
            9,
            Some([8u8; 32]),
            Commitment::Realtime,
        ))
        .unwrap();
    assert_eq!(reducer.cursor().unwrap().block_number, 10);
    assert_eq!(reducer.cursor().unwrap().commitment, Commitment::Finalized);
}
