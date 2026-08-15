use crate::rpc::backend::RpcHttpBackend;
use crate::rpc::client::{RpcHttpClient, RpcHttpLimits, backfill_filter};
use crate::rpc::codec::{parse_filtered_rpc_log, parse_rpc_log};
use crate::rpc::snapshot::RpcSnapshotProvider;
use alloy_primitives::{Bytes, keccak256};
use alloy_rpc_client::RpcClient;
use alloy_sol_types::SolCall;
use alloy_transport::mock::Asserter;
use lunarbase_client::model::{
    BackfillRequest, ChainCursor, Checkpoint, Commitment, ContractFilter, DeploymentConfig,
    MATH_COMPATIBILITY_VERSION, Network, QuoteEvent,
};
use lunarbase_client::protocol::abi::{core, quote_critical_topics};
use lunarbase_client::state::reducer::QuoteReducer;
use lunarbase_math::{Address, B256, FeeClass, QuoteState, U256};

#[test]
fn generated_core_selectors_match_the_pinned_abi() {
    assert_eq!(core::cashCall::SELECTOR, [0x96, 0x1b, 0xe3, 0x91]);
    assert_eq!(core::laneCall::SELECTOR, [0xd1, 0xba, 0xcd, 0x10]);
    assert_eq!(core::reservesCall::SELECTOR, [0xd6, 0x6b, 0xd5, 0x24]);
}

#[test]
fn alloy_filter_serializes_topics_as_topic0_or_values() {
    let request = request();
    let value = serde_json::to_value(backfill_filter(&request)).unwrap();
    assert_eq!(
        value["topics"][0].as_array().map(Vec::len),
        Some(quote_critical_topics().len())
    );
}

#[test]
fn all_core_filter_omits_topics_entirely() {
    let mut request = request();
    request.filter.topics.clear();
    let value = backfill_filter(&request);
    assert!(value.get("topics").is_none());
}

#[test]
fn rpc_log_preserves_transaction_hash() {
    let transaction_hash = B256::new([0x44; 32]);
    let log = parse_rpc_log(&rpc_log_value(), 97, Commitment::Realtime).unwrap();
    assert_eq!(log.transaction_hash, Some(transaction_hash));
}

#[test]
fn rpc_log_requires_removed_boolean() {
    let mut value = rpc_log_value();
    value.as_object_mut().unwrap().remove("removed");

    assert!(parse_rpc_log(&value, 97, Commitment::Realtime).is_err());
}

#[test]
fn rpc_log_rejects_more_than_four_topics() {
    let mut value = rpc_log_value();
    value["topics"] = serde_json::json!([
        format!("{:#x}", B256::new([1; 32])),
        format!("{:#x}", B256::new([2; 32])),
        format!("{:#x}", B256::new([3; 32])),
        format!("{:#x}", B256::new([4; 32])),
        format!("{:#x}", B256::new([5; 32])),
    ]);

    let error = parse_rpc_log(&value, 97, Commitment::Realtime).unwrap_err();
    assert!(error.to_string().contains("more than four topics"));
}

#[test]
fn filtered_rpc_log_rejects_a_foreign_contract_address() {
    let filter = ContractFilter {
        address: Address::new([2_u8; 20]),
        topics: Vec::new(),
    };

    let error =
        parse_filtered_rpc_log(&rpc_log_value(), 97, Commitment::Realtime, &filter).unwrap_err();

    assert!(error.to_string().contains("RPC log address mismatch"));
}

#[tokio::test]
async fn backfill_rejects_a_foreign_contract_address() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    let mut value = rpc_log_value();
    value["address"] = serde_json::json!(format!("{:#x}", Address::new([2_u8; 20])));
    asserter.push_success(&vec![rpc_log_value(), value]);

    let error = client
        .get_logs(&request(), 8453, Commitment::Canonical)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("RPC log address mismatch"));
}

#[tokio::test]
async fn finalized_backend_marks_recovery_logs_finalized() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    let backend = RpcHttpBackend::new(client, Network::Evm, 97, "finalized");
    asserter.push_success(&serde_json::json!("0x61"));
    asserter.push_success(&vec![rpc_log_value()]);

    let logs = backend.backfill(request()).await.unwrap();

    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].cursor.commitment, Commitment::Finalized);
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn standalone_canonical_boundaries_reject_a_foreign_http_chain() {
    let (backend, asserter) = backend_with_chain_response(98);
    let error = backend.snapshot_cursor(Network::Evm).await.unwrap_err();
    assert!(error.to_string().contains("expected 97, got 98"));
    assert!(asserter.read_q().is_empty());

    let (backend, asserter) = backend_with_chain_response(98);
    let error = backend.backfill(request()).await.unwrap_err();
    assert!(error.to_string().contains("expected 97, got 98"));
    assert!(asserter.read_q().is_empty());

    let (backend, asserter) = backend_with_chain_response(98);
    let error = backend
        .validate_checkpoint(&checkpoint())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("expected 97, got 98"));
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn checkpoint_validation_rejects_foreign_identity_before_rpc() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    let backend = RpcHttpBackend::new(client, Network::Evm, 97, "latest");
    let mut checkpoint = checkpoint();
    checkpoint.chain_id = 98;
    checkpoint.cursor.chain_id = 98;

    assert!(!backend.validate_checkpoint(&checkpoint).await.unwrap());
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn verified_http_session_is_shared_but_explicit_reconnect_rechecks() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    let backend = RpcHttpBackend::new(client, Network::Evm, 97, "latest");
    asserter.push_success(&serde_json::json!("0x61"));
    asserter.push_success(&serde_json::json!({
        "number": "0x2a",
        "hash": format!("{:#x}", B256::new([0x11; 32])),
    }));
    asserter.push_success(&Vec::<serde_json::Value>::new());
    asserter.push_success(&serde_json::json!({
        "number": "0x2a",
        "hash": format!("{:#x}", B256::new([0x22; 32])),
    }));
    asserter.push_success(&serde_json::json!("0x62"));

    backend.verify_chain_id().await.unwrap();
    backend.clone().snapshot_cursor(Network::Evm).await.unwrap();
    assert!(backend.backfill(request()).await.unwrap().is_empty());
    assert!(!backend.validate_checkpoint(&checkpoint()).await.unwrap());
    let error = backend.verify_chain_id().await.unwrap_err();

    assert!(error.to_string().contains("expected 97, got 98"));
    assert!(asserter.read_q().is_empty());
}

fn backend_with_chain_response(chain_id: u64) -> (RpcHttpBackend, Asserter) {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    asserter.push_success(&serde_json::json!(format!("0x{chain_id:x}")));
    (
        RpcHttpBackend::new(client, Network::Evm, 97, "latest"),
        asserter,
    )
}

fn checkpoint() -> Checkpoint {
    let deployment = DeploymentConfig {
        network: Network::Evm,
        chain_id: 97,
        core: Address::new([1; 20]),
        fee_class: FeeClass::Whitelisted,
        verified_router: None,
        deployment_block: 1,
        expected_implementation: Address::new([3; 20]),
        expected_implementation_code_hash: B256::new([4; 32]),
        contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        explicit_lane_assets: Vec::new(),
    };
    let mut reducer = QuoteReducer::new(QuoteState::default(), deployment.fee_class, None);
    reducer.bootstrap(ChainCursor::block(
        97,
        42,
        Some(B256::new([0x11; 32])),
        Commitment::Canonical,
    ));
    reducer.publish_ready();
    reducer.checkpoint(&deployment).unwrap()
}

fn rpc_log_value() -> serde_json::Value {
    let transaction_hash = B256::new([0x44; 32]);
    serde_json::json!({
        "address": format!("{:#x}", Address::new([1_u8; 20])),
        "transactionHash": format!("{transaction_hash:#x}"),
        "topics": [],
        "data": "0x",
        "blockNumber": "0x2a",
        "blockHash": format!("{:#x}", B256::new([0x33; 32])),
        "transactionIndex": "0x2",
        "logIndex": "0x3",
        "removed": false,
    })
}

#[tokio::test]
async fn read_only_provider_makes_no_hidden_or_retry_requests() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));

    assert!(
        asserter.read_q().is_empty(),
        "construction touched transport"
    );
    asserter.push_success(&Bytes::from_static(&[0x60, 0x00]));
    asserter.push_failure_msg("must remain queued");
    assert_eq!(
        client
            .get_code(Address::new([1_u8; 20]), "latest")
            .await
            .unwrap(),
        Bytes::from_static(&[0x60, 0x00])
    );
    assert_eq!(
        asserter.read_q().len(),
        1,
        "one read consumed more than one RPC response"
    );
}

#[tokio::test]
async fn backfill_consumes_exactly_one_rpc_response() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    asserter.push_success(&Vec::<serde_json::Value>::new());
    asserter.push_failure_msg("must remain queued");

    let logs = client
        .get_logs(&request(), 8453, Commitment::Canonical)
        .await
        .unwrap();
    assert!(logs.is_empty());
    assert_eq!(asserter.read_q().len(), 1);
}

#[tokio::test]
async fn backfill_uses_configured_block_pages() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()))
        .with_limits(RpcHttpLimits {
            max_backfill_page_blocks: 10_000,
            ..RpcHttpLimits::default()
        })
        .unwrap();
    asserter.push_success(&Vec::<serde_json::Value>::new());
    asserter.push_success(&Vec::<serde_json::Value>::new());

    let mut request = request();
    request.to_block = request.from_block + 10_000;
    let logs = client
        .get_logs(&request, 8453, Commitment::Canonical)
        .await
        .unwrap();

    assert!(logs.is_empty());
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn backfill_bisects_only_the_rejected_range() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    asserter.push_failure_msg("response size exceeds provider limit");
    asserter.push_success(&Vec::<serde_json::Value>::new());
    asserter.push_success(&Vec::<serde_json::Value>::new());

    assert!(
        client
            .get_logs(&request(), 8453, Commitment::Canonical)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(asserter.read_q().is_empty());
}

fn request() -> BackfillRequest {
    BackfillRequest {
        from_block: 10,
        to_block: 20,
        filter: ContractFilter {
            address: Address::new([1_u8; 20]),
            topics: quote_critical_topics().to_vec(),
        },
    }
}
#[tokio::test]
async fn class_snapshot_skips_router_calls_and_ignores_whitelist_events() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    let provider = RpcSnapshotProvider::new(client, "latest");
    let block_hash = B256::new([0x11; 32]);
    let implementation = Address::new([0xaa; 20]);
    let mut implementation_word = [0_u8; 32];
    implementation_word[12..].copy_from_slice(implementation.as_slice());
    let implementation_word = B256::new(implementation_word);
    let runtime_code = Bytes::from_static(&[0x60, 0x00]);
    let core_address = Address::new([1; 20]);
    let router = Address::new([2; 20]);
    let cash = Address::new([3; 20]);
    let head = serde_json::json!({
        "number": "0x2a",
        "hash": format!("{block_hash:#x}"),
    });

    asserter.push_success(&serde_json::json!("0x61"));
    asserter.push_success(&head);
    asserter.push_success(&implementation_word);
    asserter.push_success(&runtime_code);
    asserter.push_success(&Vec::<serde_json::Value>::new());
    asserter.push_success(&Bytes::from(core::cashCall::abi_encode_returns(&cash)));
    asserter.push_success(&Bytes::from(
        core::blacklistFeeMultiplierCall::abi_encode_returns(&U256::from(9)),
    ));
    asserter.push_success(&Bytes::from(core::reservesCall::abi_encode_returns(
        &core::reservesReturn {
            assetReserve: 2_000,
            treasuryFees: 0,
            partnerFees: 0,
            escrowedAssets: 0,
            totalPrincipalAmount: 0,
        },
    )));
    asserter.push_success(&head);

    let config = DeploymentConfig {
        network: Network::Evm,
        chain_id: 97,
        core: core_address,
        fee_class: FeeClass::Whitelisted,
        verified_router: None,
        deployment_block: 1,
        expected_implementation: implementation,
        expected_implementation_code_hash: keccak256(&runtime_code),
        contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        explicit_lane_assets: Vec::new(),
    };

    let snapshot = provider.snapshot(&config).await.unwrap();
    assert_eq!(snapshot.state.blacklist_fee_multiplier, U256::from(9));
    assert!(snapshot.verified_router.is_none());

    let mut event_cursor = snapshot.cursor.clone();
    event_cursor.block_number = 43;
    event_cursor.execution_block_number = 43;
    event_cursor.block_hash = Some(B256::new([0x22; 32]));
    event_cursor.transaction_index = Some(0);
    event_cursor.log_index = Some(0);
    let mut reducer = QuoteReducer::new(snapshot.state, config.fee_class, None);
    reducer.bootstrap(snapshot.cursor);
    reducer
        .apply(
            event_cursor,
            QuoteEvent::WhitelistSet {
                router,
                whitelisted: false,
            },
        )
        .unwrap();

    assert_eq!(reducer.state().blacklist_fee_multiplier, U256::from(9));
    assert!(asserter.read_q().is_empty());

    asserter.push_success(&serde_json::json!("0x61"));
    asserter.push_success(&head);
    asserter.push_success(&implementation_word);
    asserter.push_success(&runtime_code);
    asserter.push_success(&Vec::<serde_json::Value>::new());
    asserter.push_success(&Bytes::from(core::cashCall::abi_encode_returns(&cash)));
    asserter.push_success(&Bytes::from(
        core::blacklistFeeMultiplierCall::abi_encode_returns(&U256::from(9)),
    ));
    asserter.push_success(&Bytes::from(core::reservesCall::abi_encode_returns(
        &core::reservesReturn {
            assetReserve: 2_000,
            treasuryFees: 0,
            partnerFees: 0,
            escrowedAssets: 0,
            totalPrincipalAmount: 0,
        },
    )));
    asserter.push_success(&Bytes::from(core::whitelistCall::abi_encode_returns(&true)));
    asserter.push_success(&Bytes::from(core::partnersCall::abi_encode_returns(
        &core::partnersReturn {
            cumFees: 0,
            fee: 250_000,
            latestWithdrawTimestamp: 0,
            operator: Address::ZERO,
        },
    )));
    asserter.push_success(&head);

    let exact = provider
        .snapshot(&DeploymentConfig {
            verified_router: Some(router),
            ..config
        })
        .await
        .unwrap();
    let verified = exact.verified_router.expect("verified router snapshot");
    assert_eq!(verified.router, router);
    assert_eq!(verified.partner_fee_bps.get(&cash), Some(&250_000));
    assert!(asserter.read_q().is_empty());
}
