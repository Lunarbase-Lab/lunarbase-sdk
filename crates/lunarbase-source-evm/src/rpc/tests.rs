use crate::rpc::client::{RpcHttpClient, backfill_filter};
use crate::rpc::codec::parse_rpc_log;
use crate::rpc::snapshot::RpcSnapshotProvider;
use alloy_primitives::{Bytes, keccak256};
use alloy_rpc_client::RpcClient;
use alloy_sol_types::SolCall;
use alloy_transport::mock::Asserter;
use lunarbase_client::model::{
    BackfillRequest, Commitment, ContractFilter, DeploymentConfig, MATH_COMPATIBILITY_VERSION,
    Network, QuoteEvent,
};
use lunarbase_client::protocol::abi::{core, quote_critical_topics};
use lunarbase_client::state::reducer::QuoteReducer;
use lunarbase_math::types::{Address, B256, U256};

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
async fn backfill_splits_ranges_larger_than_ten_thousand_blocks() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
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
async fn snapshot_preserves_global_blacklist_multiplier_after_whitelist_removal() {
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
    asserter.push_success(&Bytes::from(core::whitelistCall::abi_encode_returns(&true)));
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
    asserter.push_success(&Bytes::from(core::partnersCall::abi_encode_returns(
        &core::partnersReturn {
            cumFees: 0,
            fee: 0,
            latestWithdrawTimestamp: 0,
            operator: Address::ZERO,
        },
    )));
    asserter.push_success(&head);

    let config = DeploymentConfig {
        network: Network::Evm,
        chain_id: 97,
        core: core_address,
        router,
        expect_whitelisted: true,
        deployment_block: 1,
        expected_implementation: implementation,
        expected_implementation_code_hash: keccak256(&runtime_code),
        contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        explicit_lane_assets: Vec::new(),
    };

    let snapshot = provider.snapshot(&config).await.unwrap();
    assert_eq!(
        snapshot.state.fee_profile.blacklist_fee_multiplier,
        U256::from(9)
    );

    let mut event_cursor = snapshot.cursor.clone();
    event_cursor.block_number = 43;
    event_cursor.execution_block_number = 43;
    event_cursor.block_hash = Some(B256::new([0x22; 32]));
    event_cursor.transaction_index = Some(0);
    event_cursor.log_index = Some(0);
    let mut reducer = QuoteReducer::new(snapshot.state, router);
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

    assert!(!reducer.state().fee_profile.whitelisted);
    assert_eq!(
        reducer.state().fee_profile.blacklist_fee_multiplier,
        U256::from(9)
    );
    assert!(asserter.read_q().is_empty());
}
