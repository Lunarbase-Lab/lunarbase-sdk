use crate::ws::{
    EvmRpcSource, WsRpcConfig, drain_completed_block, is_at_or_before_watermark,
    observe_standard_head,
    protocol::{
        head_discontinuity, parse_ws_head, parse_ws_head_with_execution_context, same_head,
        subscription_request,
    },
    standard_head_deadline, take_ready_standard_head, validate_preceding_startup_logs,
};
use crate::{rpc::backend::RpcHttpBackend, rpc::client::RpcHttpClient};
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use lunarbase_client::{
    model::{
        ChainCursor, ChainUpdate, Commitment, ContractFilter, ContractLog, DeploymentConfig,
        MATH_COMPATIBILITY_VERSION, Network,
    },
    source::ChainDataSource,
    state::ordering::CursorReorderBuffer,
};
use lunarbase_math::types::{Address, B256, Bytes, U256};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

#[test]
fn builds_standard_logs_subscription() {
    let address = "0x0000000000000000000000000000000000000001"
        .parse::<Address>()
        .unwrap();
    let request = subscription_request(
        1,
        &ContractFilter {
            address,
            topics: vec![B256::new(U256::ONE.to_be_bytes::<32>())],
        },
        "logs",
    );
    let value: Value = serde_json::from_str(&request).unwrap();
    assert_eq!(value["method"], "eth_subscribe");
    assert_eq!(value["params"][0], "logs");
    assert_eq!(value["params"][1]["address"], format!("{address:#x}"));
    assert_eq!(value["params"][1]["topics"][0][0], format!("0x{:064x}", 1));
}

#[test]
fn builds_all_core_logs_subscription_without_topic_filter() {
    let address = Address::new([1_u8; 20]);
    let request = subscription_request(
        1,
        &ContractFilter {
            address,
            topics: Vec::new(),
        },
        "logs",
    );
    let value: Value = serde_json::from_str(&request).unwrap();
    assert_eq!(value["params"][1]["address"], format!("{address:#x}"));
    assert!(value["params"][1].get("topics").is_none());
}

#[test]
fn standard_profile_holds_logs_but_pending_profile_does_not() {
    assert!(WsRpcConfig::default().holds_standard_logs_until_successor());
    assert!(!WsRpcConfig::base_flashblocks().holds_standard_logs_until_successor());
    assert!(WsRpcConfig::default().validate().is_ok());
    assert!(WsRpcConfig::base_flashblocks().validate().is_ok());
}

#[test]
fn watermark_rejects_late_logs_without_rejecting_the_next_block() {
    let watermark = cursor(42, None, None);
    assert!(is_at_or_before_watermark(
        &cursor(42, Some(2), Some(3)),
        Some(&watermark)
    ));
    assert!(!is_at_or_before_watermark(
        &cursor(43, Some(0), Some(0)),
        Some(&watermark)
    ));
    assert!(!is_at_or_before_watermark(
        &cursor(1, Some(0), Some(0)),
        None
    ));
}

#[test]
fn parses_heads_and_preserves_parent_hash() {
    let hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let parent = "0x2222222222222222222222222222222222222222222222222222222222222222";
    let value = json!({"number":"0x2a","hash":hash,"parentHash":parent});
    let head = parse_ws_head(&value, 42161).unwrap();
    assert_eq!(head.cursor.block_number, 42);
    assert_eq!(head.cursor.block_hash, Some(B256::new([0x11; 32])));
    assert_eq!(head.parent_hash, Some(B256::new([0x22; 32])));
    assert_eq!(head.cursor.commitment, Commitment::Realtime);
}

#[test]
fn arbitrum_heads_require_explicit_execution_context() {
    let value = json!({
        "number": "0x2a",
        "hash": format!("0x{}", "11".repeat(32)),
        "parentHash": format!("0x{}", "22".repeat(32)),
    });
    assert!(parse_ws_head_with_execution_context(&value, 42161, true).is_err());

    let mut explicit = value;
    explicit["l1BlockNumber"] = json!("0x01");
    assert!(parse_ws_head_with_execution_context(&explicit, 42161, true).is_err());
    explicit["l1BlockNumber"] = json!("0x2a");
    assert_eq!(
        parse_ws_head_with_execution_context(&explicit, 42161, true)
            .unwrap()
            .cursor
            .execution_block_number,
        42
    );
}

#[test]
fn rejects_invalid_head_hash_width() {
    let value = json!({"number":"0x2a","hash":"0x01"});
    assert!(parse_ws_head(&value, 42161).is_err());
}

#[test]
fn exact_duplicate_head_is_idempotent_for_standard_and_base_profiles() {
    let mut first = parse_ws_head(
        &json!({
            "number":"0x2a",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
            "l1BlockNumber": "0x7",
        }),
        8453,
    )
    .unwrap();
    first.cursor.source_sequence = Some(1);
    let duplicate = parse_ws_head(
        &json!({
            "number":"0x2a",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
            "l1BlockNumber": "0x7",
        }),
        8453,
    )
    .unwrap();

    assert!(same_head(&first, &duplicate));
    assert!(!head_discontinuity(&first, &duplicate, false));
    assert!(!head_discontinuity(&first, &duplicate, true));
}

#[test]
fn progressive_heads_accept_same_height_when_parent_is_stable() {
    let first = parse_ws_head(
        &json!({
            "number":"0x2a",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
        }),
        8453,
    )
    .unwrap();
    let second = parse_ws_head(
        &json!({
            "number":"0x2a",
            "hash": format!("0x{}", "33".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
        }),
        8453,
    )
    .unwrap();

    assert!(!head_discontinuity(&first, &second, true));
    assert!(head_discontinuity(&first, &second, false));
}

#[test]
fn successor_grace_accepts_late_log_and_releases_at_deadline_without_a_third_head() {
    let head = parse_ws_head(
        &json!({
            "number": "0x2a",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
            "l1BlockNumber": "0x7",
        }),
        97,
    )
    .unwrap();
    let successor = parse_ws_head(
        &json!({
            "number": "0x2b",
            "hash": format!("0x{}", "33".repeat(32)),
            "parentHash": format!("0x{}", "11".repeat(32)),
        }),
        97,
    )
    .unwrap();
    let hash = head.cursor.block_hash.unwrap();
    let mut open_heads = VecDeque::new();
    let started = Instant::now();
    let successor_observed_at = started + Duration::from_secs(12);
    observe_standard_head(&mut open_heads, head.clone(), started);
    assert!(standard_head_deadline(&open_heads).is_none());
    observe_standard_head(&mut open_heads, successor.clone(), successor_observed_at);
    let deadline = successor_observed_at + Duration::from_secs(2);
    assert_eq!(standard_head_deadline(&open_heads), Some(deadline));
    assert!(
        take_ready_standard_head(&mut open_heads, deadline - Duration::from_millis(1)).is_none()
    );

    let mut reorder = CursorReorderBuffer::new(4).unwrap();
    reorder
        .push(ChainUpdate::Head(head.cursor.clone()))
        .unwrap();
    reorder
        .push(ChainUpdate::Head(successor.cursor.clone()))
        .unwrap();
    reorder
        .push(ChainUpdate::Log(rpc_log(42, hash, 5, 8)))
        .unwrap();
    reorder
        .push(ChainUpdate::Log(rpc_log(42, hash, 2, 3)))
        .unwrap();

    let completed = take_ready_standard_head(&mut open_heads, deadline).unwrap();
    let updates = drain_completed_block(&mut reorder, &completed, true).unwrap();

    assert!(matches!(
        &updates[0],
        ChainUpdate::Log(log)
            if log.cursor.transaction_index == Some(2)
                && log.cursor.log_index == Some(3)
                && log.cursor.execution_block_number == 7
    ));
    assert!(matches!(
        &updates[1],
        ChainUpdate::Log(log)
            if log.cursor.transaction_index == Some(5) && log.cursor.log_index == Some(8)
    ));
    assert!(matches!(&updates[2], ChainUpdate::Head(cursor) if cursor.block_number == 42));
    assert_eq!(open_heads.len(), 1, "the successor remains open");
    assert_eq!(reorder.len(), 1, "the successor remains buffered");
}

#[test]
fn completed_block_rejects_a_log_from_another_hash_before_publication() {
    let head = parse_ws_head(
        &json!({
            "number": "0x2a",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
        }),
        97,
    )
    .unwrap();
    let mut reorder = CursorReorderBuffer::new(2).unwrap();
    reorder
        .push(ChainUpdate::Head(head.cursor.clone()))
        .unwrap();
    reorder
        .push(ChainUpdate::Log(rpc_log(42, B256::new([0x33; 32]), 2, 3)))
        .unwrap();

    let error = drain_completed_block(&mut reorder, &head, false).unwrap_err();
    assert!(error.to_string().contains("does not match"));
}

#[test]
fn first_completed_head_retains_an_earlier_startup_log_for_handoff() {
    let head = parse_ws_head(
        &json!({
            "number": "0x2a",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
        }),
        97,
    )
    .unwrap();
    let mut reorder = CursorReorderBuffer::new(3).unwrap();
    reorder
        .push(ChainUpdate::Log(rpc_log(41, B256::new([0x22; 32]), 2, 3)))
        .unwrap();
    reorder
        .push(ChainUpdate::Log(rpc_log(42, B256::new([0x11; 32]), 2, 3)))
        .unwrap();
    reorder
        .push(ChainUpdate::Head(head.cursor.clone()))
        .unwrap();

    let retained = drain_completed_block(&mut reorder, &head, true).unwrap();
    assert!(matches!(
        retained.as_slice(),
        [ChainUpdate::Log(previous), ChainUpdate::Log(current), ChainUpdate::Head(_)]
            if previous.cursor.block_number == 41 && current.cursor.block_number == 42
    ));
}

#[tokio::test]
async fn preceding_startup_log_is_validated_and_gets_execution_context() {
    let previous_hash = B256::new([0x22; 32]);
    let completed_head = parse_ws_head(
        &json!({
            "number": "0x2a",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("{previous_hash:#x}"),
        }),
        97,
    )
    .unwrap();
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    asserter.push_success(&json!({
        "number": "0x29",
        "hash": format!("{previous_hash:#x}"),
        "parentHash": format!("0x{}", "33".repeat(32)),
        "l1BlockNumber": "0x7",
    }));
    let http = RpcHttpBackend::new(client, Network::Evm, 97, "latest");
    let mut updates = vec![ChainUpdate::Log(rpc_log(41, previous_hash, 2, 3))];

    validate_preceding_startup_logs(&mut updates, &completed_head, &http)
        .await
        .unwrap();

    assert!(matches!(
        &updates[0],
        ChainUpdate::Log(log) if log.cursor.execution_block_number == 7
    ));
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn source_rejects_snapshot_deployment_chain_mismatch_before_rpc() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    let source = EvmRpcSource::new(client, "ws://localhost", Network::Evm, 97, "latest");
    let deployment = DeploymentConfig {
        network: Network::Evm,
        chain_id: 98,
        core: Address::new([1; 20]),
        router: Address::new([2; 20]),
        expect_whitelisted: true,
        deployment_block: 1,
        expected_implementation: Address::new([3; 20]),
        expected_implementation_code_hash: B256::new([4; 32]),
        contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        explicit_lane_assets: Vec::new(),
    };

    let error = source.snapshot(&deployment).await.unwrap_err();

    assert!(error.to_string().contains("chain id mismatch"));
    assert!(asserter.read_q().is_empty());
}

#[test]
fn duplicate_subscription_cursor_fails_closed_before_publication() {
    let hash = B256::new([0x11; 32]);
    let mut reorder = CursorReorderBuffer::new(4).unwrap();
    reorder
        .push(ChainUpdate::Log(rpc_log(42, hash, 2, 3)))
        .unwrap();
    assert!(
        reorder
            .push(ChainUpdate::Log(rpc_log(42, hash, 2, 3)))
            .is_err()
    );
    assert!(reorder.is_poisoned());
}

fn rpc_log(
    block_number: u64,
    block_hash: B256,
    transaction_index: u32,
    log_index: u32,
) -> ContractLog {
    ContractLog {
        address: Address::new([1_u8; 20]),
        transaction_hash: None,
        topics: Vec::new(),
        data: Bytes::new(),
        removed: false,
        cursor: ChainCursor {
            block_hash: Some(block_hash),
            ..cursor(block_number, Some(transaction_index), Some(log_index))
        },
    }
}

fn cursor(
    block_number: u64,
    transaction_index: Option<u32>,
    log_index: Option<u32>,
) -> ChainCursor {
    ChainCursor {
        chain_id: 97,
        block_number,
        execution_block_number: block_number,
        block_hash: None,
        transaction_index,
        log_index,
        source_sequence: None,
        source_sub_index: None,
        commitment: Commitment::Realtime,
    }
}
