use crate::rpc::client::{RpcHttpClient, backfill_filter};
use alloy_primitives::Bytes;
use alloy_rpc_client::RpcClient;
use alloy_sol_types::SolCall;
use alloy_transport::mock::Asserter;
use lunarbase_client::model::{BackfillRequest, Commitment, ContractFilter};
use lunarbase_client::protocol::abi::{core, quote_critical_topics};
use lunarbase_math::types::Address;

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
