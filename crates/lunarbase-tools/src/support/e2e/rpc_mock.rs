use crate::support::e2e::environment::MockState;
use crate::support::e2e::helpers::{address_word, block_hash, word_hex};
use crate::support::e2e::{ASSET, CASH, CORE, IMPLEMENTATION};
use alloy_sol_types::{SolCall, SolValue};
use axum::Json;
use axum::extract::State;
use lunarbase_client::protocol::abi::{TOPIC_LANE_ADDED, core};
use lunarbase_math::types::{Address, B256, Bytes, U256};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::sleep;

pub(super) async fn rpc(
    State(state): State<Arc<MockState>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let delay = state.recovery_delay_milliseconds.load(Ordering::Relaxed);
    if delay > 0 && matches!(method, "eth_getBlockByNumber" | "eth_getLogs") {
        sleep(Duration::from_millis(delay)).await;
    }
    let block = state.block.load(Ordering::Relaxed);
    let result = match method {
        "eth_getBlockByNumber" => json!({
            "number": format!("0x{block:x}"),
            "hash": block_hash(block),
        }),
        "eth_getCode" => json!("0x"),
        "eth_getStorageAt" => json!(address_word(IMPLEMENTATION)),
        "eth_getLogs" => discovery_logs(&request, block),
        "eth_call" => {
            let data = request
                .pointer("/params/0/data")
                .and_then(Value::as_str)
                .or_else(|| request.pointer("/params/0/input").and_then(Value::as_str))
                .unwrap_or_default();
            json!(eth_call_result(data, state.slot0))
        }
        "eth_chainId" => json!("0x2105"),
        _ => Value::Null,
    };
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

fn discovery_logs(request: &Value, block: u64) -> Value {
    let requested_topics = request.pointer("/params/0/topics/0");
    let added = word_hex(TOPIC_LANE_ADDED);
    let includes_added = match requested_topics {
        Some(Value::String(topic)) => topic == &added,
        Some(Value::Array(topics)) => topics.iter().any(|topic| topic.as_str() == Some(&added)),
        _ => false,
    };
    if !includes_added {
        return json!([]);
    }
    json!([{
        "address": CORE,
        "topics": [added, address_word(ASSET)],
        "data": "0x",
        "removed": false,
        "blockNumber": "0x1",
        "blockHash": block_hash(1),
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "transactionHash": block_hash(block),
    }])
}

fn eth_call_result(data: &str, slot0: U256) -> String {
    let call = data.parse::<Bytes>().unwrap_or_default();
    let selector = call.get(..4).unwrap_or_default();
    let cash = CASH.parse::<Address>().expect("valid cash address");
    let encoded = if selector == core::cashCall::SELECTOR {
        core::cashCall::abi_encode_returns(&cash)
    } else if selector == core::blacklistFeeMultiplierCall::SELECTOR {
        core::blacklistFeeMultiplierCall::abi_encode_returns(&U256::ONE)
    } else if selector == core::laneCall::SELECTOR {
        core::laneCall::abi_encode_returns(&B256::from(slot0.to_be_bytes::<32>()))
    } else if selector == core::reservesCall::SELECTOR {
        core::reservesCall::abi_encode_returns(&core::reservesReturn {
            assetReserve: 1_000_000_000,
            treasuryFees: 0,
            partnerFees: 0,
            escrowedAssets: 0,
            totalPrincipalAmount: 1_000_000,
        })
    } else if selector == core::whitelistCall::SELECTOR {
        core::whitelistCall::abi_encode_returns(&true)
    } else if selector == core::partnersCall::SELECTOR {
        core::partnersCall::abi_encode_returns(&core::partnersReturn {
            cumFees: 0,
            fee: 0,
            latestWithdrawTimestamp: 0,
            operator: Address::ZERO,
        })
    } else {
        U256::ZERO.abi_encode()
    };
    Bytes::from(encoded).to_string()
}
