use super::{
    environment::MockState,
    helpers::{address_word, block_hash, word_hex, words},
    *,
};

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
        "eth_getLogs" => discovery_logs(&request, block),
        "eth_call" => {
            let data = request
                .pointer("/params/0/data")
                .and_then(Value::as_str)
                .unwrap_or_default();
            json!(eth_call_result(data, state.slot0))
        }
        "eth_chainId" => json!("0x2105"),
        _ => Value::Null,
    };
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

fn discovery_logs(request: &Value, block: u64) -> Value {
    let requested_topic = request
        .pointer("/params/0/topics/0")
        .and_then(Value::as_str);
    let added = word_hex(TOPIC_LANE_ADDED);
    let removed = word_hex(TOPIC_LANE_REMOVED);
    if requested_topic == Some(removed.as_str()) || requested_topic.is_none() {
        return json!([]);
    }
    if requested_topic != Some(added.as_str()) {
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
    match data.get(..10).unwrap_or_default() {
        "0x961be391" => address_word(CASH),
        "0x93b6ab27" => words(&[U256::ONE]),
        "0xd1bacd10" => words(&[slot0, U256::ONE, U256::ZERO, U256::ZERO, U256::ZERO]),
        "0xd66bd524" => words(&[
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::from(1_000_000),
        ]),
        "0x9b19251a" => words(&[U256::ONE]),
        "0xaa5f434c" => words(&[U256::ZERO, U256::ZERO]),
        _ => words(&[U256::ZERO]),
    }
}
