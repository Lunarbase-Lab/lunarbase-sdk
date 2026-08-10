use crate::source::ArbitrumNitroSource;
use axum::{Json, Router, extract::State, routing::post};
use lunarbase_client::model::{BackfillRequest, ContractFilter, SourceError};
use lunarbase_client::source::ChainDataSource;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, task::JoinHandle};

const ADDRESS: &str = "0x0000000000000000000000000000000000000001";

#[derive(Clone)]
struct RpcState {
    logs: Arc<Vec<Value>>,
    contexts: Arc<HashMap<String, Value>>,
    context_response_id: Option<Value>,
    calls: Arc<Mutex<Vec<RpcCall>>>,
}

#[derive(Clone, Debug)]
struct RpcCall {
    method: String,
    block_tag: Option<String>,
}

struct MockRpc {
    url: String,
    calls: Arc<Mutex<Vec<RpcCall>>>,
    task: JoinHandle<()>,
}

impl MockRpc {
    async fn start(logs: Vec<Value>, contexts: HashMap<String, Value>) -> Self {
        Self::start_with_context_response_id(logs, contexts, None).await
    }

    async fn start_with_context_response_id(
        logs: Vec<Value>,
        contexts: HashMap<String, Value>,
        context_response_id: Option<Value>,
    ) -> Self {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = RpcState {
            logs: Arc::new(logs),
            contexts: Arc::new(contexts),
            context_response_id,
            calls: calls.clone(),
        };
        let app = Router::new().route("/", post(rpc)).with_state(state);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url: format!("http://{address}"),
            calls,
            task,
        }
    }

    fn calls(&self) -> Vec<RpcCall> {
        self.calls.lock().unwrap().clone()
    }
}

impl Drop for MockRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn rpc(State(state): State<RpcState>, Json(request): Json<Value>) -> Json<Value> {
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let block_tag = request
        .pointer("/params/0")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    state.calls.lock().unwrap().push(RpcCall {
        method: method.to_owned(),
        block_tag: block_tag.clone(),
    });
    let result = match method {
        "eth_chainId" => Some(json!("0xa4b1")),
        "eth_getLogs" => Some(Value::Array(state.logs.as_ref().clone())),
        "eth_getBlockByNumber" => block_tag
            .as_ref()
            .and_then(|tag| state.contexts.get(tag))
            .cloned(),
        _ => None,
    };
    let request_id = request.get("id").cloned().unwrap_or(Value::Null);
    let id = if method == "eth_getBlockByNumber" {
        state.context_response_id.clone().unwrap_or(request_id)
    } else {
        request_id
    };
    match result {
        Some(result) => Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })),
        None => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32601, "message": "unexpected request" }
        })),
    }
}

fn request() -> BackfillRequest {
    BackfillRequest {
        from_block: 42,
        to_block: 43,
        filter: ContractFilter {
            address: ADDRESS.parse().unwrap(),
            topics: Vec::new(),
        },
    }
}

fn raw_log(block_number: u64, log_index: u32) -> Value {
    let hash_byte = if block_number == 42 { "11" } else { "22" };
    json!({
        "address": ADDRESS,
        "transactionHash": format!("0x{}", "aa".repeat(32)),
        "topics": [],
        "data": "0x",
        "blockNumber": format!("0x{block_number:x}"),
        "blockHash": format!("0x{}", hash_byte.repeat(32)),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "removed": false,
    })
}

fn source(mock: &MockRpc) -> ArbitrumNitroSource {
    ArbitrumNitroSource::from_urls(&mock.url, "ws://unused", 42161).unwrap()
}

#[tokio::test]
async fn backfill_maps_nitro_context_once_per_distinct_l2_block() {
    let mock = MockRpc::start(
        vec![raw_log(42, 0), raw_log(42, 1), raw_log(43, 0)],
        HashMap::from([
            (
                "0x2a".into(),
                json!({
                    "number": "0x2a",
                    "hash": format!("0x{}", "11".repeat(32)),
                    "l1BlockNumber": "0x7"
                }),
            ),
            (
                "0x2b".into(),
                json!({
                    "number": "0x2b",
                    "hash": format!("0x{}", "22".repeat(32)),
                    "l1BlockNumber": "0x8"
                }),
            ),
        ]),
    )
    .await;

    let logs = source(&mock).backfill(request()).await.unwrap();

    assert_eq!(
        logs.iter()
            .map(|log| log.cursor.execution_block_number)
            .collect::<Vec<_>>(),
        vec![7, 7, 8]
    );
    let calls = mock.calls();
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.method == "eth_getLogs")
            .count(),
        1
    );
    let mut block_tags = calls
        .iter()
        .filter(|call| call.method == "eth_getBlockByNumber")
        .filter_map(|call| call.block_tag.clone())
        .collect::<Vec<_>>();
    block_tags.sort();
    assert_eq!(block_tags, vec!["0x2a", "0x2b"]);
}

#[tokio::test]
async fn canonical_head_is_pinned_to_explicit_nitro_context() {
    let hash = format!("0x{}", "11".repeat(32));
    let mock = MockRpc::start(
        Vec::new(),
        HashMap::from([
            ("latest".into(), json!({ "number": "0x2a", "hash": &hash })),
            (
                "0x2a".into(),
                json!({ "number": "0x2a", "hash": &hash, "l1BlockNumber": "0x7" }),
            ),
        ]),
    )
    .await;

    let cursor = source(&mock).canonical_head().await.unwrap();

    assert_eq!(cursor.execution_block_number, 7);
    assert_eq!(
        mock.calls()
            .iter()
            .filter(|call| call.method == "eth_getBlockByNumber")
            .filter_map(|call| call.block_tag.clone())
            .collect::<Vec<_>>(),
        vec!["latest", "0x2a"]
    );
}

#[tokio::test]
async fn backfill_rejects_context_from_a_different_branch() {
    let mock = MockRpc::start(
        vec![raw_log(42, 0)],
        HashMap::from([(
            "0x2a".into(),
            json!({
                "number": "0x2a",
                "hash": format!("0x{}", "22".repeat(32)),
                "l1BlockNumber": "0x7"
            }),
        )]),
    )
    .await;

    let error = source(&mock).backfill(request()).await.unwrap_err();

    assert!(error.to_string().contains("context hash mismatch"));
}

#[tokio::test]
async fn backfill_rejects_mismatched_context_response_id() {
    let mock = MockRpc::start_with_context_response_id(
        vec![raw_log(42, 0)],
        HashMap::from([(
            "0x2a".into(),
            json!({
                "number": "0x2a",
                "hash": format!("0x{}", "11".repeat(32)),
                "l1BlockNumber": "0x7"
            }),
        )]),
        Some(json!("another-request")),
    )
    .await;

    let error = source(&mock).backfill(request()).await.unwrap_err();

    assert!(error.to_string().contains("response id mismatch"));
}

#[tokio::test]
async fn backfill_rejects_missing_or_conflicting_log_block_hashes() {
    let mut missing_hash = raw_log(42, 0);
    missing_hash.as_object_mut().unwrap().remove("blockHash");
    let mut conflicting_hash = raw_log(42, 1);
    conflicting_hash
        .as_object_mut()
        .unwrap()
        .insert("blockHash".into(), json!(format!("0x{}", "22".repeat(32))));
    for logs in [vec![missing_hash], vec![raw_log(42, 0), conflicting_hash]] {
        let mock = MockRpc::start(logs, HashMap::new()).await;

        let error = source(&mock).backfill(request()).await.unwrap_err();

        assert!(matches!(error, SourceError::Unavailable(_)));
        assert_eq!(
            mock.calls()
                .iter()
                .filter(|call| call.method == "eth_getBlockByNumber")
                .count(),
            0
        );
    }
}

#[tokio::test]
async fn backfill_rejects_absent_or_malformed_nitro_context() {
    for context in [
        json!({
            "number": "0x2a",
            "hash": format!("0x{}", "11".repeat(32))
        }),
        json!({
            "number": "0x2a",
            "hash": format!("0x{}", "11".repeat(32)),
            "l1BlockNumber": "0x01"
        }),
    ] {
        let mock = MockRpc::start(
            vec![raw_log(42, 0)],
            HashMap::from([("0x2a".into(), context)]),
        )
        .await;

        let error = source(&mock).backfill(request()).await.unwrap_err();

        assert!(matches!(error, SourceError::Unavailable(_)));
        assert!(error.to_string().contains("l1BlockNumber"));
        assert_eq!(
            mock.calls()
                .iter()
                .filter(|call| call.method == "eth_getBlockByNumber")
                .count(),
            1
        );
    }
}
