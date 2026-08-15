use super::{EvmDeliveryMode, EvmRpcSource, WsRpcConfig};
use crate::rpc::client::RpcHttpClient;
use crate::{rpc::codec::parse_rpc_log, ws::ordering::validate_finalized_page};
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use futures_util::{SinkExt, StreamExt};
use lunarbase_client::{
    model::{ChainUpdate, Commitment, ContractFilter, Network},
    source::ChainDataSource,
};
use lunarbase_math::{Address, B256};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test]
async fn mismatched_finalized_mode_and_snapshot_tag_fail_before_rpc() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    let config = WsRpcConfig {
        delivery_mode: EvmDeliveryMode::Finalized,
        ..WsRpcConfig::default()
    };
    let source =
        EvmRpcSource::with_config(client, "ws://unused", Network::Evm, 97, "latest", config);

    let error = source.canonical_head().await.unwrap_err();

    assert!(error.to_string().contains("snapshot tag disagree"));
    assert!(asserter.read_q().is_empty());
}

#[test]
fn finalized_pages_reject_removed_or_out_of_range_logs() {
    let log = parse_rpc_log(&raw_rpc_log(false), 97, Commitment::Finalized).unwrap();
    assert_eq!(
        validate_finalized_page(vec![log.clone()], &(42..=42))
            .unwrap()
            .len(),
        1
    );

    let mut removed = log.clone();
    removed.removed = true;
    assert!(validate_finalized_page(vec![removed], &(42..=42)).is_err());
    assert!(validate_finalized_page(vec![log], &(43..=43)).is_err());
}

#[tokio::test]
async fn realtime_stream_delivers_receive_order_and_removal_before_gap() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    asserter.push_success(&json!("0x61"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(serve_realtime_updates(listener));
    let source = EvmRpcSource::with_delivery_mode(
        client,
        endpoint,
        Network::Evm,
        97,
        EvmDeliveryMode::Realtime,
    );
    let mut stream = source
        .subscribe(ContractFilter {
            address: Address::new([1; 20]),
            topics: Vec::new(),
        })
        .await
        .unwrap();

    let applied = stream.next().await.unwrap().unwrap();
    let removed = stream.next().await.unwrap().unwrap();
    let recovery = stream.next().await.unwrap().unwrap();

    assert!(
        matches!(applied, ChainUpdate::Log(log) if !log.removed && log.cursor.source_sequence == Some(1))
    );
    assert!(
        matches!(removed, ChainUpdate::Log(log) if log.removed && log.cursor.source_sequence == Some(2))
    );
    assert!(matches!(recovery, ChainUpdate::Gap { .. }));
    assert!(asserter.read_q().is_empty());
    server.abort();
}

#[tokio::test]
async fn realtime_stream_emits_reorg_before_the_replacement_head() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    asserter.push_success(&json!("0x61"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(serve_competing_heads(listener));
    let source = EvmRpcSource::with_delivery_mode(
        client,
        endpoint,
        Network::Evm,
        97,
        EvmDeliveryMode::Realtime,
    );
    let mut stream = source
        .subscribe(ContractFilter {
            address: Address::new([1; 20]),
            topics: Vec::new(),
        })
        .await
        .unwrap();

    let original = stream.next().await.unwrap().unwrap();
    let reorg = stream.next().await.unwrap().unwrap();
    let replacement = stream.next().await.unwrap().unwrap();

    assert!(
        matches!(&original, ChainUpdate::Head(cursor) if cursor.block_hash == Some(B256::new([0x11; 32])))
    );
    assert!(matches!(
        &reorg,
        ChainUpdate::Reorg { old_head, new_head }
            if old_head.block_hash == Some(B256::new([0x11; 32]))
                && new_head.block_hash == Some(B256::new([0x33; 32]))
    ));
    assert!(
        matches!(&replacement, ChainUpdate::Head(cursor) if cursor.block_hash == Some(B256::new([0x33; 32])))
    );
    server.await.unwrap();
}

#[tokio::test]
async fn finalized_stream_advances_with_bounded_http_pages() {
    let asserter = Asserter::new();
    let client = RpcHttpClient::from_client(RpcClient::mocked(asserter.clone()));
    asserter.push_success(&json!("0x61"));
    asserter.push_success(&rpc_block(10, 0x11));
    asserter.push_success(&rpc_block(17, 0x22));
    for _ in 0..3 {
        asserter.push_success(&Vec::<Value>::new());
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(serve_head_update(listener));
    let source = EvmRpcSource::with_delivery_mode(
        client,
        endpoint,
        Network::Evm,
        97,
        EvmDeliveryMode::Finalized,
    )
    .with_backfill_page_blocks(3);
    let mut stream = source
        .subscribe(ContractFilter {
            address: Address::new([1; 20]),
            topics: Vec::new(),
        })
        .await
        .unwrap();

    let update = stream.next().await.unwrap().unwrap();

    assert!(
        matches!(update, ChainUpdate::Head(cursor) if cursor.block_number == 17 && cursor.commitment == Commitment::Finalized)
    );
    assert!(
        asserter.read_q().is_empty(),
        "expected three HTTP log pages"
    );
    server.abort();
}

async fn serve_realtime_updates(listener: TcpListener) {
    let (stream, _) = listener.accept().await.unwrap();
    let mut socket = accept_async(stream).await.unwrap();
    acknowledge_subscriptions(&mut socket).await;
    for removed in [false, true] {
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {
                        "subscription": "logs-subscription",
                        "result": raw_rpc_log(removed),
                    }
                })
                .to_string(),
            ))
            .await
            .unwrap();
    }
    std::future::pending::<()>().await;
}

async fn serve_head_update(listener: TcpListener) {
    let (stream, _) = listener.accept().await.unwrap();
    let mut socket = accept_async(stream).await.unwrap();
    acknowledge_subscriptions(&mut socket).await;
    socket
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {
                    "subscription": "heads-subscription",
                    "result": {
                        "number": "0x12",
                        "hash": format!("{:#x}", B256::new([0x33; 32])),
                        "parentHash": format!("{:#x}", B256::new([0x44; 32])),
                    }
                }
            })
            .to_string(),
        ))
        .await
        .unwrap();
    std::future::pending::<()>().await;
}

async fn serve_competing_heads(listener: TcpListener) {
    let (stream, _) = listener.accept().await.unwrap();
    let mut socket = accept_async(stream).await.unwrap();
    acknowledge_subscriptions(&mut socket).await;
    for (hash, parent) in [(0x11, 0x22), (0x33, 0x44)] {
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {
                        "subscription": "heads-subscription",
                        "result": {
                            "number": "0x2a",
                            "hash": format!("{:#x}", B256::new([hash; 32])),
                            "parentHash": format!("{:#x}", B256::new([parent; 32])),
                        }
                    }
                })
                .to_string(),
            ))
            .await
            .unwrap();
    }
}

async fn acknowledge_subscriptions(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) {
    for _ in 0..3 {
        let Message::Text(request) = socket.next().await.unwrap().unwrap() else {
            panic!("expected JSON-RPC text request");
        };
        let request: Value = serde_json::from_str(&request).unwrap();
        let id = request["id"].as_u64().unwrap();
        let result = match id {
            1 => json!("logs-subscription"),
            2 => json!("heads-subscription"),
            3 => json!("0x61"),
            _ => panic!("unexpected request id"),
        };
        socket
            .send(Message::Text(
                json!({"jsonrpc":"2.0","id":id,"result":result}).to_string(),
            ))
            .await
            .unwrap();
    }
}

fn rpc_block(number: u64, hash_byte: u8) -> Value {
    json!({
        "number": format!("0x{number:x}"),
        "hash": format!("{:#x}", B256::new([hash_byte; 32])),
    })
}

fn raw_rpc_log(removed: bool) -> Value {
    json!({
        "address": format!("{:#x}", Address::new([1_u8; 20])),
        "transactionHash": format!("{:#x}", B256::new([0xaa; 32])),
        "topics": [],
        "data": "0x",
        "blockNumber": "0x2a",
        "blockHash": format!("{:#x}", B256::new([0x11; 32])),
        "transactionIndex": "0x2",
        "logIndex": "0x3",
        "removed": removed,
    })
}
