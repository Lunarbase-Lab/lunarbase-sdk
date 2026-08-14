use super::*;
use crate::execution::MonadDeliveryMode;
use crate::lifecycle::RawExecRecord;
use crate::parser::MonadParserProtocol;
use futures_util::{SinkExt, StreamExt};
use lunarbase_math::{Address, B256};
use serde_json::{Value, json};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn config() -> MonadParserConfig {
    MonadParserConfig {
        core: Address::new([0x11; 20]),
        chain_id: 143,
        delivery_mode: MonadDeliveryMode::Realtime,
        ..Default::default()
    }
}

fn filter() -> ContractFilter {
    ContractFilter {
        address: config().core,
        topics: vec![B256::new([0x22; 32])],
    }
}

fn empty_record(sequence: u64) -> RawExecRecord {
    RawExecRecord {
        sequence,
        source_sequence: sequence + 100,
        timestamp_ns: 1,
        block_number: Some(7),
        event_type_id: 4,
        event_name: "BLOCK_PERF_EVM_ENTER".into(),
        flow_block_seqno: 100,
        flow_txn_index: None,
        flow_account_index: 0,
        payload: Default::default(),
    }
}

#[test]
fn new_session_starts_at_tail_and_reconnects_from_confirmed_ack() {
    let session = ParserV2Session::default();
    let bounds = StreamBounds {
        earliest_sequence: Some(1),
        latest_sequence: Some(10),
    };
    assert_eq!(
        session
            .prepare("stream-a", bounds, &config(), filter())
            .unwrap(),
        10
    );
    assert!(session.process(empty_record(11)).unwrap().0.is_empty());
    assert_eq!(session.acknowledged().unwrap(), 10);
    session.confirm_ack(11).unwrap();
    assert_eq!(
        session
            .prepare("stream-a", bounds, &config(), filter())
            .unwrap(),
        11
    );
}

#[test]
fn replayed_records_are_duplicate_safe_but_forward_gaps_fail_closed() {
    let session = ParserV2Session::default();
    session
        .prepare(
            "stream-a",
            StreamBounds {
                earliest_sequence: Some(1),
                latest_sequence: Some(10),
            },
            &config(),
            filter(),
        )
        .unwrap();
    session.process(empty_record(11)).unwrap();
    assert!(session.process(empty_record(11)).unwrap().0.is_empty());
    let error = session.process(empty_record(13)).unwrap_err();
    assert!(error.to_string().contains("non-contiguous"));
}

#[test]
fn explicit_rebase_discards_partial_lifecycle_and_moves_to_latest_tail() {
    let session = ParserV2Session::default();
    let initial = StreamBounds {
        earliest_sequence: Some(1),
        latest_sequence: Some(10),
    };
    session
        .prepare("stream-a", initial, &config(), filter())
        .unwrap();
    session.process(empty_record(11)).unwrap();
    session.mark_rebase().unwrap();
    let after = session
        .prepare(
            "stream-a",
            StreamBounds {
                earliest_sequence: Some(8),
                latest_sequence: Some(20),
            },
            &config(),
            filter(),
        )
        .unwrap();
    assert_eq!(after, 20);
    assert_eq!(session.acknowledged().unwrap(), 20);
}

#[test]
fn changed_stream_identity_fails_once_then_rebases_at_the_new_tail() {
    let session = ParserV2Session::default();
    let initial = StreamBounds {
        earliest_sequence: Some(1),
        latest_sequence: Some(10),
    };
    session
        .prepare("stream-a", initial, &config(), filter())
        .unwrap();
    assert!(
        session
            .prepare("stream-b", initial, &config(), filter())
            .unwrap_err()
            .to_string()
            .contains("identity changed")
    );
    assert_eq!(
        session
            .prepare(
                "stream-b",
                StreamBounds {
                    earliest_sequence: Some(8),
                    latest_sequence: Some(20),
                },
                &config(),
                filter(),
            )
            .unwrap(),
        20
    );
}

#[test]
fn wire_requests_and_identity_parsing_are_stable() {
    let handshake: serde_json::Value =
        serde_json::from_str(&wire::handshake_request(143, Some("stream-a"))).unwrap();
    assert_eq!(handshake["params"]["protocolVersion"], 2);
    assert_eq!(handshake["params"]["expectedStreamId"], "stream-a");
    let subscribe: serde_json::Value =
        serde_json::from_str(&wire::subscribe_request(99, "stream-a")).unwrap();
    assert_eq!(subscribe["params"][0], "execEventsV2");
    assert_eq!(subscribe["params"][1]["afterSequence"], 99);
    assert_eq!(wire::parse_chain_id("0x8f"), Some(143));
}

#[tokio::test]
async fn adapter_confirms_processed_records_before_resuming() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(tcp).await.unwrap();

        let handshake = read_json(&mut socket).await;
        assert_eq!(handshake["method"], "handshake");
        assert_eq!(handshake["params"]["chainId"], 143);
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "identity": {
                            "protocolVersion": 2,
                            "streamId": "stream-a",
                            "chainId": "0x8f"
                        },
                        "bounds": {"earliestSequence": 1, "latestSequence": 10}
                    }
                })
                .to_string(),
            ))
            .await
            .unwrap();

        let subscribe = read_json(&mut socket).await;
        assert_eq!(subscribe["method"], "subscribe");
        assert_eq!(subscribe["params"][1]["afterSequence"], 10);
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": {
                        "subscription": "sub-a",
                        "streamId": "stream-a",
                        "replayFrom": 11,
                        "replayThrough": 10
                    }
                })
                .to_string(),
            ))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "method": "stream",
                    "params": {
                        "subscription": "sub-a",
                        "record": {
                            "type": "execEvent",
                            "sequence": 11,
                            "sourceSequence": 111,
                            "timestampNs": 1,
                            "blockNumber": 7,
                            "eventTypeId": 4,
                            "eventName": "BLOCK_PERF_EVM_ENTER",
                            "flowInfo": {
                                "blockSeqno": 100,
                                "txnIndex": null,
                                "accountIndex": 0
                            },
                            "payloadHex": "0x"
                        }
                    }
                })
                .to_string(),
            ))
            .await
            .unwrap();

        let ack = read_json(&mut socket).await;
        assert_eq!(ack["method"], "ack");
        assert_eq!(ack["params"]["sequence"], 11);
        socket
            .send(Message::Text(
                json!({"jsonrpc": "2.0", "id": ack["id"], "result": true}).to_string(),
            ))
            .await
            .unwrap();
        socket.close(None).await.unwrap();
    });

    let session = Arc::new(ParserV2Session::default());
    let mut adapter_config = config();
    adapter_config.ws_url = format!("ws://{address}");
    adapter_config.protocol = MonadParserProtocol::DurableV2;
    adapter_config.ack_interval = 1;
    let mut events = connect(adapter_config, filter(), session.clone())
        .await
        .unwrap();
    let event = timeout(std::time::Duration::from_secs(2), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(event, ExecutionEvent::Gap { .. }));
    server.await.unwrap();
    assert_eq!(session.acknowledged().unwrap(), 11);
}

#[tokio::test]
async fn subscribe_gap_rebases_an_expired_resume_cursor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(tcp).await.unwrap();
        assert_eq!(read_json(&mut socket).await["method"], "handshake");
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "identity": {
                            "protocolVersion": 2,
                            "streamId": "stream-a",
                            "chainId": "0x8f"
                        },
                        "bounds": {"earliestSequence": 20, "latestSequence": 30}
                    }
                })
                .to_string(),
            ))
            .await
            .unwrap();
        let subscribe = read_json(&mut socket).await;
        assert_eq!(subscribe["params"][1]["afterSequence"], 10);
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "method": "gap",
                    "params": {
                        "requestedAfter": 10,
                        "earliestSequence": 20,
                        "latestSequence": 30,
                        "reason": "resume cursor expired from durable retention",
                        "resubscribeRequired": true
                    }
                })
                .to_string(),
            ))
            .await
            .unwrap();
    });

    let session = Arc::new(ParserV2Session::default());
    session
        .prepare(
            "stream-a",
            StreamBounds {
                earliest_sequence: Some(1),
                latest_sequence: Some(10),
            },
            &config(),
            filter(),
        )
        .unwrap();
    let mut adapter_config = config();
    adapter_config.ws_url = format!("ws://{address}");
    adapter_config.protocol = MonadParserProtocol::DurableV2;
    let error = match connect(adapter_config, filter(), session.clone()).await {
        Ok(_) => panic!("expired resume cursor must fail closed"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("expired"));
    server.await.unwrap();
    assert_eq!(
        session
            .prepare(
                "stream-a",
                StreamBounds {
                    earliest_sequence: Some(20),
                    latest_sequence: Some(30),
                },
                &config(),
                filter(),
            )
            .unwrap(),
        30
    );
}

async fn read_json(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> Value {
    let message = socket.next().await.unwrap().unwrap();
    serde_json::from_slice(message.into_data().as_ref()).unwrap()
}
