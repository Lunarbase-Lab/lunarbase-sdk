use crate::support::e2e::environment::{E2eError, MockEvent, MockState};
use crate::support::e2e::helpers::{block_hash, raw_event_log, stop_requested};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_tungstenite::{accept_async, tungstenite::Message};

pub(super) async fn serve_websockets(
    listener: TcpListener,
    state: Arc<MockState>,
    stop: &mut watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = stop_requested(stop) => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break };
                let connection_state = state.clone();
                connections.spawn(async move {
                    websocket_connection(stream, connection_state).await;
                });
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

async fn websocket_connection(stream: TcpStream, state: Arc<MockState>) {
    state.websocket_connections.fetch_add(1, Ordering::Relaxed);
    let _ = websocket_connection_inner(stream, &state).await;
    state.websocket_connections.fetch_sub(1, Ordering::Relaxed);
}

async fn websocket_connection_inner(
    stream: TcpStream,
    state: &Arc<MockState>,
) -> Result<(), E2eError> {
    let socket = accept_async(stream)
        .await
        .map_err(|error| E2eError::Scenario(error.to_string()))?;
    let (mut writer, mut reader) = socket.split();
    // Register before acknowledgements can make the client publish source
    // readiness; otherwise an immediate post-ready notification can be lost.
    let mut events = state.events.subscribe();
    let mut accepts_all_logs = false;
    for _ in 0..3 {
        let Some(Ok(message)) = reader.next().await else {
            return Ok(());
        };
        let text = message
            .to_text()
            .map_err(|error| E2eError::Scenario(error.to_string()))?;
        let request: Value =
            serde_json::from_str(text).map_err(|error| E2eError::Scenario(error.to_string()))?;
        let id = request.get("id").and_then(Value::as_u64).unwrap_or(0);
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let result = match (method, id) {
            ("eth_chainId", 3) => json!("0x2105"),
            ("eth_subscribe", 1) => {
                accepts_all_logs = request.pointer("/params/1/topics").is_none();
                json!("pending")
            }
            ("eth_subscribe", 2) => json!("flashblocks"),
            _ => {
                return Err(E2eError::Scenario(format!(
                    "unexpected WebSocket handshake request: {request}"
                )));
            }
        };
        writer
            .send(Message::Text(
                json!({"jsonrpc":"2.0","id":id,"result":result}).to_string(),
            ))
            .await
            .map_err(|error| E2eError::Scenario(error.to_string()))?;
    }
    loop {
        tokio::select! {
            incoming = reader.next() => {
                match incoming {
                    Some(Ok(Message::Ping(bytes))) => {
                        if writer.send(Message::Pong(bytes)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_) | Message::Text(_) | Message::Binary(_))) => {}
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
            event = events.recv() => {
                let Ok(event) = event else { break };
                if let MockEvent::Log(log) = event {
                    if !accepts_all_logs {
                        continue;
                    }
                    let notification = json!({
                        "jsonrpc": "2.0",
                        "method": "eth_subscription",
                        "params": {
                            "subscription": "pending",
                            "result": raw_event_log(log)
                        }
                    });
                    if writer
                        .send(Message::Text(notification.to_string()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    continue;
                }
                let block = match event {
                    MockEvent::Header(block) | MockEvent::Gap(block) => block,
                    MockEvent::Log(_) => unreachable!("log handled above"),
                };
                let notification = json!({
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {
                        "subscription": "flashblocks",
                        "result": {
                            "number": format!("0x{block:x}"),
                            "hash": block_hash(block),
                            "parentHash": block_hash(block.saturating_sub(1))
                        }
                    }
                });
                if writer
                    .send(Message::Text(notification.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
    Ok(())
}
