//! Bounded WebSocket connection and subscription handshake.

use crate::ws::protocol::subscription_request;
use alloy_primitives::U64;
use futures_util::{SinkExt, StreamExt};
use lunarbase_client::model::{ContractFilter, SourceError};
use serde_json::{Value, json};
use std::{collections::VecDeque, future::Future, str::FromStr, time::Duration};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) type RpcSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(super) struct EstablishedSocket {
    pub(super) socket: RpcSocket,
    pub(super) logs_subscription: String,
    pub(super) heads_subscription: String,
    pub(super) buffered: VecDeque<Vec<u8>>,
}

pub(super) async fn establish(
    endpoint: &str,
    filter: &ContractFilter,
    logs_kind: &str,
    expected_chain_id: u64,
    max_frame_bytes: usize,
    prefetch_capacity: usize,
) -> Result<EstablishedSocket, SourceError> {
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    before_handshake_deadline(
        deadline,
        establish_before_deadline(
            endpoint,
            filter,
            logs_kind,
            expected_chain_id,
            max_frame_bytes,
            prefetch_capacity,
        ),
    )
    .await
}

async fn establish_before_deadline(
    endpoint: &str,
    filter: &ContractFilter,
    logs_kind: &str,
    expected_chain_id: u64,
    max_frame_bytes: usize,
    prefetch_capacity: usize,
) -> Result<EstablishedSocket, SourceError> {
    let bounds = WebSocketConfig {
        max_message_size: Some(max_frame_bytes),
        max_frame_size: Some(max_frame_bytes),
        ..Default::default()
    };
    let (mut socket, _) = connect_async_with_config(endpoint, Some(bounds), false)
        .await
        .map_err(|error| {
            SourceError::Unavailable(format!("RPC WebSocket connect failed: {error}"))
        })?;
    socket
        .send(Message::Text(subscription_request(1, filter, logs_kind)))
        .await
        .map_err(|error| {
            SourceError::Unavailable(format!("RPC logs subscription failed: {error}"))
        })?;
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":2,"method":"eth_subscribe","params":["newHeads"]})
                .to_string(),
        ))
        .await
        .map_err(|error| {
            SourceError::Unavailable(format!("RPC heads subscription failed: {error}"))
        })?;
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":3,"method":"eth_chainId","params":[]}).to_string(),
        ))
        .await
        .map_err(|error| {
            SourceError::Unavailable(format!("RPC chain-id request failed: {error}"))
        })?;

    read_acknowledgements(socket, expected_chain_id, prefetch_capacity).await
}

async fn before_handshake_deadline<T>(
    deadline: Instant,
    operation: impl Future<Output = Result<T, SourceError>>,
) -> Result<T, SourceError> {
    timeout_at(deadline, operation)
        .await
        .map_err(|_| SourceError::Unavailable("RPC subscription handshake timed out".into()))?
}

async fn read_acknowledgements(
    mut socket: RpcSocket,
    expected_chain_id: u64,
    prefetch_capacity: usize,
) -> Result<EstablishedSocket, SourceError> {
    let mut logs_subscription = None;
    let mut heads_subscription = None;
    let mut chain_verified = false;
    let mut buffered = VecDeque::new();
    while logs_subscription.is_none() || heads_subscription.is_none() || !chain_verified {
        let message = socket
            .next()
            .await
            .ok_or_else(|| {
                SourceError::Unavailable("RPC WebSocket closed during handshake".into())
            })?
            .map_err(|error| {
                SourceError::Unavailable(format!("RPC WebSocket handshake failed: {error}"))
            })?;
        let Some(payload) = websocket_payload(message, &mut socket).await? else {
            continue;
        };
        let value: Value = serde_json::from_slice(&payload).map_err(|error| {
            SourceError::Unavailable(format!("invalid RPC handshake JSON: {error}"))
        })?;
        if let Some(error) = value.get("error") {
            return Err(SourceError::Unavailable(format!(
                "RPC subscription handshake error: {error}"
            )));
        }
        match handshake_response_id(&value)? {
            Some(1) => {
                record_subscription_ack(&mut logs_subscription, &value, "logs")?;
            }
            Some(2) => {
                record_subscription_ack(&mut heads_subscription, &value, "heads")?;
            }
            Some(3) => {
                validate_chain_id_response(&value, expected_chain_id)?;
                chain_verified = true;
            }
            _ => push_prefetched(&mut buffered, payload, prefetch_capacity)?,
        }
    }
    Ok(EstablishedSocket {
        socket,
        logs_subscription: logs_subscription.expect("checked above"),
        heads_subscription: heads_subscription.expect("checked above"),
        buffered,
    })
}

fn validate_chain_id_response(value: &Value, expected_chain_id: u64) -> Result<(), SourceError> {
    let raw = value
        .get("result")
        .and_then(Value::as_str)
        .ok_or_else(|| SourceError::Unavailable("RPC chain-id response is invalid".into()))?;
    let actual = U64::from_str(raw)
        .map_err(|_| SourceError::Unavailable("RPC chain id is invalid".into()))?
        .to::<u64>();
    if actual != expected_chain_id {
        return Err(SourceError::Unavailable(format!(
            "WebSocket RPC chain id mismatch: expected {expected_chain_id}, got {actual}"
        )));
    }
    Ok(())
}

fn push_prefetched(
    buffered: &mut VecDeque<Vec<u8>>,
    payload: Vec<u8>,
    capacity: usize,
) -> Result<(), SourceError> {
    if buffered.len() >= capacity {
        return Err(SourceError::Unavailable(
            "RPC subscription handshake prefetch overflow".into(),
        ));
    }
    buffered.push_back(payload);
    Ok(())
}

fn handshake_response_id(value: &Value) -> Result<Option<u64>, SourceError> {
    let Some(id) = value.get("id") else {
        return Ok(None);
    };
    id.as_u64().map(Some).ok_or_else(|| {
        SourceError::Unavailable("RPC handshake response id must be an integer".into())
    })
}

fn record_subscription_ack(
    current: &mut Option<String>,
    value: &Value,
    kind: &str,
) -> Result<(), SourceError> {
    let incoming = value
        .get("result")
        .and_then(Value::as_str)
        .filter(|subscription| !subscription.is_empty())
        .ok_or_else(|| {
            SourceError::Unavailable(format!(
                "RPC {kind} subscription acknowledgement is invalid"
            ))
        })?;
    if current
        .as_deref()
        .is_some_and(|existing| existing != incoming)
    {
        return Err(SourceError::Unavailable(format!(
            "RPC {kind} subscription acknowledgement changed"
        )));
    }
    *current = Some(incoming.to_owned());
    Ok(())
}

pub(super) async fn websocket_payload<S>(
    message: Message,
    writer: &mut S,
) -> Result<Option<Vec<u8>>, SourceError>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    match message {
        Message::Text(text) => Ok(Some(text.as_bytes().to_vec())),
        Message::Binary(bytes) => Ok(Some(bytes.to_vec())),
        Message::Ping(bytes) => {
            writer.send(Message::Pong(bytes)).await.map_err(|error| {
                SourceError::Unavailable(format!("RPC WebSocket pong failed: {error}"))
            })?;
            Ok(None)
        }
        Message::Pong(_) => Ok(None),
        Message::Close(frame) => Err(SourceError::Gap(match frame {
            Some(frame) => format!(
                "RPC WebSocket closed ({}); canonical recovery required",
                frame.reason
            ),
            None => "RPC WebSocket closed; canonical recovery required".into(),
        })),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        before_handshake_deadline, establish, handshake_response_id, push_prefetched,
        record_subscription_ack, validate_chain_id_response,
    };
    use futures_util::{SinkExt, StreamExt};
    use lunarbase_client::model::{ContractFilter, SourceError};
    use lunarbase_math::Address;
    use serde_json::json;
    use std::{collections::VecDeque, future::pending};
    use tokio::net::TcpListener;
    use tokio::time::Instant;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[test]
    fn handshake_requires_numeric_request_ids_and_stable_subscription_acks() {
        assert!(handshake_response_id(&json!({"id": "1"})).is_err());
        assert!(handshake_response_id(&json!({"id": true})).is_err());
        assert_eq!(handshake_response_id(&json!({"id": 1})).unwrap(), Some(1));

        let mut subscription = None;
        record_subscription_ack(&mut subscription, &json!({"result": "logs-a"}), "logs").unwrap();
        record_subscription_ack(&mut subscription, &json!({"result": "logs-a"}), "logs").unwrap();
        assert!(
            record_subscription_ack(&mut subscription, &json!({"result": "logs-b"}), "logs",)
                .unwrap_err()
                .to_string()
                .contains("acknowledgement changed")
        );
    }

    #[test]
    fn handshake_prefetch_fails_closed_at_its_bound() {
        let mut buffered = VecDeque::new();
        push_prefetched(&mut buffered, vec![1], 2).unwrap();
        push_prefetched(&mut buffered, vec![2], 2).unwrap();

        let error = push_prefetched(&mut buffered, vec![3], 2).unwrap_err();

        assert!(error.to_string().contains("prefetch overflow"));
        assert_eq!(buffered.len(), 2);
    }

    #[test]
    fn handshake_rejects_invalid_or_foreign_chain_ids() {
        validate_chain_id_response(&json!({"result": "0x61"}), 97).unwrap();

        let mismatch = validate_chain_id_response(&json!({"result": "0x1"}), 97).unwrap_err();
        assert!(mismatch.to_string().contains("expected 97, got 1"));

        assert!(validate_chain_id_response(&json!({"result": 97}), 97).is_err());
        assert!(validate_chain_id_response(&json!({"result": "not-a-chain-id"}), 97).is_err());
    }

    #[tokio::test]
    async fn initial_connect_and_reconnect_both_verify_chain_id() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for chain_id in [97_u64, 1] {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                for _ in 0..3 {
                    let request = socket.next().await.unwrap().unwrap();
                    let Message::Text(request) = request else {
                        panic!("expected a text JSON-RPC request");
                    };
                    let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                    let id = request["id"].as_u64().unwrap();
                    let result = match id {
                        1 => json!("logs-subscription"),
                        2 => json!("heads-subscription"),
                        3 => json!(format!("0x{chain_id:x}")),
                        _ => panic!("unexpected JSON-RPC request id"),
                    };
                    socket
                        .send(Message::Text(
                            json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string(),
                        ))
                        .await
                        .unwrap();
                }
            }
        });
        let filter = ContractFilter {
            address: Address::new([1_u8; 20]),
            topics: Vec::new(),
        };

        let initial = establish(&endpoint, &filter, "logs", 97, 1024, 4)
            .await
            .unwrap();
        drop(initial);
        let reconnect = establish(&endpoint, &filter, "logs", 97, 1024, 4).await;
        let error = match reconnect {
            Ok(_) => panic!("foreign reconnect chain id was accepted"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("expected 97, got 1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn expired_absolute_handshake_deadline_fails_closed() {
        let error = before_handshake_deadline(Instant::now(), pending::<Result<(), SourceError>>())
            .await
            .unwrap_err();

        assert!(error.to_string().contains("handshake timed out"));
    }
}
