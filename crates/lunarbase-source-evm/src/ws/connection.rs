//! Bounded WebSocket connection and subscription handshake.

use crate::ws::protocol::subscription_request;
use alloy_primitives::U64;
use futures_util::{SinkExt, StreamExt};
use lunarbase_client::model::{ContractFilter, SourceError};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::str::FromStr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
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

    timeout(
        HANDSHAKE_TIMEOUT,
        read_acknowledgements(socket, expected_chain_id),
    )
    .await
    .map_err(|_| SourceError::Unavailable("RPC subscription handshake timed out".into()))?
}

async fn read_acknowledgements(
    mut socket: RpcSocket,
    expected_chain_id: u64,
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
        match value.get("id").and_then(Value::as_u64) {
            Some(1) => logs_subscription = subscription_id(&value),
            Some(2) => heads_subscription = subscription_id(&value),
            Some(3) => {
                let raw = value.get("result").and_then(Value::as_str).ok_or_else(|| {
                    SourceError::Unavailable("RPC chain-id response is invalid".into())
                })?;
                let actual = U64::from_str(raw)
                    .map_err(|_| SourceError::Unavailable("RPC chain id is invalid".into()))?
                    .to::<u64>();
                if actual != expected_chain_id {
                    return Err(SourceError::Unavailable(format!(
                        "WebSocket RPC chain id mismatch: expected {expected_chain_id}, got {actual}"
                    )));
                }
                chain_verified = true;
            }
            _ => buffered.push_back(payload),
        }
    }
    Ok(EstablishedSocket {
        socket,
        logs_subscription: logs_subscription.expect("checked above"),
        heads_subscription: heads_subscription.expect("checked above"),
        buffered,
    })
}

fn subscription_id(value: &Value) -> Option<String> {
    value
        .get("result")
        .and_then(Value::as_str)
        .map(str::to_owned)
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
