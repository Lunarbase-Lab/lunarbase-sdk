//! Generic Ethereum JSON-RPC WebSocket source.
//!
//! The HTTP endpoint remains the authority for block-tagged snapshots and
//! canonical backfill.  This module only consumes the low-latency `logs` and
//! `newHeads` subscriptions and normalizes them into the same source model as
//! the network-specific adapters.  A closed socket is deliberately terminal:
//! callers must recover from the HTTP source before claiming freshness again.

use crate::source::{ChainDataSource, SourceStream};
use crate::state::ordering::CursorReorderBuffer;
use crate::transport::rpc::{parse_rpc_log, RpcError, RpcHttpBackend, RpcHttpClient};
use crate::{
    BackfillRequest, BootstrapSnapshot, ChainCursor, ChainUpdate, Checkpoint, Commitment,
    ContractFilter, DeploymentConfig, Network, RpcSnapshotProvider, SourceError,
};
use async_stream::stream;
use futures_util::{SinkExt, StreamExt};
use lunarbase_math::U256;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Bounds transport frames and the number of updates that may wait for a
/// block-head watermark.  Both bounds are required for predictable memory
/// use when an RPC provider stops producing heads or sends a burst of logs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WsRpcConfig {
    pub max_frame_bytes: usize,
    pub reorder_capacity: usize,
    pub logs_subscription: String,
    /// Accept multiple monotonically sequenced heads at one block height.
    pub progressive_heads: bool,
}

impl Default for WsRpcConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: 256 * 1024,
            reorder_capacity: 4096,
            logs_subscription: "logs".into(),
            progressive_heads: false,
        }
    }
}

impl WsRpcConfig {
    /// Validates the hard bounds used to protect the WebSocket ingestion
    /// queue. A zero bound would either make every frame invalid or make the
    /// reorder buffer unable to accept its first update.
    pub fn validate(&self) -> Result<(), SourceError> {
        if self.max_frame_bytes == 0
            || self.reorder_capacity == 0
            || !matches!(self.logs_subscription.as_str(), "logs" | "pendingLogs")
        {
            return Err(SourceError::Unavailable(
                "WS frame and reorder bounds must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

/// Standard JSON-RPC WebSocket backend. It is suitable for an executed local
/// Nitro node and for providers that expose ordinary Ethereum subscriptions.
/// Base Flashblocks providers can use the specialized adapter in
/// `flashblocks.rs` while retaining this backend for canonical fallback.
#[derive(Clone)]
pub struct WsRpcBackend {
    http: RpcHttpBackend,
    ws_endpoint: Arc<str>,
    config: WsRpcConfig,
}

impl WsRpcBackend {
    /// Creates a standard Ethereum WebSocket backend with conservative
    /// defaults for frame size and out-of-order buffering.
    pub fn new(
        rpc: RpcHttpClient,
        ws_endpoint: impl Into<String>,
        network: Network,
        chain_id: u64,
        snapshot_tag: impl Into<String>,
    ) -> Self {
        Self::with_config(
            rpc,
            ws_endpoint,
            network,
            chain_id,
            snapshot_tag,
            WsRpcConfig::default(),
        )
    }

    /// Creates a WebSocket backend with explicit resource limits.
    ///
    /// The HTTP client remains the source of block-tagged snapshots and
    /// canonical backfills; `ws_endpoint` is used only for realtime logs and
    /// head notifications.
    pub fn with_config(
        rpc: RpcHttpClient,
        ws_endpoint: impl Into<String>,
        network: Network,
        chain_id: u64,
        snapshot_tag: impl Into<String>,
        config: WsRpcConfig,
    ) -> Self {
        Self {
            http: RpcHttpBackend::new(rpc, network, chain_id, snapshot_tag),
            ws_endpoint: Arc::from(ws_endpoint.into()),
            config,
        }
    }

    /// Returns the configured WebSocket endpoint.
    pub fn endpoint(&self) -> &str {
        &self.ws_endpoint
    }

    /// Returns the transport limits used by this backend.
    pub fn config(&self) -> &WsRpcConfig {
        &self.config
    }
}

impl ChainDataSource for WsRpcBackend {
    fn network(&self) -> Network {
        self.http.network()
    }

    async fn snapshot(
        &self,
        deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        RpcSnapshotProvider::new(self.http.rpc().clone(), self.http.snapshot_tag().to_owned())
            .snapshot(deployment)
            .await
    }

    async fn backfill(
        &self,
        request: BackfillRequest,
    ) -> Result<Vec<crate::ContractLog>, SourceError> {
        self.http.backfill(request).await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        self.config.validate()?;
        let (socket, _) = connect_async(self.ws_endpoint.as_ref())
            .await
            .map_err(|error| {
                SourceError::Unavailable(format!("RPC WebSocket connect failed: {error}"))
            })?;
        let (mut writer, mut reader) = socket.split();
        writer
            .send(Message::Text(subscription_request(
                1,
                &filter,
                &self.config.logs_subscription,
            )))
            .await
            .map_err(|error| {
                SourceError::Unavailable(format!("RPC logs subscription failed: {error}"))
            })?;
        writer
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "eth_subscribe",
                    "params": ["newHeads"],
                })
                .to_string(),
            ))
            .await
            .map_err(|error| {
                SourceError::Unavailable(format!("RPC heads subscription failed: {error}"))
            })?;

        let chain_id = self.http.chain_id();
        let config = self.config.clone();
        let stream = stream! {
            let mut logs_subscription: Option<String> = None;
            let mut heads_subscription: Option<String> = None;
            let mut reorder = match CursorReorderBuffer::new(config.reorder_capacity) {
                Ok(buffer) => buffer,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            let mut last_head: Option<WsHead> = None;
            let mut source_sequence = 0_u64;

            while let Some(message) = reader.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        yield Ok(ChainUpdate::Gap {
                            cursor: last_head.as_ref().map(|head| head.cursor.clone()),
                            reason: format!("RPC WebSocket read failed; canonical recovery required: {error}"),
                        });
                        break;
                    }
                };
                let payload = match websocket_payload(message, &mut writer).await {
                    Ok(Some(payload)) => payload,
                    Ok(None) => continue,
                    Err(error) => {
                        yield Ok(ChainUpdate::Gap {
                            cursor: last_head.as_ref().map(|head| head.cursor.clone()),
                            reason: error.to_string(),
                        });
                        break;
                    }
                };
                if payload.len() > config.max_frame_bytes {
                    yield Ok(ChainUpdate::Gap {
                        cursor: last_head.as_ref().map(|head| head.cursor.clone()),
                        reason: "RPC WebSocket frame exceeded configured bound".into(),
                    });
                    break;
                }
                let value: Value = match serde_json::from_slice(&payload) {
                    Ok(value) => value,
                    Err(error) => {
                        yield Ok(ChainUpdate::Gap {
                            cursor: last_head.as_ref().map(|head| head.cursor.clone()),
                            reason: format!("invalid RPC WebSocket JSON; canonical recovery required: {error}"),
                        });
                        break;
                    }
                };

                if let Some(error) = value.get("error") {
                    yield Err(SourceError::Unavailable(format!("RPC subscription error: {error}")));
                    break;
                }
                if let (Some(id), Some(result)) = (value.get("id"), value.get("result")) {
                    if result.as_str().is_some() {
                        match id.as_u64() {
                            Some(1) => logs_subscription = result.as_str().map(str::to_owned),
                            Some(2) => heads_subscription = result.as_str().map(str::to_owned),
                            _ => {}
                        }
                    }
                    continue;
                }
                let Some(params) = value.get("params").and_then(Value::as_object) else {
                    continue;
                };
                if value.get("method").and_then(Value::as_str) != Some("eth_subscription") {
                    continue;
                }
                let Some(subscription) = params.get("subscription").and_then(Value::as_str) else {
                    yield Ok(ChainUpdate::Gap {
                        cursor: last_head.as_ref().map(|head| head.cursor.clone()),
                        reason: "RPC subscription notification has no subscription id".into(),
                    });
                    break;
                };
                let Some(result) = params.get("result") else {
                    yield Ok(ChainUpdate::Gap {
                        cursor: last_head.as_ref().map(|head| head.cursor.clone()),
                        reason: "RPC subscription notification has no result".into(),
                    });
                    break;
                };

                if logs_subscription.as_deref() == Some(subscription) {
                    let mut log = match parse_rpc_log(result, chain_id, Commitment::Realtime) {
                        Ok(log) => log,
                        Err(error) => {
                            yield Err(SourceError::Unavailable(format!("invalid RPC log notification: {error}")));
                            break;
                        }
                    };
                    source_sequence = source_sequence.saturating_add(1);
                    log.cursor.source_sequence = Some(source_sequence);
                    let update = ChainUpdate::Log(log);
                    match reorder.push(update) {
                        Ok(_) => {}
                        Err(error) => {
                            yield Err(error);
                            break;
                        }
                    }
                    if let Some(head) = last_head.as_ref() {
                        for update in reorder.drain_through(&head.cursor) {
                            yield Ok(update);
                        }
                    }
                    continue;
                }
                if heads_subscription.as_deref() == Some(subscription) {
                    let mut head = match parse_ws_head(result, chain_id) {
                        Ok(head) => head,
                        Err(error) => {
                            yield Err(SourceError::Unavailable(format!("invalid RPC head notification: {error}")));
                            break;
                        }
                    };
                    source_sequence = source_sequence.saturating_add(1);
                    head.cursor.source_sequence = Some(source_sequence);
                    if let Some(previous) = last_head.as_ref() {
                        if head.cursor.block_number
                            > previous.cursor.block_number.saturating_add(1)
                        {
                            yield Ok(ChainUpdate::Gap {
                                cursor: Some(previous.cursor.clone()),
                                reason: "RPC WebSocket skipped one or more block heads; canonical recovery required".into(),
                            });
                            break;
                        }
                        if head_discontinuity(previous, &head, config.progressive_heads) {
                            yield Ok(ChainUpdate::Reorg {
                                old_head: previous.cursor.clone(),
                                new_head: head.cursor.clone(),
                            });
                            reorder = match CursorReorderBuffer::new(config.reorder_capacity) {
                                Ok(buffer) => buffer,
                                Err(error) => {
                                    yield Err(error);
                                    break;
                                }
                            };
                        }
                    }
                    last_head = Some(head.clone());
                    if let Err(error) = reorder.push(ChainUpdate::Head(head.cursor.clone())) {
                        yield Err(error);
                        break;
                    }
                    for update in reorder.drain_through(&head.cursor) {
                        yield Ok(update);
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        self.http.snapshot_cursor(self.http.network()).await
    }

    async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        self.http.validate_checkpoint(checkpoint).await
    }
}

#[derive(Clone, Debug)]
struct WsHead {
    cursor: ChainCursor,
    parent_hash: Option<[u8; 32]>,
}

fn head_discontinuity(previous: &WsHead, next: &WsHead, progressive: bool) -> bool {
    let same_height = next.cursor.block_number == previous.cursor.block_number;
    let same_height_discontinuity = same_height
        && (!progressive
            || (next.parent_hash.is_some()
                && previous.parent_hash.is_some()
                && next.parent_hash != previous.parent_hash));
    next.cursor.block_number < previous.cursor.block_number
        || same_height_discontinuity
        || (next.cursor.block_number == previous.cursor.block_number.saturating_add(1)
            && next.parent_hash.is_some()
            && previous.cursor.block_hash.is_some()
            && next.parent_hash != previous.cursor.block_hash)
}

fn subscription_request(id: u64, filter: &ContractFilter, kind: &str) -> String {
    let mut options = serde_json::Map::new();
    options.insert("address".into(), Value::String(filter.address.to_hex()));
    if !filter.topics.is_empty() {
        options.insert(
            "topics".into(),
            Value::Array(
                filter
                    .topics
                    .iter()
                    .map(|topic| Value::String(word_hex(*topic)))
                    .collect(),
            ),
        );
    }
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "eth_subscribe",
        "params": [kind, Value::Object(options)],
    })
    .to_string()
}

fn parse_ws_head(value: &Value, chain_id: u64) -> Result<WsHead, RpcError> {
    let object = value
        .as_object()
        .ok_or_else(|| RpcError::Invalid("newHeads result is not an object".into()))?;
    let block_number = parse_hex_u64_value(object.get("number"), "head.number")?;
    let block_hash = parse_optional_hash_value(object.get("hash"), "head.hash")?;
    let parent_hash = parse_optional_hash_value(object.get("parentHash"), "head.parentHash")?;
    let evm_parent_block = object
        .get("l1BlockNumber")
        .map(|value| parse_hex_u64_value(Some(value), "head.l1BlockNumber"))
        .transpose()?;
    Ok(WsHead {
        cursor: ChainCursor {
            chain_id,
            block_number,
            execution_block_number: evm_parent_block.unwrap_or(block_number),
            block_hash,
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Realtime,
        },
        parent_hash,
    })
}

async fn websocket_payload<S>(
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

fn parse_hex_u64_value(value: Option<&Value>, field: &str) -> Result<u64, RpcError> {
    let value = value
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::Invalid(format!("{field} is not a hex string")))?;
    let value = value
        .strip_prefix("0x")
        .ok_or_else(|| RpcError::Invalid(format!("{field} is missing 0x prefix")))?;
    u64::from_str_radix(value, 16).map_err(|_| RpcError::Invalid(format!("{field} is invalid")))
}

fn parse_optional_hash_value(
    value: Option<&Value>,
    field: &str,
) -> Result<Option<[u8; 32]>, RpcError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let text = value
                .as_str()
                .ok_or_else(|| RpcError::Invalid(format!("{field} is not a string")))?;
            let text = text
                .strip_prefix("0x")
                .ok_or_else(|| RpcError::Invalid(format!("{field} is missing 0x prefix")))?;
            if text.len() != 64 {
                return Err(RpcError::Invalid(format!("{field} is not 32 bytes")));
            }
            let mut output = [0u8; 32];
            for (index, byte) in output.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
                    .map_err(|_| RpcError::Invalid(format!("{field} is invalid hex")))?;
            }
            Ok(Some(output))
        }
    }
}

fn word_hex(value: U256) -> String {
    let mut result = String::with_capacity(66);
    result.push_str("0x");
    for byte in value.to_be_bytes::<32>() {
        result.push_str(&format!("{byte:02x}"));
    }
    result
}

#[cfg(test)]
#[path = "ws_tests.rs"]
mod tests;
