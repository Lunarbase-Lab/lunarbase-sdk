use async_stream::try_stream;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use lunarbase_client::{
    BackfillRequest, ChainCursor, ChainEventSource, ChainUpdate, Commitment, ContractFilter,
    ContractLog, MonadExecutionNormalizer, Network, NormalizedBackend, RpcHttpBackend,
    RpcHttpClient, SourceError, SourceStream,
};
use lunarbase_math::Address;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::protocol::{decode_parser_message, ParserMessage};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonadParserConfig {
    pub ws_url: String,
    pub core: Address,
    pub chain_id: u64,
    pub max_frame_bytes: usize,
}

impl Default for MonadParserConfig {
    fn default() -> Self {
        Self {
            ws_url: "ws://127.0.0.1:8080/ws/subscriptions".into(),
            core: Address::ZERO,
            chain_id: 143,
            max_frame_bytes: 64 * 1024,
        }
    }
}

impl MonadParserConfig {
    /// Validates parser endpoint, chain id, and frame-memory bounds.
    pub fn validate(&self) -> Result<(), SourceError> {
        if !(self.ws_url.starts_with("ws://") || self.ws_url.starts_with("wss://")) {
            return Err(SourceError::Unavailable(
                "Monad parser URL must use ws:// or wss://".into(),
            ));
        }
        if self.chain_id == 0 || self.max_frame_bytes == 0 {
            return Err(SourceError::Unavailable(
                "Monad parser chain id and frame bound must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait MonadCanonicalBackend: Send + Sync {
    /// Returns the canonical/finalized cursor used for recovery.
    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError>;
    /// Backfills canonical logs for the requested inclusive range.
    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError>;
}

pub struct UnavailableCanonicalBackend;

#[async_trait]
impl MonadCanonicalBackend for UnavailableCanonicalBackend {
    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        Err(SourceError::Unavailable(
            "Monad canonical RPC backend is not configured".into(),
        ))
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        Err(SourceError::Unavailable(
            "Monad canonical RPC backend is not configured".into(),
        ))
    }
}

/// Canonical recovery adapter for a colocated parser. Realtime data comes
/// from the parser WebSocket; authoritative snapshots and log backfills come
/// from standard Monad JSON-RPC at a finalized tag.
pub struct MonadRpcCanonicalBackend {
    backend: RpcHttpBackend,
}

impl MonadRpcCanonicalBackend {
    /// Creates a finalized Monad JSON-RPC recovery backend.
    pub fn new(endpoint: impl Into<String>, chain_id: u64) -> Self {
        Self {
            backend: RpcHttpBackend::new(
                RpcHttpClient::new(endpoint),
                Network::Monad,
                chain_id,
                "finalized",
            ),
        }
    }
}

#[async_trait]
impl MonadCanonicalBackend for MonadRpcCanonicalBackend {
    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        self.backend.snapshot_cursor(Network::Monad).await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.backend.backfill(request).await
    }
}

pub struct MonadParserSource<B> {
    config: MonadParserConfig,
    canonical: Arc<B>,
}

impl<B> MonadParserSource<B> {
    /// Validates and creates a parser source with canonical recovery injected.
    pub fn new(config: MonadParserConfig, canonical: Arc<B>) -> Result<Self, SourceError> {
        config.validate()?;
        Ok(Self { config, canonical })
    }

    /// Returns the immutable parser configuration.
    pub fn config(&self) -> &MonadParserConfig {
        &self.config
    }
}

#[async_trait]
impl<B: MonadCanonicalBackend + 'static> ChainEventSource for MonadParserSource<B> {
    fn network(&self) -> lunarbase_client::Network {
        lunarbase_client::Network::Monad
    }

    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        self.canonical.snapshot_cursor().await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.canonical.backfill(request).await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        if filter.address != self.config.core {
            return Err(SourceError::NetworkMismatch);
        }
        connect_parser_stream(self.config.clone(), filter).await
    }
}

/// Connects to the parser subscriptions and normalizes heads, logs, and gaps.
pub async fn connect_parser_stream(
    config: MonadParserConfig,
    filter: ContractFilter,
) -> Result<SourceStream, SourceError> {
    config.validate()?;
    if filter.address != config.core {
        return Err(SourceError::NetworkMismatch);
    }
    let (socket, _) = connect_async(&config.ws_url).await.map_err(|error| {
        SourceError::Unavailable(format!("Monad parser connect failed: {error}"))
    })?;
    let (mut writer, mut reader) = socket.split();
    writer
        .send(Message::Text(subscription_request(
            1,
            "logs",
            Some((&config.core, &filter)),
        )))
        .await
        .map_err(|error| {
            SourceError::Unavailable(format!("Monad parser subscribe failed: {error}"))
        })?;
    writer
        .send(Message::Text(subscription_request(2, "all", None)))
        .await
        .map_err(|error| {
            SourceError::Unavailable(format!("Monad parser subscribe failed: {error}"))
        })?;

    let stream = try_stream! {
        let mut normalizer = MonadExecutionNormalizer::new(config.chain_id);
        // The parser's `all` subscription interleaves proposed/finalized/
        // verified heads for different blocks. A single global commitment
        // would incorrectly label a newer log with an older block's finality.
        // Keep commitment by block and let the reducer promote the cursor
        // when a later head for that block arrives.
        let mut commitments = BTreeMap::<u64, Commitment>::new();
        while let Some(message) = reader.next().await {
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    yield ChainUpdate::Gap {
                        cursor: None,
                        reason: format!("Monad parser websocket error; resubscribe required: {error}"),
                    };
                    break;
                }
            };
            let payload = match message {
                Message::Text(text) => text.as_bytes().to_vec(),
                Message::Binary(bytes) => bytes.to_vec(),
                Message::Ping(bytes) => {
                    writer.send(Message::Pong(bytes)).await.map_err(|error| SourceError::Unavailable(format!("Monad parser pong failed: {error}")))?;
                    continue;
                }
                Message::Pong(_) => continue,
                Message::Close(frame) => {
                    let reason = frame.map_or_else(|| "connection closed".to_owned(), |frame| format!("connection closed: {}", frame.reason));
                    yield ChainUpdate::Gap { cursor: None, reason: format!("Monad parser {reason}; resubscribe required") };
                    break;
                }
                _ => continue,
            };
            if payload.len() > config.max_frame_bytes {
                yield ChainUpdate::Gap { cursor: None, reason: "Monad parser frame exceeded configured bound".into() };
                break;
            }
            match decode_parser_message(&payload) {
                Ok(ParserMessage::Head(head)) => {
                    commitments.insert(head.block_number, head.commitment);
                    while commitments.len() > 64 {
                        commitments.pop_first();
                    }
                    yield normalizer.normalize_head(head);
                }
                Ok(ParserMessage::Log(mut log)) => {
                    if log.address != config.core {
                        continue;
                    }
                    log.commitment = commitments
                        .get(&log.block_number)
                        .copied()
                        .unwrap_or(Commitment::Realtime);
                    if let Some(update) = normalizer.normalize_txn_log(log).map_err(|error| SourceError::Gap(error.to_string()))? {
                        yield update;
                    }
                }
                Ok(ParserMessage::Gap(reason)) => {
                    yield normalizer.normalize_gap(reason);
                    break;
                }
                Ok(ParserMessage::SubscriptionAck | ParserMessage::Ignore) => {}
                Err(error) => {
                    yield ChainUpdate::Gap { cursor: None, reason: format!("invalid Monad parser message; resnapshot required: {error}") };
                    break;
                }
            }
        }
    };
    Ok(Box::pin(stream))
}

fn subscription_request(
    id: u64,
    kind: &str,
    core_filter: Option<(&Address, &ContractFilter)>,
) -> String {
    let params = match core_filter {
        Some((core, filter)) => {
            let mut options = serde_json::Map::new();
            options.insert("address".into(), Value::String(core.to_hex()));
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
            json!([kind, Value::Object(options)])
        }
        None => json!([kind]),
    };
    json!({"jsonrpc":"2.0", "id":id, "method":"subscribe", "params":params}).to_string()
}

fn word_hex(value: lunarbase_math::U256) -> String {
    let mut result = String::with_capacity(66);
    result.push_str("0x");
    for byte in value.to_be_bytes::<32>() {
        result.push_str(&format!("{byte:02x}"));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{decode_parser_message, ParserMessage};

    #[test]
    fn subscription_request_matches_parser_shape() {
        let message = subscription_request(
            1,
            "logs",
            Some((
                &Address::from_hex("0x0000000000000000000000000000000000000001").unwrap(),
                &ContractFilter {
                    address: Address::from_hex("0x0000000000000000000000000000000000000001")
                        .unwrap(),
                    topics: vec![lunarbase_math::U256::ONE],
                },
            )),
        );
        let value: Value = serde_json::from_str(&message).unwrap();
        assert_eq!(value["params"][0], "logs");
        assert_eq!(
            value["params"][1]["address"],
            "0x0000000000000000000000000000000000000001"
        );
        assert_eq!(value["params"][1]["topics"][0], format!("0x{:064x}", 1));
    }

    #[test]
    fn parser_gap_control_message_is_not_downgraded_to_health() {
        let message = br#"{"jsonrpc":"2.0","method":"subscriptionGap","params":{"skipped":42,"resubscribeRequired":true}}"#;
        assert!(
            matches!(decode_parser_message(message).unwrap(), ParserMessage::Gap(reason) if reason.contains("42"))
        );
    }
}
