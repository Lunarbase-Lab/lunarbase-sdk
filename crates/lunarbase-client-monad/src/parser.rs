//! Monad parser WebSocket reader and canonical recovery adapter.

use async_stream::stream;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use lunarbase_client_core::{
    BackfillRequest, ChainCursor, ChainEventSource, ContractFilter, ContractLog, ExecutionEvent,
    ExecutionEventReader, ExecutionEventStream, MonadExecutionEngine, Network, NormalizedBackend,
    RpcHttpBackend, RpcHttpClient, SourceError, SourceStream,
};
use lunarbase_math::Address;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::protocol::{decode_parser_message, ParserMessage};

/// Resource and identity settings for the local Monad parser connection.
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

/// Execution-event reader backed by `monad-exec-events-parser`.
#[derive(Clone)]
pub struct MonadParserReader {
    config: MonadParserConfig,
}

impl MonadParserReader {
    /// Creates a validated parser execution-event reader.
    pub fn new(config: MonadParserConfig) -> Result<Self, SourceError> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Returns the immutable parser configuration.
    pub fn config(&self) -> &MonadParserConfig {
        &self.config
    }
}

#[async_trait]
impl ExecutionEventReader for MonadParserReader {
    async fn subscribe_execution(
        &self,
        filter: ContractFilter,
    ) -> Result<ExecutionEventStream, SourceError> {
        connect_parser_stream(self.config.clone(), filter).await
    }
}

/// Canonical recovery backend for a colocated Monad execution reader.
pub struct MonadRpcCanonicalBackend {
    backend: RpcHttpBackend,
}

impl MonadRpcCanonicalBackend {
    /// Creates a finalized Monad JSON-RPC snapshot/backfill backend.
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

    /// Returns the generic HTTP backend.
    pub fn inner(&self) -> &RpcHttpBackend {
        &self.backend
    }
}

#[async_trait]
impl NormalizedBackend for MonadRpcCanonicalBackend {
    async fn snapshot_cursor(&self, network: Network) -> Result<ChainCursor, SourceError> {
        self.backend.snapshot_cursor(network).await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.backend.backfill(request).await
    }

    async fn subscribe(
        &self,
        _network: Network,
        _filter: ContractFilter,
    ) -> Result<SourceStream, SourceError> {
        Err(SourceError::Unavailable(
            "Monad canonical backend has no realtime execution stream".into(),
        ))
    }
}

/// Explicitly unavailable recovery backend for parser-only smoke tests.
pub struct UnavailableCanonicalBackend;

#[async_trait]
impl NormalizedBackend for UnavailableCanonicalBackend {
    async fn snapshot_cursor(&self, _network: Network) -> Result<ChainCursor, SourceError> {
        Err(SourceError::Unavailable(
            "Monad canonical RPC backend is not configured".into(),
        ))
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        Err(SourceError::Unavailable(
            "Monad canonical RPC backend is not configured".into(),
        ))
    }

    async fn subscribe(
        &self,
        _network: Network,
        _filter: ContractFilter,
    ) -> Result<SourceStream, SourceError> {
        Err(SourceError::Unavailable(
            "Monad canonical RPC backend has no realtime stream".into(),
        ))
    }
}

/// Runtime-facing Monad source using the common execution engine.
pub struct MonadParserSource<B> {
    config: MonadParserConfig,
    engine: MonadExecutionEngine<MonadParserReader, B>,
}

impl<B> MonadParserSource<B> {
    /// Creates a parser source with an injected canonical recovery backend.
    pub fn new(config: MonadParserConfig, canonical: Arc<B>) -> Result<Self, SourceError> {
        let reader = Arc::new(MonadParserReader::new(config.clone())?);
        let engine = MonadExecutionEngine::new(reader, canonical, config.chain_id);
        Ok(Self { config, engine })
    }

    /// Returns the immutable parser configuration.
    pub fn config(&self) -> &MonadParserConfig {
        &self.config
    }

    /// Returns the common Monad execution engine.
    pub fn engine(&self) -> &MonadExecutionEngine<MonadParserReader, B> {
        &self.engine
    }
}

#[async_trait]
impl<B: NormalizedBackend + 'static> ChainEventSource for MonadParserSource<B> {
    fn network(&self) -> Network {
        Network::Monad
    }

    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        self.engine.snapshot_cursor().await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.engine.backfill(request).await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        self.engine.subscribe(filter).await
    }
}

/// Connects to parser subscriptions and returns raw execution lifecycle events.
pub async fn connect_parser_stream(
    config: MonadParserConfig,
    filter: ContractFilter,
) -> Result<ExecutionEventStream, SourceError> {
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

    let output = stream! {
        let mut commitments = BTreeMap::new();
        while let Some(message) = reader.next().await {
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    yield Ok(ExecutionEvent::Gap {
                        cursor: None,
                        reason: format!("Monad parser websocket error; resubscribe required: {error}"),
                    });
                    break;
                }
            };
            let payload = match message {
                Message::Text(text) => text.as_bytes().to_vec(),
                Message::Binary(bytes) => bytes.to_vec(),
                Message::Ping(bytes) => {
                    if let Err(error) = writer.send(Message::Pong(bytes)).await {
                        yield Err(SourceError::Unavailable(format!("Monad parser pong failed: {error}")));
                        break;
                    }
                    continue;
                }
                Message::Pong(_) => continue,
                Message::Close(frame) => {
                    let reason = frame.map_or_else(
                        || "connection closed".to_owned(),
                        |frame| format!("connection closed: {}", frame.reason),
                    );
                    yield Ok(ExecutionEvent::Gap {
                        cursor: None,
                        reason: format!("Monad parser {reason}; resubscribe required"),
                    });
                    break;
                }
                _ => continue,
            };
            if payload.len() > config.max_frame_bytes {
                yield Ok(ExecutionEvent::Gap {
                    cursor: None,
                    reason: "Monad parser frame exceeded configured bound".into(),
                });
                break;
            }
            match decode_parser_message(&payload) {
                Ok(ParserMessage::Head(head)) => {
                    commitments.insert(head.block_number, head.commitment);
                    while commitments.len() > 64 {
                        commitments.pop_first();
                    }
                    yield Ok(ExecutionEvent::Head(head));
                }
                Ok(ParserMessage::Log(mut log)) => {
                    if log.address != config.core {
                        continue;
                    }
                    log.commitment = commitments
                        .get(&log.block_number)
                        .copied()
                        .unwrap_or(lunarbase_client_core::Commitment::Realtime);
                    yield Ok(ExecutionEvent::Log(log));
                }
                Ok(ParserMessage::Gap(reason)) => {
                    yield Ok(ExecutionEvent::Gap { cursor: None, reason });
                    break;
                }
                Ok(ParserMessage::SubscriptionAck | ParserMessage::Ignore) => {}
                Err(error) => {
                    yield Ok(ExecutionEvent::Gap {
                        cursor: None,
                        reason: format!("invalid Monad parser message; resnapshot required: {error}"),
                    });
                    break;
                }
            }
        }
    };
    Ok(Box::pin(output))
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
    format!("0x{value:064x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_request_matches_parser_shape() {
        let address = Address::from_hex("0x0000000000000000000000000000000000000001").unwrap();
        let message = subscription_request(
            1,
            "logs",
            Some((
                &address,
                &ContractFilter {
                    address,
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
