//! Monad parser WebSocket reader and canonical recovery source.

use async_stream::stream;
use futures_util::{SinkExt, StreamExt};
use lunarbase_client::bootstrap::BootstrapSnapshot;
use lunarbase_client::model::{
    BackfillRequest, ChainCursor, Checkpoint, Commitment, ContractFilter, ContractLog,
    DeploymentConfig, Network, SourceError,
};
use lunarbase_client::source::{ChainDataSource, SourceStream};
use lunarbase_math::{Address, B256};
use lunarbase_source_evm::rpc::backend::RpcHttpBackend;
use lunarbase_source_evm::rpc::client::RpcHttpClient;
use lunarbase_source_evm::rpc::snapshot::RpcSnapshotProvider;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    time::{Instant, timeout_at},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use url::Url;

use crate::execution::{ExecutionEvent, ExecutionEventStream, MonadExecutionNormalizer};
use crate::protocol::{ParserMessage, decode_parser_message};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
type ParserSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct ParserHandshake {
    logs_subscription: String,
    all_subscription: String,
    buffered: VecDeque<Vec<u8>>,
}

#[derive(Default)]
struct ParserHandshakeState {
    logs_subscription: Option<String>,
    all_subscription: Option<String>,
    buffered: VecDeque<Vec<u8>>,
}

impl ParserHandshakeState {
    fn is_complete(&self) -> bool {
        self.logs_subscription.is_some() && self.all_subscription.is_some()
    }

    fn finish(self) -> Result<ParserHandshake, SourceError> {
        Ok(ParserHandshake {
            logs_subscription: self.logs_subscription.ok_or_else(|| {
                SourceError::Unavailable(
                    "Monad parser logs subscription was not acknowledged".into(),
                )
            })?,
            all_subscription: self.all_subscription.ok_or_else(|| {
                SourceError::Unavailable(
                    "Monad parser all subscription was not acknowledged".into(),
                )
            })?,
            buffered: self.buffered,
        })
    }
}

/// Resource and identity settings for the local Monad parser connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonadParserConfig {
    /// Parser WebSocket subscription endpoint.
    pub ws_url: String,
    /// Core contract used to reject unrelated parser logs.
    pub core: Address,
    /// EIP-155 chain identifier attached to normalized updates.
    pub chain_id: u64,
    /// Maximum accepted WebSocket frame size before fail-closed recovery.
    pub max_frame_bytes: usize,
    /// Maximum notifications retained while subscription acknowledgements arrive.
    pub max_prefetched_frames: usize,
}

impl Default for MonadParserConfig {
    fn default() -> Self {
        Self {
            ws_url: "ws://127.0.0.1:8080/ws/subscriptions".into(),
            core: Address::ZERO,
            chain_id: 143,
            max_frame_bytes: 64 * 1024,
            max_prefetched_frames: 4096,
        }
    }
}

impl MonadParserConfig {
    /// Validates parser endpoint, chain id, and frame-memory bounds.
    pub fn validate(&self) -> Result<(), SourceError> {
        let url = Url::parse(&self.ws_url).map_err(|error| {
            SourceError::Unavailable(format!("invalid Monad parser URL: {error}"))
        })?;
        if !matches!(url.scheme(), "ws" | "wss") {
            return Err(SourceError::Unavailable(
                "Monad parser URL must use ws or wss".into(),
            ));
        }
        if self.chain_id == 0
            || self.core == Address::ZERO
            || self.max_frame_bytes == 0
            || self.max_prefetched_frames == 0
        {
            return Err(SourceError::Unavailable(
                "Monad parser chain id, Core, and resource bounds must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

/// Portable parser-WebSocket source with pinned latest-state RPC recovery.
pub struct MonadParserSource {
    /// Validated identity and resource limits for the portable parser.
    config: MonadParserConfig,
    /// Latest executed HTTP source used for snapshots, backfill, and checkpoint validation.
    canonical: RpcHttpBackend,
}

impl MonadParserSource {
    /// Creates a source from parser and canonical RPC endpoints.
    pub fn new(
        config: MonadParserConfig,
        rpc_endpoint: impl Into<String>,
    ) -> Result<Self, SourceError> {
        config.validate()?;
        let canonical = RpcHttpBackend::new(
            RpcHttpClient::new(rpc_endpoint).map_err(SourceError::from)?,
            Network::Monad,
            config.chain_id,
            "latest",
        );
        Ok(Self { config, canonical })
    }

    /// Returns the immutable parser configuration.
    pub fn config(&self) -> &MonadParserConfig {
        &self.config
    }

    /// Returns the canonical RPC helper.
    pub fn canonical(&self) -> &RpcHttpBackend {
        &self.canonical
    }
}

impl ChainDataSource for MonadParserSource {
    fn network(&self) -> Network {
        Network::Monad
    }

    async fn snapshot(
        &self,
        deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        RpcSnapshotProvider::new(
            self.canonical.rpc().clone(),
            self.canonical.snapshot_tag().to_owned(),
        )
        .snapshot(deployment)
        .await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.canonical.backfill(request).await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        let events = connect_parser_stream(self.config.clone(), filter).await?;
        Ok(MonadExecutionNormalizer::new(self.config.chain_id).normalize_stream(events))
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        self.canonical.snapshot_cursor(Network::Monad).await
    }

    async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        self.canonical.validate_checkpoint(checkpoint).await
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
    let bounds = WebSocketConfig {
        max_message_size: Some(config.max_frame_bytes),
        max_frame_size: Some(config.max_frame_bytes),
        ..Default::default()
    };
    let handshake_deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let (mut socket, _) = timeout_at(
        handshake_deadline,
        connect_async_with_config(&config.ws_url, Some(bounds), false),
    )
    .await
    .map_err(|_| handshake_timed_out())?
    .map_err(|error| SourceError::Unavailable(format!("Monad parser connect failed: {error}")))?;
    timeout_at(
        handshake_deadline,
        socket.send(Message::Text(subscription_request(
            1,
            "logs",
            Some((&config.core, &filter)),
        ))),
    )
    .await
    .map_err(|_| handshake_timed_out())?
    .map_err(|error| SourceError::Unavailable(format!("Monad parser subscribe failed: {error}")))?;
    timeout_at(
        handshake_deadline,
        socket.send(Message::Text(subscription_request(2, "all", None))),
    )
    .await
    .map_err(|_| handshake_timed_out())?
    .map_err(|error| SourceError::Unavailable(format!("Monad parser subscribe failed: {error}")))?;
    let handshake = timeout_at(
        handshake_deadline,
        read_acknowledgements(
            &mut socket,
            config.max_frame_bytes,
            config.max_prefetched_frames,
        ),
    )
    .await
    .map_err(|_| handshake_timed_out())??;
    let mut buffered = handshake.buffered;
    let logs_subscription = handshake.logs_subscription;
    let all_subscription = handshake.all_subscription;
    let (mut writer, mut reader) = socket.split();

    let output = stream! {
        let mut commitments = BTreeMap::new();
        loop {
            let payload = if let Some(payload) = buffered.pop_front() {
                payload
            } else {
                let Some(message) = reader.next().await else {
                    yield Ok(ExecutionEvent::Gap {
                        cursor: None,
                        reason: "Monad parser connection closed; resubscribe required".into(),
                    });
                    break;
                };
                match parser_payload(message, &mut writer).await {
                    Ok(Some(payload)) => payload,
                    Ok(None) => continue,
                    Err(error) => {
                        yield Ok(ExecutionEvent::Gap {
                            cursor: None,
                            reason: format!("Monad parser websocket error; resubscribe required: {error}"),
                        });
                        break;
                    }
                }
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
                    if let Err(error) =
                        validate_notification_subscription(&payload, &all_subscription, "head")
                    {
                        yield Ok(ExecutionEvent::Gap {
                            cursor: None,
                            reason: error.to_string(),
                        });
                        break;
                    }
                    commitments.insert(head.block_number, head.commitment);
                    while commitments.len() > 64 {
                        commitments.pop_first();
                    }
                    yield Ok(ExecutionEvent::Head(head));
                }
                Ok(ParserMessage::Log(mut log)) => {
                    if let Err(error) =
                        validate_notification_subscription(&payload, &logs_subscription, "log")
                    {
                        yield Ok(ExecutionEvent::Gap {
                            cursor: None,
                            reason: error.to_string(),
                        });
                        break;
                    }
                    if log.address != config.core {
                        continue;
                    }
                    log.commitment = commitments
                        .get(&log.block_number)
                        .copied()
                        .unwrap_or(Commitment::Realtime);
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

async fn read_acknowledgements(
    socket: &mut ParserSocket,
    max_frame_bytes: usize,
    max_prefetched_frames: usize,
) -> Result<ParserHandshake, SourceError> {
    let mut state = ParserHandshakeState::default();
    while !state.is_complete() {
        let message = socket
            .next()
            .await
            .ok_or_else(|| SourceError::Unavailable("Monad parser closed during handshake".into()))?
            .map_err(|error| {
                SourceError::Unavailable(format!("Monad parser handshake failed: {error}"))
            })?;
        let Some(payload) = parser_payload(Ok(message), socket).await? else {
            continue;
        };
        if payload.len() > max_frame_bytes {
            return Err(SourceError::Unavailable(
                "Monad parser handshake frame exceeded configured bound".into(),
            ));
        }
        observe_handshake_payload(&mut state, payload, max_prefetched_frames)?;
    }
    state.finish()
}

fn observe_handshake_payload(
    state: &mut ParserHandshakeState,
    payload: Vec<u8>,
    max_prefetched_frames: usize,
) -> Result<(), SourceError> {
    let value: Value = serde_json::from_slice(&payload).map_err(|error| {
        SourceError::Unavailable(format!("invalid Monad parser handshake JSON: {error}"))
    })?;
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(SourceError::Unavailable(format!(
            "Monad parser subscription rejected: {error}"
        )));
    }

    let Some(id_value) = value.get("id") else {
        if state.buffered.len() >= max_prefetched_frames {
            return Err(SourceError::Unavailable(
                "Monad parser handshake prefetch exceeded configured bound".into(),
            ));
        }
        state.buffered.push_back(payload);
        return Ok(());
    };
    let id = id_value
        .as_u64()
        .filter(|id| matches!(id, 1 | 2))
        .ok_or_else(|| {
            SourceError::Unavailable(
                "Monad parser acknowledgement has an unexpected numeric id".into(),
            )
        })?;
    let subscription = value
        .get("result")
        .and_then(Value::as_str)
        .filter(|subscription| !subscription.is_empty())
        .ok_or_else(|| {
            SourceError::Unavailable("Monad parser acknowledgement has no subscription id".into())
        })?;
    let current = if id == 1 {
        &mut state.logs_subscription
    } else {
        &mut state.all_subscription
    };
    if let Some(previous) = current {
        if previous != subscription {
            return Err(SourceError::Unavailable(format!(
                "Monad parser acknowledgement {id} changed subscription id"
            )));
        }
    } else {
        *current = Some(subscription.to_owned());
    }
    Ok(())
}

fn handshake_timed_out() -> SourceError {
    SourceError::Unavailable("Monad parser subscription handshake timed out".into())
}

fn validate_notification_subscription(
    payload: &[u8],
    expected: &str,
    kind: &str,
) -> Result<(), SourceError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|error| SourceError::Unavailable(format!("invalid Monad parser JSON: {error}")))?;
    let subscription = value
        .get("result")
        .and_then(|result| result.get("subscription"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            SourceError::Gap(format!(
                "Monad parser {kind} notification has no subscription id"
            ))
        })?;
    if subscription != expected {
        return Err(SourceError::Gap(format!(
            "Monad parser {kind} notification used an unexpected subscription id"
        )));
    }
    Ok(())
}

async fn parser_payload<S>(
    message: Result<Message, tokio_tungstenite::tungstenite::Error>,
    writer: &mut S,
) -> Result<Option<Vec<u8>>, SourceError>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let message = message
        .map_err(|error| SourceError::Unavailable(format!("Monad parser read failed: {error}")))?;
    match message {
        Message::Text(text) => Ok(Some(text.as_bytes().to_vec())),
        Message::Binary(bytes) => Ok(Some(bytes.to_vec())),
        Message::Ping(bytes) => {
            writer.send(Message::Pong(bytes)).await.map_err(|error| {
                SourceError::Unavailable(format!("Monad parser pong failed: {error}"))
            })?;
            Ok(None)
        }
        Message::Pong(_) => Ok(None),
        Message::Close(frame) => Err(SourceError::Gap(frame.map_or_else(
            || "Monad parser connection closed".into(),
            |frame| format!("Monad parser connection closed: {}", frame.reason),
        ))),
        _ => Ok(None),
    }
}

fn subscription_request(
    id: u64,
    kind: &str,
    core_filter: Option<(&Address, &ContractFilter)>,
) -> String {
    let params = match core_filter {
        Some((core, filter)) => {
            let mut options = serde_json::Map::new();
            options.insert("address".into(), Value::String(format!("{core:#x}")));
            if !filter.topics.is_empty() {
                options.insert(
                    "topics".into(),
                    json!([filter
                        .topics
                        .iter()
                        .map(|topic| word_hex(*topic))
                        .collect::<Vec<_>>()]),
                );
            }
            json!([kind, Value::Object(options)])
        }
        None => json!([kind]),
    };
    json!({"jsonrpc":"2.0", "id":id, "method":"subscribe", "params":params}).to_string()
}

fn word_hex(value: B256) -> String {
    format!("{value:#x}")
}

#[cfg(test)]
mod tests;
