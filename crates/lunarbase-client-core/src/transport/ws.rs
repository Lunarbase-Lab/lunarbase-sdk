//! Generic Ethereum JSON-RPC WebSocket source.
//!
//! The HTTP endpoint remains the authority for block-tagged snapshots and
//! canonical backfill.  This module only consumes the low-latency `logs` and
//! `newHeads` subscriptions and normalizes them into the same source model as
//! the network-specific adapters.  A closed socket is deliberately terminal:
//! callers must recover from the HTTP source before claiming freshness again.

use crate::source::{ChainDataSource, SourceStream};
use crate::state::ordering::CursorReorderBuffer;
use crate::transport::rpc::backend::RpcHttpBackend;
use crate::transport::rpc::client::RpcHttpClient;
use crate::transport::rpc::codec::parse_rpc_log;
use crate::transport::rpc::snapshot::RpcSnapshotProvider;
use crate::{
    bootstrap::BootstrapSnapshot,
    model::{
        BackfillRequest, ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractFilter,
        ContractLog, DeploymentConfig, Network, SourceError,
    },
};
use async_stream::stream;
use futures_util::StreamExt;
use serde_json::Value;
use std::sync::Arc;

mod connection;
mod protocol;

use connection::{establish, websocket_payload};
use protocol::{WsHead, head_discontinuity, parse_ws_head};

/// Bounds transport frames and the number of updates that may wait for a
/// block-head watermark.  Both bounds are required for predictable memory
/// use when an RPC provider stops producing heads or sends a burst of logs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WsRpcConfig {
    /// Maximum accepted WebSocket frame size before the stream fails closed.
    pub max_frame_bytes: usize,
    /// Maximum normalized updates retained while waiting for an ordering watermark.
    pub reorder_capacity: usize,
    /// Ethereum subscription method, either `logs` or provider-specific `pendingLogs`.
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
    /// Canonical HTTP backend used for snapshots, backfills, and validation.
    http: RpcHttpBackend,
    /// WebSocket endpoint used exclusively for realtime subscriptions.
    ws_endpoint: Arc<str>,
    /// Frame, ordering, and subscription behavior limits.
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

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.http.backfill(request).await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        self.config.validate()?;
        let established = establish(
            self.ws_endpoint.as_ref(),
            &filter,
            &self.config.logs_subscription,
            self.http.chain_id(),
            self.config.max_frame_bytes,
        )
        .await?;
        let logs_subscription = established.logs_subscription;
        let heads_subscription = established.heads_subscription;
        let mut buffered = established.buffered;
        let socket = established.socket;
        let (mut writer, mut reader) = socket.split();

        let chain_id = self.http.chain_id();
        let config = self.config.clone();
        let stream = stream! {
            let mut reorder = match CursorReorderBuffer::new(config.reorder_capacity) {
                Ok(buffer) => buffer,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            let mut last_head: Option<WsHead> = None;
            let mut source_sequence = 0_u64;

            loop {
                let payload = if let Some(payload) = buffered.pop_front() {
                    payload
                } else {
                    let Some(message) = reader.next().await else {
                        yield Ok(ChainUpdate::Gap {
                            cursor: last_head.as_ref().map(|head| head.cursor.clone()),
                            reason: "RPC WebSocket closed; canonical recovery required".into(),
                        });
                        break;
                    };
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
                    match websocket_payload(message, &mut writer).await {
                        Ok(Some(payload)) => payload,
                        Ok(None) => continue,
                        Err(error) => {
                            yield Ok(ChainUpdate::Gap {
                                cursor: last_head.as_ref().map(|head| head.cursor.clone()),
                                reason: error.to_string(),
                            });
                            break;
                        }
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
                if value.get("id").is_some() {
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

                if logs_subscription == subscription {
                    let mut log = match parse_rpc_log(result, chain_id, Commitment::Realtime) {
                        Ok(log) => log,
                        Err(error) => {
                            yield Err(SourceError::Unavailable(format!("invalid RPC log notification: {error}")));
                            break;
                        }
                    };
                    source_sequence = source_sequence.saturating_add(1);
                    log.cursor.source_sequence = Some(source_sequence);
                    if let Some(head) = last_head.as_ref()
                        && head.cursor.block_number == log.cursor.block_number
                    {
                        log.cursor.execution_block_number =
                            head.cursor.execution_block_number;
                    }
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
                            yield Ok(with_execution_context(update, &head.cursor));
                        }
                    }
                    continue;
                }
                if heads_subscription == subscription {
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
                        yield Ok(with_execution_context(update, &head.cursor));
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

fn with_execution_context(mut update: ChainUpdate, head: &ChainCursor) -> ChainUpdate {
    if let ChainUpdate::Log(log) = &mut update
        && log.cursor.block_number == head.block_number
    {
        log.cursor.execution_block_number = head.execution_block_number;
    }
    update
}

#[cfg(test)]
#[path = "ws_tests.rs"]
mod tests;
