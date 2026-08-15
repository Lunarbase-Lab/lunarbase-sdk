//! Generic Ethereum JSON-RPC WebSocket source.
//!
//! The HTTP endpoint remains the authority for block-tagged snapshots and
//! canonical backfill. Standard EVM `logs` and `newHeads` subscriptions are
//! not ordered relative to each other, so a normal EVM profile closes block
//! `N` only after a later head arrives and a bounded grace period has elapsed.
//! Provider-specific pending-log profiles may consume subscription logs
//! immediately. A closed socket is deliberately terminal: callers must
//! recover from the HTTP source before claiming freshness again.

use crate::rpc::backend::RpcHttpBackend;
use crate::rpc::codec::parse_filtered_rpc_log;
use crate::rpc::snapshot::RpcSnapshotProvider;
use async_stream::stream;
use futures_util::StreamExt;
use lunarbase_client::{
    bootstrap::BootstrapSnapshot,
    model::{
        BackfillRequest, ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractFilter,
        ContractLog, DeploymentConfig, Network, SourceError,
    },
    source::{ChainDataSource, SourceStream},
    state::ordering::CursorReorderBuffer,
};
use serde_json::Value;
use std::{collections::VecDeque, sync::Arc, time::Instant};

mod config;
mod connection;
mod ordering;
pub(crate) mod protocol;

pub use config::{EvmDeliveryMode, WsRpcConfig};

use connection::{establish, websocket_payload};
use ordering::{
    backfill_pages, drain_completed_block, is_at_or_before_watermark, observe_standard_head,
    promote_updates, retraction_updates, standard_head_deadline, take_ready_standard_head,
    validate_finalized_advance, validate_finalized_page, validate_preceding_startup_logs,
    with_execution_context,
};
use protocol::{WsHead, head_discontinuity, parse_ws_head_with_execution_context, same_head};

/// Generic EVM source with HTTP authority and WebSocket realtime updates.
#[derive(Clone)]
pub struct EvmRpcSource {
    /// Canonical HTTP backend used for snapshots, backfills, and validation.
    http: RpcHttpBackend,
    /// WebSocket endpoint used exclusively for realtime subscriptions.
    ws_endpoint: Arc<str>,
    /// Frame, ordering, and subscription behavior limits.
    config: WsRpcConfig,
}

impl ChainDataSource for EvmRpcSource {
    fn network(&self) -> Network {
        self.http.network()
    }

    async fn snapshot(
        &self,
        deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        self.validate_config()?;
        if deployment.network != self.http.network() {
            return Err(SourceError::NetworkMismatch);
        }
        if deployment.chain_id != self.http.chain_id() {
            return Err(SourceError::Unavailable(
                "RPC source chain id mismatch".into(),
            ));
        }
        RpcSnapshotProvider::new(self.http.rpc().clone(), self.http.snapshot_tag().to_owned())
            .snapshot(deployment)
            .await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.validate_config()?;
        self.http.backfill(request).await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        self.validate_config()?;
        let (_, established) = tokio::try_join!(
            self.http.verify_chain_id(),
            establish(
                self.ws_endpoint.as_ref(),
                &filter,
                &self.config.logs_subscription,
                self.http.chain_id(),
                self.config.max_frame_bytes,
                self.config.reorder_capacity,
                self.config.max_prefetch_bytes,
            ),
        )?;
        let logs_subscription = established.logs_subscription;
        let heads_subscription = established.heads_subscription;
        let mut buffered = established.buffered;
        let socket = established.socket;
        let (mut writer, mut reader) = socket.split();

        let chain_id = self.http.chain_id();
        let http = self.http.clone();
        let config = self.config.clone();
        let initial_finalized = if config.delivery_mode == EvmDeliveryMode::Finalized {
            Some(http.snapshot_cursor(http.network()).await?)
        } else {
            None
        };
        let stream = stream! {
            let mut reorder = match CursorReorderBuffer::with_limits(
                config.reorder_capacity,
                config.reorder_byte_capacity,
            ) {
                Ok(buffer) => buffer,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            let mut last_head: Option<WsHead> = None;
            let mut open_standard_heads = VecDeque::new();
            let mut first_standard_head_block = None;
            let mut published_watermark: Option<ChainCursor> = None;
            let mut finalized_watermark = initial_finalized;
            let mut source_sequence = 0_u64;

            loop {
                let payload = if let Some(payload) = buffered.pop_front() {
                    payload
                } else {
                    let message = if config.holds_standard_logs_until_successor()
                        && let Some(deadline) = standard_head_deadline(&open_standard_heads)
                    {
                        tokio::select! {
                            biased;
                            () = tokio::time::sleep_until(deadline.into()) => {
                                let Some(completed_head) =
                                    take_ready_standard_head(&mut open_standard_heads, Instant::now())
                                else {
                                    continue;
                                };
                                let mut updates = match drain_completed_block(
                                    &mut reorder,
                                    &completed_head,
                                    first_standard_head_block == Some(completed_head.cursor.block_number),
                                ) {
                                    Ok(updates) => updates,
                                    Err(error) => {
                                        yield Err(error);
                                        break;
                                    }
                                };
                                if let Err(error) = validate_preceding_startup_logs(
                                    &mut updates,
                                    &completed_head,
                                    &http,
                                )
                                .await
                                {
                                    yield Err(error);
                                    break;
                                }
                                promote_updates(&mut updates, Commitment::Canonical);
                                for update in updates {
                                    yield Ok(update);
                                }
                                published_watermark = Some(completed_head.cursor);
                                continue;
                            },
                            message = reader.next() => message,
                        }
                    } else {
                        reader.next().await
                    };
                    let Some(message) = message else {
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
                    if config.delivery_mode == EvmDeliveryMode::Finalized {
                        continue;
                    }
                    let mut log = match parse_filtered_rpc_log(
                        result,
                        chain_id,
                        config.delivery_mode.commitment(),
                        &filter,
                    ) {
                        Ok(log) => log,
                        Err(error) => {
                            yield Err(SourceError::Unavailable(format!("invalid RPC log notification: {error}")));
                            break;
                        }
                    };
                    source_sequence = source_sequence.saturating_add(1);
                    log.cursor.source_sequence = Some(source_sequence);
                    if log.removed {
                        for update in retraction_updates(log) {
                            yield Ok(update);
                        }
                        break;
                    }
                    if config.holds_standard_logs_until_successor()
                        && is_at_or_before_watermark(&log.cursor, published_watermark.as_ref())
                    {
                        yield Ok(ChainUpdate::Gap {
                            cursor: Some(log.cursor),
                            reason: "RPC delivered a log after its block watermark; canonical recovery required".into(),
                        });
                        break;
                    }
                    if let Some(head) = last_head.as_ref()
                        && head.cursor.block_number == log.cursor.block_number
                    {
                        log.cursor.execution_block_number =
                            head.cursor.execution_block_number;
                    }
                    if config.delivery_mode == EvmDeliveryMode::Realtime {
                        yield Ok(ChainUpdate::Log(log));
                        continue;
                    }
                    let update = ChainUpdate::Log(log);
                    match reorder.push(update) {
                        Ok(_) => {}
                        Err(error) => {
                            yield Err(error);
                            break;
                        }
                    }
                    if !config.holds_standard_logs_until_successor()
                        && let Some(head) = last_head.as_ref()
                    {
                        for update in reorder.drain_through(&head.cursor) {
                            yield Ok(with_execution_context(update, &head.cursor));
                        }
                    }
                    continue;
                }
                if heads_subscription == subscription {
                    let mut head = match parse_ws_head_with_execution_context(
                        result,
                        chain_id,
                        http.network() == Network::Arbitrum,
                    ) {
                        Ok(head) => head,
                        Err(error) => {
                            yield Err(SourceError::Unavailable(format!("invalid RPC head notification: {error}")));
                            break;
                        }
                    };
                    if last_head
                        .as_ref()
                        .is_some_and(|previous| same_head(previous, &head))
                    {
                        continue;
                    }
                    if config.delivery_mode == EvmDeliveryMode::Finalized {
                        last_head = Some(head);
                        let finalized = match http.snapshot_cursor(http.network()).await {
                            Ok(cursor) => cursor,
                            Err(error) => {
                                yield Err(error);
                                break;
                            }
                        };
                        let previous = finalized_watermark
                            .as_ref()
                            .expect("finalized mode initializes its watermark");
                        match validate_finalized_advance(previous, &finalized) {
                            Ok(false) => {
                                yield Ok(ChainUpdate::Head(finalized));
                                continue;
                            }
                            Err(error) => {
                                yield Err(error);
                                break;
                            }
                            Ok(true) => {}
                        }
                        let from_block = previous.block_number.saturating_add(1);
                        for page in backfill_pages(
                            from_block,
                            finalized.block_number,
                            config.backfill_page_blocks,
                        ) {
                            let request = BackfillRequest {
                                from_block: *page.start(),
                                to_block: *page.end(),
                                filter: filter.clone(),
                            };
                            let logs = match http.backfill(request).await {
                                Ok(logs) => logs,
                                Err(error) => {
                                    yield Err(error);
                                    return;
                                }
                            };
                            let logs = match validate_finalized_page(logs, &page) {
                                Ok(logs) => logs,
                                Err(error) => {
                                    yield Err(error);
                                    return;
                                }
                            };
                            for log in logs {
                                yield Ok(ChainUpdate::Log(log));
                            }
                        }
                        yield Ok(ChainUpdate::Head(finalized.clone()));
                        finalized_watermark = Some(finalized);
                        continue;
                    }
                    source_sequence = source_sequence.saturating_add(1);
                    head.cursor.source_sequence = Some(source_sequence);
                    head.cursor.commitment = config.delivery_mode.commitment();
                    if config.holds_standard_logs_until_successor()
                        && first_standard_head_block.is_none()
                    {
                        first_standard_head_block = Some(head.cursor.block_number);
                    }
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
                            reorder = match CursorReorderBuffer::with_limits(
                                config.reorder_capacity,
                                config.reorder_byte_capacity,
                            ) {
                                Ok(buffer) => buffer,
                                Err(error) => {
                                    yield Err(error);
                                    break;
                                }
                            };
                            open_standard_heads.clear();
                            first_standard_head_block = Some(head.cursor.block_number);
                        }
                    }
                    last_head = Some(head.clone());
                    if config.delivery_mode == EvmDeliveryMode::Realtime {
                        yield Ok(ChainUpdate::Head(head.cursor));
                        continue;
                    }
                    if config.holds_standard_logs_until_successor()
                        && let Err(error) = observe_standard_head(
                            &mut open_standard_heads,
                            head.clone(),
                            Instant::now(),
                            config.reorder_capacity,
                            config.reorder_byte_capacity,
                        )
                    {
                        yield Err(error);
                        break;
                    }
                    if let Err(error) = reorder.push(ChainUpdate::Head(head.cursor.clone())) {
                        yield Err(error);
                        break;
                    }
                    if !config.holds_standard_logs_until_successor() {
                        for update in reorder.drain_through(&head.cursor) {
                            yield Ok(with_execution_context(update, &head.cursor));
                        }
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        self.validate_config()?;
        self.http.snapshot_cursor(self.http.network()).await
    }

    async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        self.validate_config()?;
        self.http.validate_checkpoint(checkpoint).await
    }
}

#[cfg(test)]
mod delivery_tests;
#[cfg(test)]
#[path = "ws_tests.rs"]
mod tests;
