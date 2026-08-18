//! Resumable protocol v2 WebSocket adapter.

mod session;
mod wire;

use super::{MonadParserConfig, ParserSocket, parser_payload};
use crate::execution::{ExecutionEvent, ExecutionEventStream};
use futures_util::{SinkExt, StreamExt};
use lunarbase_client::model::{ContractFilter, SourceError};
use session::ConnectionLease;
pub(super) use session::ParserV2Session;
use std::sync::Arc;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
#[cfg(test)]
use wire::StreamBounds;
use wire::{DurableRecord, GapParams, HandshakeResult, RpcEnvelope, StreamParams, SubscribeResult};

const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub(super) async fn connect(
    config: MonadParserConfig,
    filter: ContractFilter,
    session: Arc<ParserV2Session>,
) -> Result<ExecutionEventStream, SourceError> {
    let lease = session.acquire()?;
    match establish(&config, &filter, session.clone()).await {
        Ok(established) => Ok(stream(established, config, session, lease)),
        Err(error) => {
            drop(lease);
            Err(error)
        }
    }
}

struct Established {
    socket: ParserSocket,
    subscription: String,
    after_sequence: u64,
}

async fn establish(
    config: &MonadParserConfig,
    filter: &ContractFilter,
    session: Arc<ParserV2Session>,
) -> Result<Established, SourceError> {
    let bounds = WebSocketConfig {
        max_message_size: Some(config.max_frame_bytes),
        max_frame_size: Some(config.max_frame_bytes),
        ..Default::default()
    };
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let (mut socket, _) = timeout_at(
        deadline,
        connect_async_with_config(&config.ws_url, Some(bounds), false),
    )
    .await
    .map_err(|_| handshake_timeout())?
    .map_err(|error| {
        SourceError::Unavailable(format!("Monad parser v2 connect failed: {error}"))
    })?;
    let expected = session.expected_stream_id()?;
    send_text(
        &mut socket,
        wire::handshake_request(config.chain_id, expected.as_deref()),
        deadline,
    )
    .await?;
    let response = read_response(&mut socket, 1, deadline).await?;
    if let Some(error) = response.error {
        if error.code == -32003 {
            session.clear_identity_for_rebase()?;
        }
        return Err(remote_error(error));
    }
    let handshake: HandshakeResult = parse_result(response, "handshake")?;
    validate_identity(&handshake, config.chain_id)?;
    let after_sequence = session.prepare(
        &handshake.identity.stream_id,
        handshake.bounds,
        config,
        filter.clone(),
    )?;
    send_text(
        &mut socket,
        wire::subscribe_request(after_sequence, &handshake.identity.stream_id),
        deadline,
    )
    .await?;
    let response = read_response(&mut socket, 2, deadline).await?;
    if response.method.as_deref() == Some("gap") {
        let gap: GapParams = parse_params(response, "gap")?;
        session.mark_rebase()?;
        return Err(SourceError::Gap(format_gap(&gap)));
    }
    if let Some(error) = response.error {
        return Err(remote_error(error));
    }
    let subscribed: SubscribeResult = parse_result(response, "subscribe")?;
    let expected_replay_from = after_sequence
        .checked_add(1)
        .ok_or_else(|| SourceError::Gap("Monad durable sequence exhausted uint64".into()))?;
    if subscribed.stream_id != handshake.identity.stream_id
        || subscribed.subscription.is_empty()
        || subscribed.replay_from != expected_replay_from
        || subscribed.replay_through < after_sequence
    {
        return Err(SourceError::Gap(
            "Monad parser returned inconsistent replay bounds".into(),
        ));
    }
    Ok(Established {
        socket,
        subscription: subscribed.subscription,
        after_sequence,
    })
}

fn stream(
    established: Established,
    config: MonadParserConfig,
    session: Arc<ParserV2Session>,
    lease: ConnectionLease,
) -> ExecutionEventStream {
    let Established {
        socket,
        subscription,
        after_sequence,
    } = established;
    let (mut writer, mut reader) = socket.split();
    Box::pin(async_stream::stream! {
        let _lease = lease;
        let mut pending_ack: Option<(u64, u64)> = None;
        let mut next_ack_id = 3_u64;
        let mut last_seen = after_sequence;
        loop {
            let Some(message) = reader.next().await else {
                yield Ok(connection_gap(last_seen, "Monad parser v2 connection closed"));
                break;
            };
            let payload = match parser_payload(message, &mut writer).await {
                Ok(Some(payload)) => payload,
                Ok(None) => continue,
                Err(error) => {
                    yield Ok(connection_gap(last_seen, &error.to_string()));
                    break;
                }
            };
            if payload.len() > config.max_frame_bytes {
                yield Ok(connection_gap(last_seen, "Monad parser v2 frame exceeded configured bound"));
                break;
            }
            let envelope = match wire::parse_envelope(&payload) {
                Ok(envelope) => envelope,
                Err(error) => {
                    let _ = session.mark_rebase();
                    yield Ok(connection_gap(last_seen, &error.to_string()));
                    break;
                }
            };
            if let Some(error) = envelope.error {
                let _ = session.mark_rebase();
                yield Ok(connection_gap(last_seen, &remote_error(error).to_string()));
                break;
            }
            if let Some(id) = envelope.id.as_ref() {
                let Some(id) = id.as_u64() else {
                    let _ = session.mark_rebase();
                    yield Ok(connection_gap(last_seen, "Monad parser used a non-numeric response id"));
                    break;
                };
                let Some((pending_id, sequence)) = pending_ack else {
                    let _ = session.mark_rebase();
                    yield Ok(connection_gap(last_seen, "Monad parser sent an unsolicited response"));
                    break;
                };
                if id != pending_id || envelope.result != Some(serde_json::Value::Bool(true)) {
                    let _ = session.mark_rebase();
                    yield Ok(connection_gap(last_seen, "Monad parser rejected an acknowledgement"));
                    break;
                }
                pending_ack = None;
                if let Err(error) = session.confirm_ack(sequence) {
                    yield Ok(connection_gap(last_seen, &error.to_string()));
                    break;
                }
                continue;
            }
            match envelope.method.as_deref() {
                Some("gap") => {
                    let gap: GapParams = match parse_params(envelope, "gap") {
                        Ok(gap) => gap,
                        Err(error) => {
                            yield Ok(connection_gap(last_seen, &error.to_string()));
                            break;
                        }
                    };
                    let _ = session.mark_rebase();
                    yield Ok(ExecutionEvent::Gap {
                        cursor: None,
                        reason: format_gap(&gap),
                    });
                    break;
                }
                Some("stream") => {}
                _ => {
                    let _ = session.mark_rebase();
                    yield Ok(connection_gap(last_seen, "Monad parser sent an unexpected notification"));
                    break;
                }
            }
            let params: StreamParams = match parse_params(envelope, "stream") {
                Ok(params) => params,
                Err(error) => {
                    let _ = session.mark_rebase();
                    yield Ok(connection_gap(last_seen, &error.to_string()));
                    break;
                }
            };
            if params.subscription != subscription {
                let _ = session.mark_rebase();
                yield Ok(connection_gap(last_seen, "Monad parser used an unexpected subscription id"));
                break;
            }
            let sequence = params.record.sequence();
            let Some(expected_sequence) = last_seen.checked_add(1) else {
                let _ = session.mark_rebase();
                yield Ok(connection_gap(last_seen, "Monad durable sequence exhausted uint64"));
                break;
            };
            if sequence != expected_sequence {
                let _ = session.mark_rebase();
                yield Ok(connection_gap(last_seen, "Monad parser live sequence regressed or skipped"));
                break;
            }
            last_seen = sequence;
            let acknowledged = match params.record {
                DurableRecord::Gap {
                    source_sequence,
                    timestamp_ns,
                    reason,
                    recovery_required,
                    ..
                } => {
                    let _ = session.mark_rebase();
                    yield Ok(ExecutionEvent::Gap {
                        cursor: None,
                        reason: format!(
                            "Monad persisted execution gap (source={source_sequence:?}, timestamp={timestamp_ns}, recovery={recovery_required}): {reason}"
                        ),
                    });
                    break;
                }
                record @ DurableRecord::ExecEvent { .. } => {
                    let record = match record.into_exec() {
                    Ok(record) => record,
                    Err(error) => {
                        let _ = session.mark_rebase();
                        yield Ok(connection_gap(last_seen, &error.to_string()));
                        break;
                    }
                    };
                    match session.process(record) {
                        Ok((events, acknowledged)) => {
                            for event in events {
                                yield Ok(event);
                            }
                            acknowledged
                        }
                        Err(error) => {
                            let _ = session.mark_rebase();
                            yield Ok(connection_gap(last_seen, &error.to_string()));
                            break;
                        }
                    }
                }
            };
            if pending_ack.is_none()
                && sequence.saturating_sub(acknowledged) >= config.ack_interval
            {
                let id = next_ack_id;
                next_ack_id = next_ack_id.saturating_add(1);
                let request = wire::ack_request(id, &subscription, sequence);
                if let Err(error) = writer.send(Message::Text(request)).await {
                    yield Ok(connection_gap(last_seen, &format!("Monad parser ack failed: {error}")));
                    break;
                }
                pending_ack = Some((id, sequence));
            }
        }
    })
}

async fn send_text(
    socket: &mut ParserSocket,
    payload: String,
    deadline: Instant,
) -> Result<(), SourceError> {
    timeout_at(deadline, socket.send(Message::Text(payload)))
        .await
        .map_err(|_| handshake_timeout())?
        .map_err(|error| SourceError::Unavailable(format!("Monad parser v2 send failed: {error}")))
}

async fn read_response(
    socket: &mut ParserSocket,
    expected_id: u64,
    deadline: Instant,
) -> Result<RpcEnvelope, SourceError> {
    loop {
        let message = timeout_at(deadline, socket.next())
            .await
            .map_err(|_| handshake_timeout())?
            .ok_or_else(|| {
                SourceError::Unavailable("Monad parser closed during v2 handshake".into())
            })?;
        let Some(payload) = parser_payload(message, socket).await? else {
            continue;
        };
        let envelope = wire::parse_envelope(&payload)?;
        if envelope.method.as_deref() == Some("gap") {
            return Ok(envelope);
        }
        if envelope.id.as_ref().and_then(|id| id.as_u64()) != Some(expected_id) {
            return Err(SourceError::Gap(
                "Monad parser v2 returned an unexpected response id".into(),
            ));
        }
        return Ok(envelope);
    }
}

fn format_gap(gap: &GapParams) -> String {
    format!(
        "Monad parser replay gap after {} (retained {:?}..={:?}): {} (resubscribe={})",
        gap.requested_after,
        gap.earliest_sequence,
        gap.latest_sequence,
        gap.reason,
        gap.resubscribe_required,
    )
}

fn validate_identity(result: &HandshakeResult, chain_id: u64) -> Result<(), SourceError> {
    let observed = result
        .identity
        .chain_id
        .as_deref()
        .and_then(wire::parse_chain_id);
    if result.identity.protocol_version != wire::PROTOCOL_VERSION
        || observed != Some(chain_id)
        || result.identity.stream_id.is_empty()
    {
        return Err(SourceError::NetworkMismatch);
    }
    let valid_bounds = match (
        result.bounds.earliest_sequence,
        result.bounds.latest_sequence,
    ) {
        (None, None) => true,
        (Some(earliest), Some(latest)) => earliest <= latest,
        _ => false,
    };
    if !valid_bounds {
        return Err(SourceError::Gap(
            "Monad parser returned invalid durable replay bounds".into(),
        ));
    }
    Ok(())
}

fn parse_result<T: serde::de::DeserializeOwned>(
    envelope: RpcEnvelope,
    method: &str,
) -> Result<T, SourceError> {
    serde_json::from_value(
        envelope.result.ok_or_else(|| {
            SourceError::Gap(format!("Monad parser {method} response has no result"))
        })?,
    )
    .map_err(|error| SourceError::Gap(format!("invalid Monad parser {method} result: {error}")))
}

fn parse_params<T: serde::de::DeserializeOwned>(
    envelope: RpcEnvelope,
    method: &str,
) -> Result<T, SourceError> {
    serde_json::from_value(envelope.params.ok_or_else(|| {
        SourceError::Gap(format!("Monad parser {method} notification has no params"))
    })?)
    .map_err(|error| SourceError::Gap(format!("invalid Monad parser {method} params: {error}")))
}

fn remote_error(error: wire::RpcError) -> SourceError {
    if error.code == -32001 {
        SourceError::NetworkMismatch
    } else {
        SourceError::Unavailable(format!(
            "Monad parser v2 error {}: {}",
            error.code, error.message
        ))
    }
}

fn connection_gap(last_seen: u64, reason: &str) -> ExecutionEvent {
    ExecutionEvent::Gap {
        cursor: None,
        reason: format!(
            "{reason}; last durable Monad sequence {last_seen}; canonical recovery required"
        ),
    }
}

fn handshake_timeout() -> SourceError {
    SourceError::Unavailable("Monad parser v2 handshake timed out".into())
}

#[cfg(test)]
#[path = "v2_tests.rs"]
mod tests;
