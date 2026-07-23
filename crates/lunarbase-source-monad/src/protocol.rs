//! Strict decoding for the portable Monad execution-events parser protocol.

use crate::execution::{ExecutionHead, ExecutionLog};
use lunarbase_client::model::{Commitment, SourceError};
use lunarbase_math::types::{Address, B256, Bytes};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
/// Invalid, incomplete, or rejected parser WebSocket payload.
pub enum ParserProtocolError {
    /// The frame is not valid JSON.
    #[error("invalid parser JSON: {0}")]
    Json(String),
    /// A required parser field is absent or null.
    #[error("parser message is missing `{0}`")]
    MissingField(&'static str),
    /// A parser field exists but cannot be converted to its protocol type.
    #[error("parser message has invalid `{field}`: {detail}")]
    InvalidField {
        /// Stable JSON field name used for diagnostics.
        field: &'static str,
        /// Conversion or range-check failure.
        detail: String,
    },
    /// The parser returned an explicit JSON-RPC error response.
    #[error("parser returned an error: {0}")]
    RemoteError(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Typed classification of one parser WebSocket frame.
pub enum ParserMessage {
    /// Successful subscription acknowledgement with no state transition.
    SubscriptionAck,
    /// Monad block lifecycle notification.
    Head(ExecutionHead),
    /// Quote-critical candidate EVM log.
    Log(ExecutionLog),
    /// Explicit parser discontinuity that requires canonical recovery.
    Gap(String),
    /// Recognized frame that does not affect quote-critical state.
    Ignore,
}

impl From<ParserProtocolError> for SourceError {
    fn from(error: ParserProtocolError) -> Self {
        SourceError::Unavailable(error.to_string())
    }
}

/// Decodes one parser WebSocket frame into a typed normalized message.
pub fn decode_parser_message(payload: &[u8]) -> Result<ParserMessage, ParserProtocolError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|error| ParserProtocolError::Json(error.to_string()))?;
    decode_parser_value(&value)
}

/// Classifies an already parsed parser JSON value, including explicit gaps.
pub fn decode_parser_value(value: &Value) -> Result<ParserMessage, ParserProtocolError> {
    if let Some(error) = value.get("error") {
        return Err(ParserProtocolError::RemoteError(error.to_string()));
    }

    if value.get("method").and_then(Value::as_str) == Some("subscriptionGap") {
        let skipped = value
            .get("params")
            .and_then(|params| params.get("skipped"))
            .and_then(parse_u64)
            .unwrap_or(0);
        return Ok(ParserMessage::Gap(format!(
            "Monad parser subscription gap; skipped={skipped}"
        )));
    }

    if value.get("result").and_then(Value::as_str).is_some() {
        return Ok(ParserMessage::SubscriptionAck);
    }

    if value.get("method").and_then(Value::as_str) != Some("subscription") {
        return Ok(ParserMessage::Ignore);
    }
    let result = value
        .get("result")
        .ok_or(ParserProtocolError::MissingField("result"))?;
    let kind = result
        .get("type")
        .and_then(Value::as_str)
        .ok_or(ParserProtocolError::MissingField("result.type"))?;

    match kind {
        "newHead" => Ok(ParserMessage::Head(parse_head(result, false)?)),
        "blockStart" => Ok(ParserMessage::Head(parse_head(result, true)?)),
        "log" if result.get("kind").and_then(Value::as_str) == Some("event") => {
            Ok(ParserMessage::Log(parse_log(result)?))
        }
        "alert" => {
            let message = result
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Monad parser alert");
            if is_recovery_alert(message) {
                Ok(ParserMessage::Gap(message.to_owned()))
            } else {
                Ok(ParserMessage::Ignore)
            }
        }
        "health" if result.get("stalled").and_then(Value::as_bool) == Some(true) => Ok(
            ParserMessage::Gap("Monad parser reports stalled reader".into()),
        ),
        "health" => Ok(ParserMessage::Ignore),
        _ => Ok(ParserMessage::Ignore),
    }
}

fn parse_head(value: &Value, block_start: bool) -> Result<ExecutionHead, ParserProtocolError> {
    Ok(ExecutionHead {
        sequence: required_u64(value, "seqno")?,
        block_number: required_u64(value, "blockNumber")?,
        block_hash: parse_block_tag_hash(value)?,
        commitment: if block_start {
            Commitment::Realtime
        } else {
            parse_commitment(value.get("commitment"))?
        },
    })
}

fn parse_log(value: &Value) -> Result<ExecutionLog, ParserProtocolError> {
    let topics = value
        .get("topics")
        .and_then(Value::as_array)
        .ok_or(ParserProtocolError::MissingField("result.topics"))?
        .iter()
        .map(parse_b256)
        .collect::<Result<Vec<_>, _>>()?;
    let data = value
        .get("data")
        .and_then(Value::as_str)
        .ok_or(ParserProtocolError::MissingField("result.data"))?
        .parse::<Bytes>()
        .map_err(|error| ParserProtocolError::InvalidField {
            field: "data",
            detail: error.to_string(),
        })?;
    let log_index = required_u64(value, "logIndex")?;
    let transaction_index = required_u64(value, "transactionIndex")?;
    Ok(ExecutionLog {
        sequence: required_u64(value, "seqno")?,
        source_sub_index: u32::try_from(log_index).map_err(|_| {
            ParserProtocolError::InvalidField {
                field: "logIndex",
                detail: "does not fit u32".into(),
            }
        })?,
        block_number: required_u64(value, "blockNumber")?,
        block_hash: parse_optional_hex32(value.get("blockHash"), "blockHash")?,
        transaction_index: u32::try_from(transaction_index).map_err(|_| {
            ParserProtocolError::InvalidField {
                field: "transactionIndex",
                detail: "does not fit u32".into(),
            }
        })?,
        log_index: u32::try_from(log_index).map_err(|_| ParserProtocolError::InvalidField {
            field: "logIndex",
            detail: "does not fit u32".into(),
        })?,
        address: value
            .get("address")
            .and_then(Value::as_str)
            .ok_or(ParserProtocolError::MissingField("result.address"))?
            .parse::<Address>()
            .map_err(|error| ParserProtocolError::InvalidField {
                field: "address",
                detail: error.to_string(),
            })?,
        topics,
        data,
        commitment: Commitment::Realtime,
    })
}

fn parse_commitment(value: Option<&Value>) -> Result<Commitment, ParserProtocolError> {
    match value.and_then(Value::as_str) {
        Some("proposed") => Ok(Commitment::Realtime),
        Some("finalized") => Ok(Commitment::Canonical),
        Some("verified") => Ok(Commitment::Finalized),
        Some(other) => Err(ParserProtocolError::InvalidField {
            field: "commitment",
            detail: format!("unknown value `{other}`"),
        }),
        None => Err(ParserProtocolError::MissingField("commitment")),
    }
}

fn parse_block_tag_hash(value: &Value) -> Result<Option<B256>, ParserProtocolError> {
    let block_tag = value
        .get("header")
        .and_then(|header| header.get("blockTag"))
        .ok_or(ParserProtocolError::MissingField("result.header.blockTag"))?;
    parse_optional_hex32(block_tag.get("id"), "header.blockTag.id")
}

fn parse_optional_hex32(
    value: Option<&Value>,
    field: &'static str,
) -> Result<Option<B256>, ParserProtocolError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => Ok(Some(parse_hex32(
            value.as_str().ok_or(ParserProtocolError::InvalidField {
                field,
                detail: "expected hex string".into(),
            })?,
            field,
        )?)),
    }
}

fn parse_b256(value: &Value) -> Result<B256, ParserProtocolError> {
    let string = value.as_str().ok_or(ParserProtocolError::InvalidField {
        field: "topics",
        detail: "expected 32-byte hex string".into(),
    })?;
    parse_hex32(string, "topics")
}

fn parse_hex32(value: &str, field: &'static str) -> Result<B256, ParserProtocolError> {
    value
        .parse::<B256>()
        .map_err(|error| ParserProtocolError::InvalidField {
            field,
            detail: error.to_string(),
        })
}

fn required_u64(value: &Value, field: &'static str) -> Result<u64, ParserProtocolError> {
    value
        .get(field)
        .and_then(parse_u64)
        .ok_or(ParserProtocolError::MissingField(field))
}

fn parse_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn is_recovery_alert(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    ["gap", "expired", "stalled", "ring"]
        .iter()
        .any(|needle| message.contains(needle))
}

#[cfg(test)]
mod tests {
    use crate::protocol::{ParserMessage, decode_parser_message, decode_parser_value};
    use lunarbase_client::model::Commitment;
    use lunarbase_math::types::{B256, U256};
    use serde_json::json;

    #[test]
    fn new_head_commitments_map_to_normalized_confidence() {
        let value = json!({
            "jsonrpc": "2.0",
            "method": "subscription",
            "result": {
                "type": "newHead",
                "commitment": "verified",
                "seqno": 123,
                "blockNumber": 456,
                "header": { "blockTag": { "id": format!("0x{}", "ab".repeat(32)), "blockNumber": 456 } }
            }
        });
        let ParserMessage::Head(head) = decode_parser_value(&value).unwrap() else {
            panic!("expected head")
        };
        assert_eq!(head.commitment, Commitment::Finalized);
        assert_eq!(head.sequence, 123);
        assert_eq!(head.block_number, 456);
        assert_eq!(head.block_hash, Some(B256::new([0xabu8; 32])));
    }

    #[test]
    fn log_notification_keeps_global_seqno_and_evm_positions() {
        let value = json!({
            "jsonrpc": "2.0",
            "method": "subscription",
            "result": {
                "type": "log",
                "kind": "event",
                "seqno": 900,
                "blockNumber": 12,
                "logIndex": 5,
                "transactionIndex": 2,
                "address": "0x0000000000000000000000000000000000000001",
                "topics": [format!("0x{}01", "00".repeat(31))],
                "data": "0x0102"
            }
        });
        let ParserMessage::Log(log) = decode_parser_value(&value).unwrap() else {
            panic!("expected log")
        };
        assert_eq!(log.sequence, 900);
        assert_eq!(log.source_sub_index, 5);
        assert_eq!(log.transaction_index, 2);
        assert_eq!(log.log_index, 5);
        assert_eq!(log.data.as_ref(), [1, 2]);
        assert_eq!(log.topics, vec![B256::new(U256::ONE.to_be_bytes::<32>())]);
    }

    #[test]
    fn replay_fixture_preserves_sparse_logs_and_terminal_gap() {
        let fixture =
            include_str!("../../../fixtures/event-replay/monad-exec-events/parser-messages.jsonl");
        let mut saw_sparse_log = false;
        let mut saw_gap = false;
        for line in fixture.lines() {
            match decode_parser_message(line.as_bytes()).unwrap() {
                ParserMessage::Log(log) => {
                    assert_eq!(log.sequence, 1004);
                    saw_sparse_log = true;
                }
                ParserMessage::Gap(reason) => {
                    assert!(reason.contains("skipped=3"));
                    saw_gap = true;
                }
                ParserMessage::Head(_) | ParserMessage::SubscriptionAck | ParserMessage::Ignore => {
                }
            }
        }
        assert!(saw_sparse_log && saw_gap);
    }
}
