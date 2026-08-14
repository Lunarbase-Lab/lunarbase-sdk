//! Versioned Redis Stream representation of one raw Core contract log.

use alloy_primitives::{Address, keccak256};
use lunarbase_client::model::{ChainCursor, Commitment, ContractLog};
use lunarbase_client::protocol::abi::describe_core_event;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub(crate) const STREAM_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug)]
pub(crate) struct DurableEvent {
    pub event_id: String,
    pub cursor_json: String,
    pub cursor_order: String,
    pub fields: Vec<(&'static str, String)>,
}

#[derive(Debug, Error)]
pub(crate) enum EventError {
    #[error("event JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("durable cursor belongs to another deployment")]
    CursorIdentity,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CursorEnvelope {
    schema_version: u16,
    chain_id: u64,
    core: String,
    cursor: ChainCursor,
}

impl DurableEvent {
    pub(crate) fn from_log(log: &ContractLog) -> Result<Self, EventError> {
        let event_id = core_event_id(log);
        let operation = if log.removed { "removed" } else { "applied" };
        let (event_name, arguments, decode_error) = match describe_core_event(log) {
            Ok(Some(description)) => (description.name, description.arguments, String::new()),
            Ok(None) => ("Unknown", String::new(), String::new()),
            Err(error) => ("Malformed", String::new(), error.to_string()),
        };
        let cursor_json = encode_cursor(&log.cursor, log.address)?;
        let fields = vec![
            ("schemaVersion", STREAM_SCHEMA_VERSION.to_string()),
            ("eventId", event_id.clone()),
            ("operation", operation.into()),
            ("eventName", event_name.into()),
            ("arguments", arguments),
            ("decodeError", decode_error),
            ("chainId", log.cursor.chain_id.to_string()),
            ("core", format!("{:#x}", log.address)),
            ("blockNumber", log.cursor.block_number.to_string()),
            (
                "executionBlockNumber",
                log.cursor.execution_block_number.to_string(),
            ),
            ("blockHash", option_hash(log.cursor.block_hash)),
            ("transactionHash", option_hash(log.transaction_hash)),
            (
                "transactionIndex",
                option_number(log.cursor.transaction_index),
            ),
            ("logIndex", option_number(log.cursor.log_index)),
            ("sourceSequence", option_number(log.cursor.source_sequence)),
            ("sourceSubIndex", option_number(log.cursor.source_sub_index)),
            ("commitment", commitment_name(log.cursor.commitment).into()),
            ("removed", log.removed.to_string()),
            (
                "topic0",
                log.topics
                    .first()
                    .map_or_else(String::new, |topic| format!("{topic:#x}")),
            ),
            ("topics", serde_json::to_string(&log.topics)?),
            ("data", format!("{:#x}", log.data)),
            ("rawLog", serde_json::to_string(log)?),
        ];
        Ok(Self {
            event_id,
            cursor_json,
            cursor_order: cursor_order(&log.cursor),
            fields,
        })
    }
}

pub(crate) fn decode_cursor(
    payload: &[u8],
    expected_chain_id: u64,
    expected_core: Address,
) -> Result<ChainCursor, EventError> {
    let envelope: CursorEnvelope = serde_json::from_slice(payload)?;
    if envelope.schema_version != STREAM_SCHEMA_VERSION
        || envelope.chain_id != expected_chain_id
        || envelope.core != format!("{expected_core:#x}")
        || envelope.cursor.chain_id != expected_chain_id
    {
        return Err(EventError::CursorIdentity);
    }
    Ok(envelope.cursor)
}

fn encode_cursor(cursor: &ChainCursor, core: Address) -> Result<String, serde_json::Error> {
    serde_json::to_string(&CursorEnvelope {
        schema_version: STREAM_SCHEMA_VERSION,
        chain_id: cursor.chain_id,
        core: format!("{core:#x}"),
        cursor: cursor.clone(),
    })
}

fn cursor_order(cursor: &ChainCursor) -> String {
    let (block, transaction, log, sequence, sub_index) = cursor.event_order();
    format!("{block:020}:{transaction:010}:{log:010}:{sequence:020}:{sub_index:010}")
}

pub(crate) fn core_event_id(log: &ContractLog) -> String {
    let stable_position = log.cursor.log_index.is_some()
        && (log.transaction_hash.is_some() || log.cursor.transaction_index.is_some());
    let fallback_bytes = if stable_position {
        0
    } else {
        log.topics.len() * 32 + log.data.len()
    };
    let mut preimage = Vec::with_capacity(192 + fallback_bytes);
    preimage.extend_from_slice(b"lunarbase-core-event-v2");
    preimage.extend_from_slice(&log.cursor.chain_id.to_be_bytes());
    preimage.extend_from_slice(&log.cursor.block_number.to_be_bytes());
    push_optional_hash(&mut preimage, log.cursor.block_hash);
    push_optional_hash(&mut preimage, log.transaction_hash);
    push_optional_u32(&mut preimage, log.cursor.transaction_index);
    push_optional_u32(&mut preimage, log.cursor.log_index);
    preimage.extend_from_slice(log.address.as_slice());
    preimage.push(u8::from(log.removed));
    if !stable_position {
        push_optional_u64(&mut preimage, log.cursor.source_sequence);
        push_optional_u32(&mut preimage, log.cursor.source_sub_index);
        preimage.extend_from_slice(&(log.topics.len() as u64).to_be_bytes());
        for topic in &log.topics {
            preimage.extend_from_slice(topic.as_slice());
        }
        preimage.extend_from_slice(&(log.data.len() as u64).to_be_bytes());
        preimage.extend_from_slice(&log.data);
    }
    format!("v2:{:#x}", keccak256(preimage))
}

fn push_optional_hash(payload: &mut Vec<u8>, value: Option<alloy_primitives::B256>) {
    match value {
        Some(value) => {
            payload.push(1);
            payload.extend_from_slice(value.as_slice());
        }
        None => payload.push(0),
    }
}

fn push_optional_u32(payload: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            payload.push(1);
            payload.extend_from_slice(&value.to_be_bytes());
        }
        None => payload.push(0),
    }
}

fn push_optional_u64(payload: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            payload.push(1);
            payload.extend_from_slice(&value.to_be_bytes());
        }
        None => payload.push(0),
    }
}

fn option_hash(value: Option<alloy_primitives::B256>) -> String {
    value.map_or_else(String::new, |hash| format!("{hash:#x}"))
}

fn option_number<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(String::new, |number| number.to_string())
}

fn commitment_name(commitment: Commitment) -> &'static str {
    match commitment {
        Commitment::Realtime => "realtime",
        Commitment::Canonical => "block-ordered",
        Commitment::Finalized => "finalized",
    }
}

#[cfg(test)]
mod tests {
    use super::{DurableEvent, core_event_id, decode_cursor};
    use alloy_primitives::{Address, B256, Bytes};
    use lunarbase_client::model::{ChainCursor, Commitment, ContractLog};

    #[test]
    fn stable_ids_distinguish_removal_and_fallback_payloads() {
        let applied = log(false, None, 1);
        let removed = log(true, None, 1);
        let other_payload = log(false, None, 2);
        assert_ne!(core_event_id(&applied), core_event_id(&removed));
        assert_ne!(core_event_id(&applied), core_event_id(&other_payload));
        assert_eq!(core_event_id(&applied).len(), 69);
    }

    #[test]
    fn stable_ids_distinguish_absent_positions_from_zero() {
        let absent = log(false, None, 1);
        let mut zero = absent.clone();
        zero.cursor.transaction_index = Some(0);
        zero.cursor.log_index = Some(0);
        assert_ne!(core_event_id(&absent), core_event_id(&zero));
    }

    #[test]
    fn stream_event_round_trips_deployment_bound_cursor() {
        let log = log(false, Some(3), 1);
        let event = DurableEvent::from_log(&log).unwrap();
        let cursor = decode_cursor(event.cursor_json.as_bytes(), 8453, log.address).unwrap();
        assert_eq!(cursor, log.cursor);
        assert!(
            event
                .fields
                .iter()
                .any(|(name, value)| { *name == "operation" && value == "applied" })
        );
    }

    fn log(removed: bool, log_index: Option<u32>, payload: u8) -> ContractLog {
        ContractLog {
            address: Address::new([4; 20]),
            transaction_hash: log_index.map(|_| B256::new([3; 32])),
            topics: vec![B256::new([payload; 32])],
            data: Bytes::from(vec![payload; 64]),
            removed,
            cursor: ChainCursor {
                chain_id: 8453,
                block_number: 41,
                execution_block_number: 41,
                block_hash: Some(B256::new([2; 32])),
                transaction_index: log_index.map(|_| 2),
                log_index,
                source_sequence: Some(7),
                source_sub_index: Some(1),
                commitment: Commitment::Realtime,
            },
        }
    }
}
