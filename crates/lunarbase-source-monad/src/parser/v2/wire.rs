//! Strict JSON shapes for the durable parser protocol.

use crate::lifecycle::RawExecRecord;
use lunarbase_client::model::SourceError;
use lunarbase_math::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};

pub(super) const PROTOCOL_VERSION: u8 = 2;
pub(super) const SUBSCRIPTION_KIND: &str = "execEventsV2";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RpcEnvelope {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<Value>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<RpcError>,
    #[serde(default)]
    pub params: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RpcError {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct HandshakeResult {
    pub identity: StreamIdentity,
    pub bounds: StreamBounds,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct StreamIdentity {
    pub protocol_version: u8,
    pub stream_id: String,
    pub chain_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct StreamBounds {
    pub earliest_sequence: Option<u64>,
    pub latest_sequence: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SubscribeResult {
    pub subscription: String,
    pub stream_id: String,
    pub replay_from: u64,
    pub replay_through: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct StreamParams {
    pub subscription: String,
    pub record: DurableRecord,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub(super) enum DurableRecord {
    #[serde(rename = "execEvent")]
    ExecEvent {
        sequence: u64,
        #[serde(rename = "sourceSequence")]
        source_sequence: u64,
        #[serde(rename = "timestampNs")]
        timestamp_ns: u64,
        #[serde(rename = "blockNumber")]
        block_number: Option<u64>,
        #[serde(rename = "eventTypeId")]
        event_type_id: u16,
        #[serde(rename = "eventName")]
        event_name: String,
        #[serde(rename = "flowInfo")]
        flow_info: FlowInfo,
        #[serde(rename = "payloadHex")]
        payload_hex: String,
    },
    #[serde(rename = "gap")]
    Gap {
        sequence: u64,
        #[serde(rename = "sourceSequence")]
        source_sequence: Option<u64>,
        #[serde(rename = "timestampNs")]
        timestamp_ns: u64,
        reason: String,
        #[serde(rename = "recoveryRequired")]
        recovery_required: bool,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FlowInfo {
    block_seqno: u64,
    txn_index: Option<usize>,
    account_index: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct GapParams {
    pub requested_after: u64,
    pub earliest_sequence: Option<u64>,
    pub latest_sequence: Option<u64>,
    pub reason: String,
    pub resubscribe_required: bool,
}

impl DurableRecord {
    pub(super) fn sequence(&self) -> u64 {
        match self {
            Self::ExecEvent { sequence, .. } | Self::Gap { sequence, .. } => *sequence,
        }
    }

    pub(super) fn into_exec(self) -> Result<RawExecRecord, SourceError> {
        let Self::ExecEvent {
            sequence,
            source_sequence,
            timestamp_ns,
            block_number,
            event_type_id,
            event_name,
            flow_info,
            payload_hex,
        } = self
        else {
            return Err(SourceError::Gap(
                "Monad durable replay contains an explicit gap".into(),
            ));
        };
        let payload = payload_hex
            .parse::<Bytes>()
            .map_err(|error| SourceError::Gap(format!("invalid Monad raw payload hex: {error}")))?;
        Ok(RawExecRecord {
            sequence,
            source_sequence,
            timestamp_ns,
            block_number,
            event_type_id,
            event_name,
            flow_block_seqno: flow_info.block_seqno,
            flow_txn_index: flow_info.txn_index,
            flow_account_index: flow_info.account_index,
            payload,
        })
    }
}

pub(super) fn handshake_request(chain_id: u64, stream_id: Option<&str>) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "handshake",
        "params": {
            "protocolVersion": PROTOCOL_VERSION,
            "chainId": chain_id,
            "expectedStreamId": stream_id,
        },
    })
    .to_string()
}

pub(super) fn subscribe_request(after_sequence: u64, stream_id: &str) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "subscribe",
        "params": [
            SUBSCRIPTION_KIND,
            {"afterSequence": after_sequence, "streamId": stream_id},
        ],
    })
    .to_string()
}

pub(super) fn ack_request(id: u64, subscription: &str, sequence: u64) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "ack",
        "params": {"subscription": subscription, "sequence": sequence},
    })
    .to_string()
}

pub(super) fn parse_envelope(payload: &[u8]) -> Result<RpcEnvelope, SourceError> {
    let envelope: RpcEnvelope = serde_json::from_slice(payload)
        .map_err(|error| SourceError::Gap(format!("invalid Monad protocol v2 JSON: {error}")))?;
    if envelope.jsonrpc != "2.0" {
        return Err(SourceError::Gap(
            "Monad protocol v2 response has an invalid jsonrpc version".into(),
        ));
    }
    Ok(envelope)
}

pub(super) fn parse_chain_id(value: &str) -> Option<u64> {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .map_or_else(
            || value.parse().ok(),
            |hex| u64::from_str_radix(hex, 16).ok(),
        )
}
