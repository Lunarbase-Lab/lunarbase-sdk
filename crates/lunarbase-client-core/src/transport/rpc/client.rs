use super::codec::{
    hex_u64, parse_hex_bytes, parse_hex_u64, parse_optional_hash, parse_rpc_log, word_hex,
};
use crate::{BackfillRequest, ChainCursor, Commitment, ContractLog, SourceError};
use lunarbase_math::Address;
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use thiserror::Error;

pub(super) const SELECTOR_CASH: &str = "0x961be391";
pub(super) const SELECTOR_LANE: &str = "0xd1bacd10";
pub(super) const SELECTOR_RESERVES: &str = "0xd66bd524";
pub(super) const SELECTOR_WHITELIST: &str = "0x9b19251a";
pub(super) const SELECTOR_BLACKLIST_FEE_MULTIPLIER: &str = "0x93b6ab27";
pub(super) const SELECTOR_PARTNERS: &str = "0xaa5f434c";

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Failure returned by the HTTP JSON-RPC boundary.
pub enum RpcError {
    #[error("HTTP RPC request failed: {0}")]
    Http(String),
    #[error("RPC response JSON is invalid: {0}")]
    Json(String),
    #[error("RPC returned error {code}: {message}")]
    Remote { code: i64, message: String },
    #[error("RPC response is invalid: {0}")]
    Invalid(String),
}

impl From<RpcError> for SourceError {
    fn from(error: RpcError) -> Self {
        SourceError::Unavailable(error.to_string())
    }
}

#[derive(Clone)]
/// Minimal pooled HTTP JSON-RPC client.
pub struct RpcHttpClient {
    endpoint: Arc<str>,
    client: Client,
    next_id: Arc<AtomicU64>,
}

impl RpcHttpClient {
    /// Creates a JSON-RPC client with a shared HTTP connection pool and
    /// monotonic request ids.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: Arc::from(endpoint.into()),
            client: Client::new(),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Returns the configured JSON-RPC endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Executes one JSON-RPC request and converts remote/HTTP/shape failures
    /// into [`RpcError`].
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let response = self
            .client
            .post(self.endpoint.as_ref())
            .json(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }))
            .send()
            .await
            .map_err(|error| RpcError::Http(error.to_string()))?;
        let status = response.status();
        let value: Value = response
            .json()
            .await
            .map_err(|error| RpcError::Json(error.to_string()))?;
        if !status.is_success() {
            return Err(RpcError::Http(format!("HTTP status {status}: {value}")));
        }
        if let Some(error) = value.get("error") {
            return Err(RpcError::Remote {
                code: error.get("code").and_then(Value::as_i64).unwrap_or(-1),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown RPC error")
                    .into(),
            });
        }
        value
            .get("result")
            .cloned()
            .ok_or_else(|| RpcError::Invalid("missing JSON-RPC result".into()))
    }

    /// Calls `eth_call` at an explicit block tag and returns raw hex bytes.
    pub async fn call_at(
        &self,
        to: Address,
        data: String,
        block_tag: &str,
    ) -> Result<String, RpcError> {
        let result = self
            .call(
                "eth_call",
                json!([{"to": to.to_hex(), "data": data}, block_tag]),
            )
            .await?;
        result
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| RpcError::Invalid("eth_call result is not a hex string".into()))
    }

    /// Fetches contract runtime bytecode at a block tag for code-hash checks.
    pub async fn get_code(&self, address: Address, block_tag: &str) -> Result<Vec<u8>, RpcError> {
        let result = self
            .call("eth_getCode", json!([address.to_hex(), block_tag]))
            .await?;
        parse_hex_bytes(
            result.as_str().ok_or_else(|| {
                RpcError::Invalid("eth_getCode result is not a hex string".into())
            })?,
        )
    }

    /// Converts an RPC block header into the normalized snapshot cursor.
    pub async fn block_cursor(
        &self,
        block_tag: &str,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<ChainCursor, RpcError> {
        let result = self
            .call("eth_getBlockByNumber", json!([block_tag, false]))
            .await?
            .as_object()
            .cloned()
            .ok_or_else(|| RpcError::Invalid("block result is null or not an object".into()))?;
        let block_number = parse_hex_u64(result.get("number"), "block.number")?;
        let execution_block_number = result
            .get("l1BlockNumber")
            .map(|value| parse_hex_u64(Some(value), "block.l1BlockNumber"))
            .transpose()?
            .unwrap_or(block_number);
        Ok(ChainCursor {
            chain_id,
            block_number,
            execution_block_number,
            block_hash: parse_optional_hash(result.get("hash"), "block.hash")?,
            transaction_index: None,
            log_index: None,
            // Nitro exposes the EVM-visible parent-chain block in this
            // Arbitrum extension. Other networks simply omit the field.
            source_sequence: None,
            source_sub_index: None,
            commitment,
        })
    }

    /// Fetches and decodes canonical logs for an inclusive block range.
    pub async fn get_logs(
        &self,
        request: &BackfillRequest,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<Vec<ContractLog>, RpcError> {
        let mut filter = serde_json::Map::new();
        filter.insert(
            "address".into(),
            Value::String(request.filter.address.to_hex()),
        );
        filter.insert(
            "fromBlock".into(),
            Value::String(hex_u64(request.from_block)),
        );
        filter.insert("toBlock".into(), Value::String(hex_u64(request.to_block)));
        if !request.filter.topics.is_empty() {
            filter.insert(
                "topics".into(),
                Value::Array(
                    request
                        .filter
                        .topics
                        .iter()
                        .map(|topic| Value::String(word_hex(*topic)))
                        .collect(),
                ),
            );
        }
        let result = self
            .call("eth_getLogs", Value::Array(vec![Value::Object(filter)]))
            .await?;
        let logs = result
            .as_array()
            .ok_or_else(|| RpcError::Invalid("eth_getLogs result is not an array".into()))?;
        logs.iter()
            .map(|log| parse_rpc_log(log, chain_id, commitment))
            .collect()
    }
}
