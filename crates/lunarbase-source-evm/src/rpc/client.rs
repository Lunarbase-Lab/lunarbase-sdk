//! Minimal read-only Alloy HTTP client without transaction fillers.

use crate::rpc::codec::{parse_rpc_head, parse_rpc_log, validate_canonical_hex_u64};
use alloy_primitives::{Bytes, U64, keccak256};
use alloy_rpc_client::RpcClient;
use lunarbase_client::model::{BackfillRequest, ChainCursor, Commitment, ContractLog, SourceError};
use lunarbase_math::Address;
use lunarbase_math::B256;
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::VecDeque, str::FromStr, sync::Arc};
use thiserror::Error;

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
const LOG_RANGE_CHUNK_BLOCKS: u64 = 10_000;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Failure returned by the Alloy HTTP JSON-RPC boundary.
pub enum RpcError {
    /// The provider failed to send, receive, or execute a JSON-RPC request.
    #[error("RPC transport failed: {0}")]
    Transport(String),
    /// Local input or a provider response could not be normalized safely.
    #[error("RPC response is invalid: {0}")]
    Invalid(String),
}

impl From<RpcError> for SourceError {
    fn from(error: RpcError) -> Self {
        SourceError::Unavailable(error.to_string())
    }
}

#[derive(Clone)]
/// Read-only Alloy provider with no transaction fillers or retry layers.
pub struct RpcHttpClient {
    /// Original validated endpoint retained for diagnostics and configuration views.
    endpoint: Arc<str>,
    /// Bounded HTTP client retained for strict JSON-RPC envelope validation.
    http: reqwest::Client,
    /// Low-level Alloy RPC client without consensus, signing, or transaction fillers.
    client: RpcClient,
}

impl RpcHttpClient {
    /// Creates a read-only HTTP provider after validating the endpoint URL.
    ///
    /// The low-level RPC client deliberately omits provider, consensus, signing,
    /// gas, nonce, and chain-id transaction layers.
    pub fn new(endpoint: impl Into<String>) -> Result<Self, RpcError> {
        let endpoint = endpoint.into();
        let url = url::Url::parse(&endpoint)
            .map_err(|error| RpcError::Invalid(format!("invalid HTTP RPC URL: {error}")))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(RpcError::Invalid(
                "HTTP RPC URL must use http or https".into(),
            ));
        }
        let http = reqwest::Client::builder()
            .timeout(RPC_TIMEOUT)
            .build()
            .map_err(|error| RpcError::Invalid(format!("build HTTP RPC client: {error}")))?;
        let client = RpcClient::new_http_with_client(http.clone(), url);
        Ok(Self {
            endpoint: Arc::from(endpoint),
            http,
            client,
        })
    }

    /// Returns the configured JSON-RPC endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Reads chain ID only when explicitly requested.
    pub async fn chain_id(&self) -> Result<u64, RpcError> {
        self.client
            .request_noparams::<U64>("eth_chainId")
            .await
            .map(|value| value.to::<u64>())
            .map_err(|error| RpcError::Transport(error.to_string()))
    }

    /// Executes one `eth_call` at an explicit block and returns ABI bytes.
    pub async fn call_at(
        &self,
        to: Address,
        data: Bytes,
        block_tag: &str,
    ) -> Result<Bytes, RpcError> {
        let transaction = serde_json::json!({ "to": to, "data": data });
        self.client
            .request("eth_call", (transaction, validate_block_tag(block_tag)?))
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))
    }

    /// Executes one `eth_call` against an EIP-1898 block-hash selector.
    pub async fn call_at_hash(
        &self,
        to: Address,
        data: Bytes,
        block_hash: B256,
    ) -> Result<Bytes, RpcError> {
        let transaction = serde_json::json!({ "to": to, "data": data });
        self.client
            .request(
                "eth_call",
                (transaction, BlockHashSelector::new(block_hash)),
            )
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))
    }

    /// Fetches contract runtime bytecode at one explicit block.
    pub async fn get_code(&self, address: Address, block_tag: &str) -> Result<Bytes, RpcError> {
        self.client
            .request("eth_getCode", (address, validate_block_tag(block_tag)?))
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))
    }

    /// Fetches runtime bytecode against an EIP-1898 block-hash selector.
    pub async fn get_code_at_hash(
        &self,
        address: Address,
        block_hash: B256,
    ) -> Result<Bytes, RpcError> {
        self.client
            .request("eth_getCode", (address, BlockHashSelector::new(block_hash)))
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))
    }

    /// Reads one storage word against an exact EIP-1898 block hash.
    pub async fn get_storage_at_hash(
        &self,
        address: Address,
        slot: B256,
        block_hash: B256,
    ) -> Result<B256, RpcError> {
        self.client
            .request(
                "eth_getStorageAt",
                (address, slot, BlockHashSelector::new(block_hash)),
            )
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))
    }

    /// Reads and hashes runtime bytecode through Alloy's Keccak-256 primitive.
    pub async fn runtime_code_hash(
        &self,
        address: Address,
        block_tag: &str,
    ) -> Result<B256, RpcError> {
        self.get_code(address, block_tag)
            .await
            .map(|code| keccak256(&code))
    }

    /// Reads and hashes runtime bytecode at one exact EIP-1898 block hash.
    pub async fn runtime_code_hash_at_hash(
        &self,
        address: Address,
        block_hash: B256,
    ) -> Result<B256, RpcError> {
        self.get_code_at_hash(address, block_hash)
            .await
            .map(|code| keccak256(&code))
    }

    /// Converts one explicit block request into the normalized snapshot cursor.
    pub async fn block_cursor(
        &self,
        block_tag: &str,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<ChainCursor, RpcError> {
        self.block_cursor_inner(block_tag, chain_id, commitment, false)
            .await
    }

    /// Resolves a block cursor only from an exact response with explicit execution context.
    pub async fn block_cursor_with_execution_context(
        &self,
        block_tag: &str,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<ChainCursor, RpcError> {
        let tag = validate_block_tag(block_tag)?;
        let request_id = format!("execution-context:{tag}");
        let response = self
            .http
            .post(self.endpoint())
            .json(&json!({
                "jsonrpc": "2.0",
                "id": &request_id,
                "method": "eth_getBlockByNumber",
                "params": [tag, false],
            }))
            .send()
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))?
            .error_for_status()
            .map_err(|error| RpcError::Transport(error.to_string()))?
            .json::<Value>()
            .await
            .map_err(|error| RpcError::Invalid(format!("invalid JSON-RPC response: {error}")))?;
        if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(RpcError::Invalid(
                "JSON-RPC response version is not 2.0".into(),
            ));
        }
        if response.get("id").and_then(Value::as_str) != Some(request_id.as_str()) {
            return Err(RpcError::Invalid("JSON-RPC response id mismatch".into()));
        }
        if let Some(error) = response.get("error").filter(|error| !error.is_null()) {
            return Err(RpcError::Transport(format!(
                "JSON-RPC returned an error: {error}"
            )));
        }
        let result = response
            .get("result")
            .ok_or_else(|| RpcError::Invalid("JSON-RPC response has no result".into()))?;
        validate_canonical_hex_u64(result.get("number"), "block.number")?;
        validate_canonical_hex_u64(result.get("l1BlockNumber"), "block.l1BlockNumber")?;
        normalize_block_cursor(result, chain_id, commitment, true)
    }

    async fn block_cursor_inner(
        &self,
        block_tag: &str,
        chain_id: u64,
        commitment: Commitment,
        require_execution_context: bool,
    ) -> Result<ChainCursor, RpcError> {
        let tag = validate_block_tag(block_tag)?;
        let value: Value = self
            .client
            .request("eth_getBlockByNumber", (tag, false))
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))?;
        normalize_block_cursor(&value, chain_id, commitment, require_execution_context)
    }

    /// Fetches canonical logs with topic0 OR semantics and bounded range splitting.
    pub async fn get_logs(
        &self,
        request: &BackfillRequest,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<Vec<ContractLog>, RpcError> {
        if request.from_block > request.to_block {
            return Err(RpcError::Invalid("log range starts after its end".into()));
        }
        let mut pending = initial_log_ranges(request.from_block, request.to_block);
        let mut normalized = Vec::new();
        while let Some((from_block, to_block)) = pending.pop_front() {
            let chunk = BackfillRequest {
                from_block,
                to_block,
                filter: request.filter.clone(),
            };
            let filter = backfill_filter(&chunk);
            let response: Result<Vec<Value>, _> =
                self.client.request("eth_getLogs", (filter,)).await;
            match response {
                Ok(logs) => {
                    let logs = logs
                        .into_iter()
                        .map(|log| parse_rpc_log(&log, chain_id, commitment))
                        .collect::<Result<Vec<_>, _>>()?;
                    normalized.extend(logs);
                }
                Err(error) if from_block < to_block && is_log_range_limit(&error.to_string()) => {
                    let middle = from_block + (to_block - from_block) / 2;
                    pending.push_front((middle.saturating_add(1), to_block));
                    pending.push_front((from_block, middle));
                }
                Err(error) => return Err(RpcError::Transport(error.to_string())),
            }
        }
        normalized.sort_by_key(|log| log.cursor.event_order());
        Ok(normalized)
    }

    #[cfg(test)]
    pub(crate) fn from_client(client: RpcClient) -> Self {
        Self {
            endpoint: Arc::from("mock://alloy"),
            http: reqwest::Client::new(),
            client,
        }
    }
}

fn normalize_block_cursor(
    value: &Value,
    chain_id: u64,
    commitment: Commitment,
    require_execution_context: bool,
) -> Result<ChainCursor, RpcError> {
    let head = parse_rpc_head(value)?;
    let execution_block_number = match head.l1_block_number {
        Some(block_number) => block_number,
        None if require_execution_context => {
            return Err(RpcError::Invalid(
                "eth_getBlockByNumber result has no l1BlockNumber".into(),
            ));
        }
        None => head.number,
    };
    Ok(ChainCursor {
        chain_id,
        block_number: head.number,
        execution_block_number,
        block_hash: head.hash,
        transaction_index: None,
        log_index: None,
        source_sequence: None,
        source_sub_index: None,
        commitment,
    })
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockHashSelector {
    block_hash: B256,
}

impl BlockHashSelector {
    const fn new(block_hash: B256) -> Self {
        Self { block_hash }
    }
}

fn initial_log_ranges(from_block: u64, to_block: u64) -> VecDeque<(u64, u64)> {
    let mut ranges = VecDeque::new();
    let mut start = from_block;
    loop {
        let end = start
            .saturating_add(LOG_RANGE_CHUNK_BLOCKS.saturating_sub(1))
            .min(to_block);
        ranges.push_back((start, end));
        if end == to_block {
            break;
        }
        start = end.saturating_add(1);
    }
    ranges
}

fn is_log_range_limit(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "too many results",
        "response size",
        "block range",
        "query exceeds",
        "limit exceeded",
        "-32005",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

pub(super) fn backfill_filter(request: &BackfillRequest) -> Value {
    let mut filter = serde_json::json!({
        "address": request.filter.address,
        "fromBlock": format!("0x{:x}", request.from_block),
        "toBlock": format!("0x{:x}", request.to_block),
    });
    if !request.filter.topics.is_empty()
        && let Some(object) = filter.as_object_mut()
    {
        object.insert("topics".into(), serde_json::json!([request.filter.topics]));
    }
    filter
}

fn validate_block_tag(value: &str) -> Result<&str, RpcError> {
    if matches!(
        value,
        "earliest" | "finalized" | "latest" | "pending" | "safe"
    ) {
        return Ok(value);
    }
    if value.starts_with("0x") && U64::from_str(value).is_ok() {
        return Ok(value);
    }
    Err(RpcError::Invalid(format!("invalid block tag: {value}")))
}
