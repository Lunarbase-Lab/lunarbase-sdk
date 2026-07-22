//! Minimal read-only Alloy HTTP client without transaction fillers.

use crate::model::{BackfillRequest, ChainCursor, Commitment, ContractLog, SourceError};
use crate::transport::rpc::codec::{parse_rpc_head, parse_rpc_log};
use alloy_primitives::{Bytes, U64, keccak256};
use alloy_rpc_client::RpcClient;
use lunarbase_math::types::Address;
use lunarbase_math::types::B256;
use serde::Serialize;
use serde_json::Value;
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
        let client = RpcClient::new_http_with_client(http, url);
        Ok(Self {
            endpoint: Arc::from(endpoint),
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
        let tag = validate_block_tag(block_tag)?;
        let value: Value = self
            .client
            .request("eth_getBlockByNumber", (tag, false))
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))?;
        let head = parse_rpc_head(&value)?;
        Ok(ChainCursor {
            chain_id,
            block_number: head.number,
            execution_block_number: head.l1_block_number.unwrap_or(head.number),
            block_hash: head.hash,
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment,
        })
    }

    /// Fetches canonical logs with topic0 OR semantics.
    ///
    /// Large ranges are split into bounded requests. Providers that still
    /// reject a dense chunk with a range/result-limit error are retried by
    /// deterministic bisection rather than forcing callers to know provider
    /// limits.
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
    pub(super) fn from_client(client: RpcClient) -> Self {
        Self {
            endpoint: Arc::from("mock://alloy"),
            client,
        }
    }
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
