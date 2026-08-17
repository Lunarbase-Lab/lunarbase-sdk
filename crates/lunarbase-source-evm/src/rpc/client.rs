//! Minimal read-only Alloy HTTP client without transaction fillers.

use crate::rpc::codec::{parse_filtered_rpc_log, parse_rpc_head, validate_canonical_hex_u64};
use crate::rpc::http::bounded_http_request;
use alloy_primitives::{Bytes, U64, keccak256};
use alloy_rpc_client::RpcClient;
use lunarbase_client::model::{
    BackfillRequest, BlockRef, ChainCursor, Commitment, ContractLog, SourceError,
};
use lunarbase_math::Address;
use lunarbase_math::B256;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    collections::VecDeque,
    fmt::Debug,
    str::FromStr,
    sync::{Arc, atomic::AtomicU64},
};
use thiserror::Error;

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Strict JSON-RPC and canonical backfill memory limits.
pub struct RpcHttpLimits {
    /// Maximum serialized JSON-RPC request body.
    pub max_request_bytes: usize,
    /// Maximum HTTP response body read before JSON deserialization.
    pub max_response_bytes: usize,
    /// Maximum inclusive block span attempted by one `eth_getLogs` request.
    pub max_backfill_page_blocks: u64,
    /// Maximum normalized logs returned by one public backfill call.
    pub max_backfill_logs: usize,
    /// Maximum normalized log payload bytes returned by one backfill call.
    pub max_backfill_bytes: usize,
}

impl Default for RpcHttpLimits {
    fn default() -> Self {
        Self {
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 8 * 1024 * 1024,
            max_backfill_page_blocks: 1_000,
            max_backfill_logs: 16_384,
            max_backfill_bytes: 32 * 1024 * 1024,
        }
    }
}

impl RpcHttpLimits {
    fn validate(self) -> Result<Self, RpcError> {
        if self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.max_backfill_page_blocks == 0
            || self.max_backfill_logs == 0
            || self.max_backfill_bytes == 0
        {
            return Err(RpcError::Invalid(
                "HTTP RPC request, response, and backfill limits must be non-zero".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Failure returned by the Alloy HTTP JSON-RPC boundary.
pub enum RpcError {
    /// The provider failed to send, receive, or execute a JSON-RPC request.
    #[error("RPC transport failed: {0}")]
    Transport(String),
    /// Local input or a provider response could not be normalized safely.
    #[error("RPC response is invalid: {0}")]
    Invalid(String),
    /// A local request, response, or normalized batch exceeded its hard budget.
    #[error("RPC resource limit exceeded: {0}")]
    Limit(String),
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
    /// Hard request/response and normalized backfill limits.
    limits: RpcHttpLimits,
    /// Whether requests use the strict bounded HTTP path instead of a test transport.
    strict_http: bool,
    /// Monotonic JSON-RPC request identity shared across clones.
    request_id: Arc<AtomicU64>,
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
            limits: RpcHttpLimits::default(),
            strict_http: true,
            request_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Replaces the default HTTP and backfill memory limits.
    pub fn with_limits(mut self, limits: RpcHttpLimits) -> Result<Self, RpcError> {
        self.limits = limits.validate()?;
        Ok(self)
    }

    /// Returns immutable HTTP and canonical backfill limits.
    pub const fn limits(&self) -> RpcHttpLimits {
        self.limits
    }

    /// Returns the configured JSON-RPC endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Reads chain ID only when explicitly requested.
    pub async fn chain_id(&self) -> Result<u64, RpcError> {
        self.request::<_, U64>("eth_chainId", Vec::<Value>::new())
            .await
            .map(|value| value.to::<u64>())
    }

    /// Executes one `eth_call` at an explicit block and returns ABI bytes.
    pub async fn call_at(
        &self,
        to: Address,
        data: Bytes,
        block_tag: &str,
    ) -> Result<Bytes, RpcError> {
        let transaction = serde_json::json!({ "to": to, "data": data });
        self.request("eth_call", (transaction, validate_block_tag(block_tag)?))
            .await
    }

    /// Executes one `eth_call` against an EIP-1898 block-hash selector.
    pub async fn call_at_hash(
        &self,
        to: Address,
        data: Bytes,
        block_hash: B256,
    ) -> Result<Bytes, RpcError> {
        let transaction = serde_json::json!({ "to": to, "data": data });
        self.request(
            "eth_call",
            (transaction, BlockHashSelector::new(block_hash)),
        )
        .await
    }

    /// Fetches contract runtime bytecode at one explicit block.
    pub async fn get_code(&self, address: Address, block_tag: &str) -> Result<Bytes, RpcError> {
        self.request("eth_getCode", (address, validate_block_tag(block_tag)?))
            .await
    }

    /// Fetches runtime bytecode against an EIP-1898 block-hash selector.
    pub async fn get_code_at_hash(
        &self,
        address: Address,
        block_hash: B256,
    ) -> Result<Bytes, RpcError> {
        self.request("eth_getCode", (address, BlockHashSelector::new(block_hash)))
            .await
    }

    /// Reads one storage word against an exact EIP-1898 block hash.
    pub async fn get_storage_at_hash(
        &self,
        address: Address,
        slot: B256,
        block_hash: B256,
    ) -> Result<B256, RpcError> {
        self.request(
            "eth_getStorageAt",
            (address, slot, BlockHashSelector::new(block_hash)),
        )
        .await
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

    /// Resolves one block cursor together with its parent linkage.
    pub async fn block_ref(
        &self,
        block_tag: &str,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<BlockRef, RpcError> {
        let tag = validate_block_tag(block_tag)?;
        let value: Value = self.request("eth_getBlockByNumber", (tag, false)).await?;
        normalize_block_ref(&value, chain_id, commitment, false)
    }

    /// Resolves block identity, parent linkage, and explicit execution context by tag.
    pub async fn block_ref_with_execution_context(
        &self,
        block_tag: &str,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<BlockRef, RpcError> {
        let tag = validate_block_tag(block_tag)?;
        let value: Value = self.request("eth_getBlockByNumber", (tag, false)).await?;
        normalize_block_ref(&value, chain_id, commitment, true)
    }

    /// Resolves block identity and parent linkage by an exact block hash.
    ///
    /// This method is intended for rare fork resolution. Normal head and log
    /// ingestion never performs a per-update HTTP lookup.
    pub async fn block_ref_by_hash(
        &self,
        block_hash: B256,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<BlockRef, RpcError> {
        self.block_ref_by_hash_inner(block_hash, chain_id, commitment, false)
            .await
    }

    /// Resolves block identity and parent linkage by hash while requiring an
    /// explicit execution context such as Nitro's `l1BlockNumber`.
    pub async fn block_ref_by_hash_with_execution_context(
        &self,
        block_hash: B256,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<BlockRef, RpcError> {
        self.block_ref_by_hash_inner(block_hash, chain_id, commitment, true)
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
        let result: Value = self.request("eth_getBlockByNumber", (tag, false)).await?;
        validate_canonical_hex_u64(result.get("number"), "block.number")?;
        validate_canonical_hex_u64(result.get("l1BlockNumber"), "block.l1BlockNumber")?;
        normalize_block_cursor(&result, chain_id, commitment, true)
    }

    /// Resolves an execution-aware cursor by exact block hash, including provisional branches.
    pub async fn block_cursor_by_hash_with_execution_context(
        &self,
        block_hash: B256,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<ChainCursor, RpcError> {
        self.block_ref_by_hash_inner(block_hash, chain_id, commitment, true)
            .await
            .map(|block| block.cursor)
    }

    async fn block_ref_by_hash_inner(
        &self,
        block_hash: B256,
        chain_id: u64,
        commitment: Commitment,
        require_execution_context: bool,
    ) -> Result<BlockRef, RpcError> {
        let hash = format!("{block_hash:#x}");
        let result: Value = self.request("eth_getBlockByHash", (&hash, false)).await?;
        if require_execution_context {
            validate_canonical_hex_u64(result.get("number"), "block.number")?;
            validate_canonical_hex_u64(result.get("l1BlockNumber"), "block.l1BlockNumber")?;
        }
        let block = normalize_block_ref(&result, chain_id, commitment, require_execution_context)?;
        if block.cursor.block_hash != Some(block_hash) {
            return Err(RpcError::Invalid("block hash response mismatch".into()));
        }
        Ok(block)
    }

    async fn block_cursor_inner(
        &self,
        block_tag: &str,
        chain_id: u64,
        commitment: Commitment,
        require_execution_context: bool,
    ) -> Result<ChainCursor, RpcError> {
        let tag = validate_block_tag(block_tag)?;
        let value: Value = self.request("eth_getBlockByNumber", (tag, false)).await?;
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
        let mut pending = initial_log_ranges(
            request.from_block,
            request.to_block,
            self.limits.max_backfill_page_blocks,
        );
        let mut normalized = Vec::new();
        let mut normalized_bytes = 0_usize;
        while let Some((from_block, to_block)) = pending.pop_front() {
            let chunk = BackfillRequest {
                from_block,
                to_block,
                filter: request.filter.clone(),
            };
            let filter = backfill_filter(&chunk);
            let response: Result<Vec<Value>, RpcError> =
                self.request("eth_getLogs", (filter,)).await;
            match response {
                Ok(logs) => {
                    for value in logs {
                        let log =
                            parse_filtered_rpc_log(&value, chain_id, commitment, &request.filter)?;
                        let bytes = log.retained_bytes();
                        if normalized.len() >= self.limits.max_backfill_logs
                            || bytes
                                > self
                                    .limits
                                    .max_backfill_bytes
                                    .saturating_sub(normalized_bytes)
                        {
                            return Err(RpcError::Limit(
                                "normalized backfill batch count or byte budget exceeded".into(),
                            ));
                        }
                        normalized_bytes += bytes;
                        normalized.push(log);
                    }
                }
                Err(error) if from_block < to_block && is_bisectable_log_range_limit(&error) => {
                    let middle = from_block + (to_block - from_block) / 2;
                    pending.push_front((middle.saturating_add(1), to_block));
                    pending.push_front((from_block, middle));
                }
                Err(error) => return Err(error),
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
            limits: RpcHttpLimits::default(),
            strict_http: false,
            request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    async fn request<Params, Response>(
        &self,
        method: &'static str,
        params: Params,
    ) -> Result<Response, RpcError>
    where
        Params: Serialize + Clone + Debug + Send + Sync + Unpin,
        Response: DeserializeOwned + Debug + Send + Sync + Unpin + 'static,
    {
        if !self.strict_http {
            return self
                .client
                .request(method, params)
                .await
                .map_err(|error| RpcError::Transport(error.to_string()));
        }
        bounded_http_request(
            &self.http,
            self.endpoint(),
            &self.request_id,
            self.limits,
            method,
            params,
        )
        .await
    }
}

fn normalize_block_cursor(
    value: &Value,
    chain_id: u64,
    commitment: Commitment,
    require_execution_context: bool,
) -> Result<ChainCursor, RpcError> {
    normalize_block_ref(value, chain_id, commitment, require_execution_context)
        .map(|block| block.cursor)
}

fn normalize_block_ref(
    value: &Value,
    chain_id: u64,
    commitment: Commitment,
    require_execution_context: bool,
) -> Result<BlockRef, RpcError> {
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
    Ok(BlockRef {
        cursor: ChainCursor {
            chain_id,
            block_number: head.number,
            execution_block_number,
            block_hash: head.hash,
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment,
        },
        parent_hash: head.parent_hash,
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

fn initial_log_ranges(
    from_block: u64,
    to_block: u64,
    max_page_blocks: u64,
) -> VecDeque<(u64, u64)> {
    let mut ranges = VecDeque::new();
    let mut start = from_block;
    loop {
        let end = start
            .saturating_add(max_page_blocks.saturating_sub(1))
            .min(to_block);
        ranges.push_back((start, end));
        if end == to_block {
            break;
        }
        start = end.saturating_add(1);
    }
    ranges
}

fn is_bisectable_log_range_limit(error: &RpcError) -> bool {
    if let RpcError::Limit(message) = error {
        let message = message.to_ascii_lowercase();
        return message.contains("http content-length") || message.contains("http response body");
    }
    let message = error.to_string().to_ascii_lowercase();
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
