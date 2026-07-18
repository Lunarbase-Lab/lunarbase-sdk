use super::codec::{normalize_rpc_log, parse_rpc_head};
use crate::{BackfillRequest, ChainCursor, Commitment, ContractLog, SourceError};
use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::Bytes;
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types_eth::{Filter, TransactionRequest};
use lunarbase_math::Address;
use serde_json::Value;
use std::{str::FromStr, sync::Arc};
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Failure returned by the Alloy HTTP JSON-RPC boundary.
pub enum RpcError {
    #[error("RPC transport failed: {0}")]
    Transport(String),
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
    endpoint: Arc<str>,
    provider: RootProvider,
}

impl RpcHttpClient {
    /// Creates a read-only HTTP provider after validating the endpoint URL.
    ///
    /// [`ProviderBuilder::default`] is intentional: unlike `new()`, it installs
    /// no gas, nonce, or chain-id transaction fillers.
    pub fn new(endpoint: impl Into<String>) -> Result<Self, RpcError> {
        let endpoint = endpoint.into();
        let url = url::Url::parse(&endpoint)
            .map_err(|error| RpcError::Invalid(format!("invalid HTTP RPC URL: {error}")))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(RpcError::Invalid(
                "HTTP RPC URL must use http or https".into(),
            ));
        }
        let provider = ProviderBuilder::default().connect_http(url);
        Ok(Self {
            endpoint: Arc::from(endpoint),
            provider,
        })
    }

    /// Returns the configured JSON-RPC endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Reads chain ID only when explicitly requested.
    pub async fn chain_id(&self) -> Result<u64, RpcError> {
        self.provider
            .get_chain_id()
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))
    }

    /// Executes one `eth_call` at an explicit block and returns ABI bytes.
    pub async fn call_at(
        &self,
        to: Address,
        data: Bytes,
        block_tag: &str,
    ) -> Result<Bytes, RpcError> {
        let transaction = TransactionRequest::default().to(to).input(data.into());
        self.provider
            .call(transaction)
            .block(parse_block_id(block_tag)?)
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))
    }

    /// Fetches contract runtime bytecode at one explicit block.
    pub async fn get_code(&self, address: Address, block_tag: &str) -> Result<Bytes, RpcError> {
        self.provider
            .get_code_at(address)
            .block_id(parse_block_id(block_tag)?)
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))
    }

    /// Converts one explicit block request into the normalized snapshot cursor.
    pub async fn block_cursor(
        &self,
        block_tag: &str,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<ChainCursor, RpcError> {
        let tag = parse_block_number_or_tag(block_tag)?;
        let value: Value = self
            .provider
            .client()
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
    pub async fn get_logs(
        &self,
        request: &BackfillRequest,
        chain_id: u64,
        commitment: Commitment,
    ) -> Result<Vec<ContractLog>, RpcError> {
        let filter = backfill_filter(request);
        self.provider
            .get_logs(&filter)
            .await
            .map_err(|error| RpcError::Transport(error.to_string()))?
            .into_iter()
            .map(|log| normalize_rpc_log(log, chain_id, commitment))
            .collect()
    }

    #[cfg(test)]
    pub(super) fn from_provider(provider: RootProvider) -> Self {
        Self {
            endpoint: Arc::from("mock://alloy"),
            provider,
        }
    }
}

pub(super) fn backfill_filter(request: &BackfillRequest) -> Filter {
    let filter = Filter::new()
        .address(request.filter.address)
        .from_block(request.from_block)
        .to_block(request.to_block);
    if request.filter.topics.is_empty() {
        filter
    } else {
        filter.event_signature(request.filter.topics.clone())
    }
}

fn parse_block_number_or_tag(value: &str) -> Result<BlockNumberOrTag, RpcError> {
    BlockNumberOrTag::from_str(value)
        .map_err(|error| RpcError::Invalid(format!("invalid block tag: {error}")))
}

fn parse_block_id(value: &str) -> Result<BlockId, RpcError> {
    parse_block_number_or_tag(value).map(Into::into)
}
