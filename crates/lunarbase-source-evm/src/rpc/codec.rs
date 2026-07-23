//! Alloy-backed normalization of Ethereum JSON-RPC heads and logs.

use crate::rpc::client::RpcError;
use lunarbase_client::model::{ChainCursor, Commitment, ContractLog};
use lunarbase_math::types::{Address, B256, Bytes};
use serde::Deserialize;
use serde_json::Value;

/// Block header fields required by the common cursor and Nitro extension.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcHead {
    #[serde(with = "alloy_serde::quantity")]
    number: u64,
    #[serde(default)]
    hash: Option<B256>,
    #[serde(default)]
    parent_hash: Option<B256>,
    #[serde(default, with = "alloy_serde::quantity::opt")]
    l1_block_number: Option<u64>,
}

/// Minimal standard JSON-RPC log DTO backed entirely by Alloy EVM primitives.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
    #[serde(default, with = "alloy_serde::quantity::opt")]
    block_number: Option<u64>,
    #[serde(default)]
    block_hash: Option<B256>,
    #[serde(default, with = "alloy_serde::quantity::opt")]
    transaction_index: Option<u64>,
    #[serde(default, with = "alloy_serde::quantity::opt")]
    log_index: Option<u64>,
    #[serde(default)]
    removed: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ParsedRpcHead {
    pub number: u64,
    pub hash: Option<B256>,
    pub parent_hash: Option<B256>,
    pub l1_block_number: Option<u64>,
}

/// Decodes one standard Ethereum JSON-RPC log into Alloy primitives.
pub fn parse_rpc_log(
    value: &Value,
    chain_id: u64,
    commitment: Commitment,
) -> Result<ContractLog, RpcError> {
    let log = serde_json::from_value::<RpcLog>(value.clone())
        .map_err(|error| RpcError::Invalid(format!("invalid RPC log: {error}")))?;
    normalize_rpc_log(log, chain_id, commitment)
}

fn normalize_rpc_log(
    log: RpcLog,
    chain_id: u64,
    commitment: Commitment,
) -> Result<ContractLog, RpcError> {
    let block_number = log
        .block_number
        .ok_or_else(|| RpcError::Invalid("pending log has no block number".into()))?;
    let transaction_index = log
        .transaction_index
        .ok_or_else(|| RpcError::Invalid("pending log has no transaction index".into()))?
        .try_into()
        .map_err(|_| RpcError::Invalid("log transaction index exceeds uint32".into()))?;
    let log_index = log
        .log_index
        .ok_or_else(|| RpcError::Invalid("pending log has no log index".into()))?
        .try_into()
        .map_err(|_| RpcError::Invalid("log index exceeds uint32".into()))?;
    Ok(ContractLog {
        address: log.address,
        topics: log.topics,
        data: log.data,
        removed: log.removed,
        cursor: ChainCursor {
            chain_id,
            block_number,
            execution_block_number: block_number,
            block_hash: log.block_hash,
            transaction_index: Some(transaction_index),
            log_index: Some(log_index),
            source_sequence: None,
            source_sub_index: None,
            commitment,
        },
    })
}

pub(crate) fn parse_rpc_head(value: &Value) -> Result<ParsedRpcHead, RpcError> {
    let head = serde_json::from_value::<RpcHead>(value.clone())
        .map_err(|error| RpcError::Invalid(format!("invalid RPC block header: {error}")))?;
    Ok(ParsedRpcHead {
        number: head.number,
        hash: head.hash,
        parent_hash: head.parent_hash,
        l1_block_number: head.l1_block_number,
    })
}
