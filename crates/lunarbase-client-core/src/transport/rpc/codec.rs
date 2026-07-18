use super::RpcError;
use crate::{ChainCursor, Commitment, ContractLog};
use alloy_rpc_types_eth::Log;
use lunarbase_math::B256;
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct ParsedRpcHead {
    pub number: u64,
    pub hash: Option<B256>,
    pub parent_hash: Option<B256>,
    pub l1_block_number: Option<u64>,
}

/// Decodes one standard Ethereum JSON-RPC log with Alloy's serde model.
pub fn parse_rpc_log(
    value: &Value,
    chain_id: u64,
    commitment: Commitment,
) -> Result<ContractLog, RpcError> {
    let log = serde_json::from_value::<Log>(value.clone())
        .map_err(|error| RpcError::Invalid(format!("invalid RPC log: {error}")))?;
    normalize_rpc_log(log, chain_id, commitment)
}

pub(super) fn normalize_rpc_log(
    log: Log,
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
        address: log.address(),
        topics: log.topics().to_vec(),
        data: log.data().data.clone(),
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
