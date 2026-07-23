//! Ethereum subscription requests and normalized head semantics.

use crate::rpc::client::RpcError;
use crate::rpc::codec::parse_rpc_head;
use lunarbase_client::model::{ChainCursor, Commitment, ContractFilter};
use lunarbase_math::types::B256;
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub(crate) struct WsHead {
    pub(crate) cursor: ChainCursor,
    pub(crate) parent_hash: Option<B256>,
}

pub(crate) fn subscription_request(id: u64, filter: &ContractFilter, kind: &str) -> String {
    let mut options = serde_json::Map::new();
    options.insert(
        "address".into(),
        Value::String(format!("{:#x}", filter.address)),
    );
    if !filter.topics.is_empty() {
        options.insert(
            "topics".into(),
            Value::Array(vec![Value::Array(
                filter
                    .topics
                    .iter()
                    .map(|topic| Value::String(format!("{topic:#x}")))
                    .collect(),
            )]),
        );
    }
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "eth_subscribe",
        "params": [kind, Value::Object(options)],
    })
    .to_string()
}

pub(crate) fn parse_ws_head(value: &Value, chain_id: u64) -> Result<WsHead, RpcError> {
    let head = parse_rpc_head(value)?;
    Ok(WsHead {
        cursor: ChainCursor {
            chain_id,
            block_number: head.number,
            execution_block_number: head.l1_block_number.unwrap_or(head.number),
            block_hash: head.hash,
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Realtime,
        },
        parent_hash: head.parent_hash,
    })
}

pub(crate) fn head_discontinuity(previous: &WsHead, next: &WsHead, progressive: bool) -> bool {
    let same_height = next.cursor.block_number == previous.cursor.block_number;
    let same_height_discontinuity = same_height
        && (!progressive
            || (next.parent_hash.is_some()
                && previous.parent_hash.is_some()
                && next.parent_hash != previous.parent_hash));
    next.cursor.block_number < previous.cursor.block_number
        || same_height_discontinuity
        || (next.cursor.block_number == previous.cursor.block_number.saturating_add(1)
            && next.parent_hash.is_some()
            && previous.cursor.block_hash.is_some()
            && next.parent_hash != previous.cursor.block_hash)
}
