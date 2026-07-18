use super::RpcError;
use crate::{ChainCursor, Commitment, ContractLog, SourceError};
use alloy_primitives::keccak256 as alloy_keccak256;
use lunarbase_math::{Address, B256, U256};
use serde_json::Value;

/// Decodes one standard Ethereum JSON-RPC log into the normalized model.
pub fn parse_rpc_log(
    value: &Value,
    chain_id: u64,
    commitment: Commitment,
) -> Result<ContractLog, RpcError> {
    let object = value
        .as_object()
        .ok_or_else(|| RpcError::Invalid("eth_getLogs entry is not an object".into()))?;
    let topics = object
        .get("topics")
        .and_then(Value::as_array)
        .ok_or_else(|| RpcError::Invalid("log topics are missing".into()))?
        .iter()
        .map(|value| {
            let value = value
                .as_str()
                .ok_or_else(|| RpcError::Invalid("log topic is not a string".into()))?;
            parse_hash(value, "log.topic")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let address = object
        .get("address")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::Invalid("log address is missing".into()))?
        .parse::<Address>()
        .map_err(|error| RpcError::Invalid(error.to_string()))?;
    let data = object
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::Invalid("log data is missing".into()))?;
    let block_number = parse_hex_u64(object.get("blockNumber"), "log.blockNumber")?;
    Ok(ContractLog {
        address,
        topics,
        data: parse_hex_bytes(data)?.into(),
        removed: object
            .get("removed")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        cursor: ChainCursor {
            chain_id,
            block_number,
            execution_block_number: block_number,
            block_hash: parse_optional_hash(object.get("blockHash"), "log.blockHash")?,
            transaction_index: Some(checked_u32(
                U256::from(parse_hex_u64(
                    object.get("transactionIndex"),
                    "log.transactionIndex",
                )?),
                "log.transactionIndex",
            )?),
            log_index: Some(checked_u32(
                U256::from(parse_hex_u64(object.get("logIndex"), "log.logIndex")?),
                "log.logIndex",
            )?),
            source_sequence: None,
            source_sub_index: None,
            commitment,
        },
    })
}

pub(super) fn selector_address(selector: &str, address: Address) -> String {
    format!(
        "{selector}{}{}",
        "0".repeat(24),
        hex_encode(address.as_slice())
    )
}

pub(super) fn selector_two_addresses(selector: &str, first: Address, second: Address) -> String {
    format!(
        "{selector}{}{}{}{}",
        "0".repeat(24),
        hex_encode(first.as_slice()),
        "0".repeat(24),
        hex_encode(second.as_slice())
    )
}

pub(super) fn parse_hex_bytes(value: &str) -> Result<Vec<u8>, RpcError> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        return Err(RpcError::Invalid("hex string has odd length".into()));
    }
    (0..value.len() / 2)
        .map(|index| {
            u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                .map_err(|_| RpcError::Invalid("invalid hex string".into()))
        })
        .collect()
}

pub(super) fn parse_hex_u64(value: Option<&Value>, field: &str) -> Result<u64, RpcError> {
    let value = value
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::Invalid(format!("{field} is not a hex string")))?;
    let value = value.strip_prefix("0x").unwrap_or(value);
    u64::from_str_radix(value, 16).map_err(|_| RpcError::Invalid(format!("{field} is invalid")))
}

pub(super) fn parse_optional_hash(
    value: Option<&Value>,
    field: &str,
) -> Result<Option<B256>, RpcError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let value = value
                .as_str()
                .ok_or_else(|| RpcError::Invalid(format!("{field} is not a string")))?;
            parse_hash(value, field).map(Some)
        }
    }
}

pub(super) fn parse_hash(value: &str, field: &str) -> Result<B256, RpcError> {
    let bytes = parse_hex_bytes(value)?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| RpcError::Invalid(format!("{field} is not 32 bytes")))?;
    Ok(B256::new(bytes))
}

pub(super) fn decode_words(value: &str, expected: usize) -> Result<Vec<U256>, RpcError> {
    let bytes = parse_hex_bytes(value)?;
    if bytes.len() != expected * 32 {
        return Err(RpcError::Invalid(format!(
            "expected {expected} ABI words, got {} bytes",
            bytes.len()
        )));
    }
    Ok(bytes
        .chunks_exact(32)
        .map(|chunk| U256::from_be_bytes::<32>(chunk.try_into().expect("exact ABI word")))
        .collect())
}

pub(super) fn decode_word(value: &str, index: usize) -> Result<U256, RpcError> {
    decode_words(value, index + 1)?
        .get(index)
        .copied()
        .ok_or_else(|| RpcError::Invalid("missing ABI word".into()))
}

pub(super) fn decode_address_word(value: &str) -> Result<Address, RpcError> {
    let word = decode_word(value, 0)?.to_be_bytes::<32>();
    if word[..12].iter().any(|byte| *byte != 0) {
        return Err(RpcError::Invalid("ABI address word is not padded".into()));
    }
    Ok(Address::from_slice(&word[12..]))
}

pub(super) fn decode_bool(value: U256) -> Result<bool, SourceError> {
    match value {
        U256::ZERO => Ok(false),
        U256::ONE => Ok(true),
        _ => Err(SourceError::Unavailable("ABI boolean is not 0 or 1".into())),
    }
}

pub(super) fn checked_u32(value: U256, field: &str) -> Result<u32, RpcError> {
    u32::try_from(value).map_err(|_| RpcError::Invalid(format!("{field} does not fit u32")))
}

pub(super) fn checked_u128(value: U256, field: &str) -> Result<U256, SourceError> {
    if value > lunarbase_math::U128_MAX {
        return Err(SourceError::Unavailable(format!(
            "{field} does not fit uint128"
        )));
    }
    Ok(value)
}

pub(super) fn hex_u64(value: u64) -> String {
    format!("0x{value:x}")
}

pub(super) fn word_hex(value: B256) -> String {
    format!("{value:#x}")
}

pub(super) fn hex_encode(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push_str(&format!("{byte:02x}"));
    }
    result
}

pub(super) fn keccak256(bytes: &[u8]) -> B256 {
    alloy_keccak256(bytes)
}
