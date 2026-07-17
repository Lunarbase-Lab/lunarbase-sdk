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
            decode_word(value, 0)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let address = Address::from_hex(
        object
            .get("address")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::Invalid("log address is missing".into()))?,
    )
    .map_err(|error| RpcError::Invalid(error.to_string()))?;
    let data = object
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::Invalid("log data is missing".into()))?;
    Ok(ContractLog {
        address,
        topics,
        data: parse_hex_bytes(data)?,
        removed: object
            .get("removed")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        cursor: ChainCursor {
            chain_id,
            block_number: parse_hex_u64(object.get("blockNumber"), "log.blockNumber")?,
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

fn selector_address(selector: &str, address: Address) -> String {
    format!("{selector}{}{}", "0".repeat(24), &address.to_hex()[2..])
}

fn selector_two_addresses(selector: &str, first: Address, second: Address) -> String {
    format!(
        "{selector}{}{}{}{}",
        "0".repeat(24),
        &first.to_hex()[2..],
        "0".repeat(24),
        &second.to_hex()[2..]
    )
}

fn parse_hex_bytes(value: &str) -> Result<Vec<u8>, RpcError> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() % 2 != 0 {
        return Err(RpcError::Invalid("hex string has odd length".into()));
    }
    (0..value.len() / 2)
        .map(|index| {
            u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                .map_err(|_| RpcError::Invalid("invalid hex string".into()))
        })
        .collect()
}

fn parse_hex_u64(value: Option<&Value>, field: &str) -> Result<u64, RpcError> {
    let value = value
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::Invalid(format!("{field} is not a hex string")))?;
    let value = value.strip_prefix("0x").unwrap_or(value);
    u64::from_str_radix(value, 16).map_err(|_| RpcError::Invalid(format!("{field} is invalid")))
}

fn parse_optional_hash(value: Option<&Value>, field: &str) -> Result<Option<[u8; 32]>, RpcError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let value = value
                .as_str()
                .ok_or_else(|| RpcError::Invalid(format!("{field} is not a string")))?;
            let bytes = parse_hex_bytes(value)?;
            Ok(Some(bytes.try_into().map_err(|_| {
                RpcError::Invalid(format!("{field} is not 32 bytes"))
            })?))
        }
    }
}

fn decode_words(value: &str, expected: usize) -> Result<Vec<U256>, RpcError> {
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

fn decode_word(value: &str, index: usize) -> Result<U256, RpcError> {
    decode_words(value, index + 1)?
        .get(index)
        .copied()
        .ok_or_else(|| RpcError::Invalid("missing ABI word".into()))
}

fn decode_address_word(value: &str) -> Result<Address, RpcError> {
    let word = decode_word(value, 0)?.to_be_bytes::<32>();
    if word[..12].iter().any(|byte| *byte != 0) {
        return Err(RpcError::Invalid("ABI address word is not padded".into()));
    }
    Ok(Address(word[12..].try_into().expect("20-byte address")))
}

fn decode_bool(value: U256) -> Result<bool, SourceError> {
    match value {
        U256::ZERO => Ok(false),
        U256::ONE => Ok(true),
        _ => Err(SourceError::Unavailable("ABI boolean is not 0 or 1".into())),
    }
}

fn checked_u32(value: U256, field: &str) -> Result<u32, RpcError> {
    u32::try_from(value).map_err(|_| RpcError::Invalid(format!("{field} does not fit u32")))
}

fn checked_u128(value: U256, field: &str) -> Result<U256, SourceError> {
    if value > lunarbase_math::U128_MAX {
        return Err(SourceError::Unavailable(format!(
            "{field} does not fit uint128"
        )));
    }
    Ok(value)
}

fn hex_u64(value: u64) -> String {
    format!("0x{value:x}")
}

fn word_hex(value: U256) -> String {
    format!("0x{}", hex_encode(&value.to_be_bytes::<32>()))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push_str(&format!("{byte:02x}"));
    }
    result
}

fn keccak256(bytes: &[u8]) -> [u8; 32] {
    let mut output = [0u8; 32];
    let mut hasher = Keccak::v256();
    hasher.update(bytes);
    hasher.finalize(&mut output);
    output
}

