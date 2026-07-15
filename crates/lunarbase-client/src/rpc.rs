use crate::abi::{lane_discovery_topics, TOPIC_LANE_ADDED, TOPIC_LANE_REMOVED};
use crate::sources::{NormalizedBackend, SourceStream};
use crate::{
    BackfillRequest, BootstrapSnapshot, ChainCursor, Commitment, ContractLog, DeploymentConfig,
    Network, SnapshotProvider, SourceError,
};
use async_trait::async_trait;
use lunarbase_math::{Address, LaneState, QuoteState, U256};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use thiserror::Error;
use tiny_keccak::{Hasher, Keccak};

const SELECTOR_CASH: &str = "0x961be391";
const SELECTOR_LANE: &str = "0xd1bacd10";
const SELECTOR_RESERVES: &str = "0xd66bd524";
const SELECTOR_WHITELIST: &str = "0x9b19251a";
const SELECTOR_BLACKLIST_FEE_MULTIPLIER: &str = "0x93b6ab27";
const SELECTOR_PARTNERS: &str = "0xaa5f434c";

#[derive(Clone, Debug, Error, Eq, PartialEq)]
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
pub struct RpcHttpClient {
    endpoint: Arc<str>,
    client: Client,
    next_id: Arc<AtomicU64>,
}

impl RpcHttpClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: Arc::from(endpoint.into()),
            client: Client::new(),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

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
        Ok(ChainCursor {
            chain_id,
            block_number: parse_hex_u64(result.get("number"), "block.number")?,
            block_hash: parse_optional_hash(result.get("hash"), "block.hash")?,
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment,
        })
    }

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

#[derive(Clone)]
pub struct RpcHttpBackend {
    rpc: RpcHttpClient,
    network: Network,
    chain_id: u64,
    snapshot_tag: Arc<str>,
}

impl RpcHttpBackend {
    pub fn new(
        rpc: RpcHttpClient,
        network: Network,
        chain_id: u64,
        snapshot_tag: impl Into<String>,
    ) -> Self {
        Self {
            rpc,
            network,
            chain_id,
            snapshot_tag: Arc::from(snapshot_tag.into()),
        }
    }

    pub fn rpc(&self) -> &RpcHttpClient {
        &self.rpc
    }

    pub fn network(&self) -> Network {
        self.network
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }
}

#[async_trait]
impl NormalizedBackend for RpcHttpBackend {
    async fn snapshot_cursor(&self, network: Network) -> Result<ChainCursor, SourceError> {
        if network != self.network {
            return Err(SourceError::NetworkMismatch);
        }
        let commitment = if self.snapshot_tag.as_ref() == "finalized" {
            Commitment::Finalized
        } else {
            Commitment::Canonical
        };
        self.rpc
            .block_cursor(&self.snapshot_tag, self.chain_id, commitment)
            .await
            .map_err(Into::into)
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.rpc
            .get_logs(&request, self.chain_id, Commitment::Canonical)
            .await
            .map_err(Into::into)
    }

    async fn subscribe(
        &self,
        _network: Network,
        _filter: crate::ContractFilter,
    ) -> Result<SourceStream, SourceError> {
        Err(SourceError::Unavailable(
            "HTTP RPC backend has no realtime subscription; use a network source or WebSocket backend".into(),
        ))
    }
}

#[derive(Clone)]
pub struct RpcSnapshotProvider {
    rpc: RpcHttpClient,
    snapshot_tag: Arc<str>,
}

impl RpcSnapshotProvider {
    pub fn new(rpc: RpcHttpClient, snapshot_tag: impl Into<String>) -> Self {
        Self {
            rpc,
            snapshot_tag: Arc::from(snapshot_tag.into()),
        }
    }

    pub fn rpc(&self) -> &RpcHttpClient {
        &self.rpc
    }
}

#[async_trait]
impl SnapshotProvider for RpcSnapshotProvider {
    async fn snapshot(
        &self,
        config: &DeploymentConfig,
        lane_assets: &[Address],
        routers: &[Address],
    ) -> Result<BootstrapSnapshot, SourceError> {
        config.validate()?;
        let commitment = if self.snapshot_tag.as_ref() == "finalized" {
            Commitment::Finalized
        } else {
            Commitment::Canonical
        };
        let cursor = self
            .rpc
            .block_cursor(&self.snapshot_tag, config.chain_id, commitment)
            .await?;
        if cursor.block_number < config.deployment_block {
            return Err(SourceError::Unavailable(
                "snapshot block precedes deployment block".into(),
            ));
        }
        let code = self.rpc.get_code(config.core, &self.snapshot_tag).await?;
        let runtime_code_hash = keccak256(&code);
        if config.expected_runtime_code_hash != [0; 32]
            && runtime_code_hash != config.expected_runtime_code_hash
        {
            return Err(SourceError::Unavailable(
                "runtime code hash does not match deployment config".into(),
            ));
        }

        let assets = self
            .resolve_lane_assets(config, lane_assets, cursor.block_number)
            .await?;
        let cash = decode_address_word(
            &self
                .rpc
                .call_at(config.core, SELECTOR_CASH.into(), &self.snapshot_tag)
                .await?,
        )?;
        let blacklist_fee_multiplier = decode_word(
            &self
                .rpc
                .call_at(
                    config.core,
                    SELECTOR_BLACKLIST_FEE_MULTIPLIER.into(),
                    &self.snapshot_tag,
                )
                .await?,
            0,
        )?;
        let mut state = QuoteState {
            cash,
            blacklist_fee_multiplier,
            ..Default::default()
        };

        for asset in &assets {
            let lane_words = decode_words(
                &self
                    .rpc
                    .call_at(
                        config.core,
                        selector_address(SELECTOR_LANE, *asset),
                        &self.snapshot_tag,
                    )
                    .await?,
                5,
            )?;
            let reserve_words = decode_words(
                &self
                    .rpc
                    .call_at(
                        config.core,
                        selector_address(SELECTOR_RESERVES, *asset),
                        &self.snapshot_tag,
                    )
                    .await?,
                5,
            )?;
            let block_delay = u8::try_from(lane_words[3]).map_err(|_| {
                SourceError::Unavailable("lane blockDelay does not fit uint8".into())
            })?;
            let slippage_k_bps = u32::try_from(lane_words[4]).map_err(|_| {
                SourceError::Unavailable("lane slippageKBps does not fit uint32".into())
            })?;
            state.lanes.insert(
                *asset,
                LaneState {
                    slot0: lane_words[0],
                    exists: decode_bool(lane_words[1])?,
                    paused: decode_bool(lane_words[2])?,
                    block_delay,
                    slippage_k_bps,
                },
            );
            state.total_principal_amount.insert(
                *asset,
                checked_u128(reserve_words[4], "totalPrincipalAmount")?,
            );
        }

        let mut partner_assets = assets.clone();
        if !partner_assets.contains(&cash) {
            partner_assets.push(cash);
        }
        for router in routers {
            let whitelist = decode_bool(decode_word(
                &self
                    .rpc
                    .call_at(
                        config.core,
                        selector_address(SELECTOR_WHITELIST, *router),
                        &self.snapshot_tag,
                    )
                    .await?,
                0,
            )?)?;
            state.whitelist.insert(*router, whitelist);
            for asset in &partner_assets {
                let fee = decode_word(
                    &self
                        .rpc
                        .call_at(
                            config.core,
                            selector_two_addresses(SELECTOR_PARTNERS, *router, *asset),
                            &self.snapshot_tag,
                        )
                        .await?,
                    1,
                )?;
                state.partner_fee_bps.insert(
                    (*router, *asset),
                    U256::from(checked_u32(fee, "partner fee")?),
                );
            }
        }
        Ok(BootstrapSnapshot {
            state,
            cursor,
            runtime_code_hash,
        })
    }
}

impl RpcSnapshotProvider {
    async fn resolve_lane_assets(
        &self,
        config: &DeploymentConfig,
        explicit: &[Address],
        snapshot_block: u64,
    ) -> Result<Vec<Address>, SourceError> {
        let mut history = Vec::new();
        for topic in lane_discovery_topics() {
            let request = BackfillRequest {
                from_block: config.deployment_block,
                to_block: snapshot_block,
                filter: crate::ContractFilter {
                    address: config.core,
                    topics: vec![topic],
                },
            };
            history.extend(
                self.rpc
                    .get_logs(&request, config.chain_id, Commitment::Canonical)
                    .await?,
            );
        }
        history.sort_by_key(|log| log.cursor.event_order());
        let mut discovered = BTreeSet::new();
        for log in history {
            let Some(topic0) = log.topics.first().copied() else {
                continue;
            };
            let Some(asset_word) = log.topics.get(1).copied() else {
                continue;
            };
            let asset = decode_address_word(&format!(
                "0x{}",
                hex_encode(&asset_word.to_be_bytes::<32>())
            ))?;
            if topic0 == TOPIC_LANE_ADDED {
                discovered.insert(asset);
            } else if topic0 == TOPIC_LANE_REMOVED {
                discovered.remove(&asset);
            }
        }
        if explicit.is_empty() {
            return Ok(discovered.into_iter().collect());
        }
        if explicit.iter().any(|asset| !discovered.contains(asset)) {
            return Err(SourceError::Unavailable(
                "explicit lane asset was not active in deployment history".into(),
            ));
        }
        Ok(explicit.to_vec())
    }
}

pub(crate) fn parse_rpc_log(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_abi_address_arguments_as_padded_words() {
        let address = Address::from_hex("0x0000000000000000000000000000000000000001").unwrap();
        assert_eq!(
            selector_address(SELECTOR_LANE, address),
            "0xd1bacd10".to_owned() + &"0".repeat(63) + "1"
        );
    }

    #[test]
    fn decodes_five_reserve_words_and_rejects_wrong_width() {
        let data = format!("0x{}", "00".repeat(32 * 5));
        assert_eq!(decode_words(&data, 5).unwrap().len(), 5);
        assert!(decode_words(&data, 4).is_err());
    }

    #[test]
    fn hashes_runtime_code_with_keccak256() {
        assert_eq!(
            keccak256(b""),
            [
                0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7,
                0x03, 0xc0, 0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04,
                0x5d, 0x85, 0xa4, 0x70,
            ]
        );
    }
}
