//! Base Flashblocks `pendingLogs` + `newFlashblocks` adapter.
//!
//! The provider-specific transport is kept here; the reducer only sees the
//! normal `ChainUpdate` model.  The adapter intentionally uses `base` on
//! index zero and `diff.block_hash` for the payload boundary.  It does not
//! read unstable `metadata` fields.

use crate::{BaseFlashblocksNormalizer, FlashblockHeader, FlashblockLog};
use async_stream::stream;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use lunarbase_client_core::{
    parse_rpc_log, BackfillRequest, ChainCursor, ChainUpdate, Commitment, ContractFilter,
    ContractLog, CursorReorderBuffer, Network, NormalizedBackend, RpcError, RpcHttpBackend,
    RpcHttpClient, SourceError, SourceStream,
};
use lunarbase_math::U256;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseFlashblocksConfig {
    pub ws_url: String,
    pub max_frame_bytes: usize,
    pub reorder_capacity: usize,
}

impl Default for BaseFlashblocksConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://mainnet-preconf.base.org".into(),
            max_frame_bytes: 512 * 1024,
            reorder_capacity: 4096,
        }
    }
}

impl BaseFlashblocksConfig {
    fn validate(&self) -> Result<(), SourceError> {
        if !(self.ws_url.starts_with("ws://") || self.ws_url.starts_with("wss://"))
            || self.max_frame_bytes == 0
            || self.reorder_capacity == 0
        {
            return Err(SourceError::Unavailable(
                "invalid Base Flashblocks WebSocket configuration".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct BaseFlashblocksBackend {
    http: RpcHttpBackend,
    config: Arc<BaseFlashblocksConfig>,
}

impl BaseFlashblocksBackend {
    /// Creates a Base Flashblocks backend with the default frame and reorder
    /// bounds.
    pub fn new(rpc: RpcHttpClient, ws_url: impl Into<String>, chain_id: u64) -> Self {
        Self::with_config(
            rpc,
            BaseFlashblocksConfig {
                ws_url: ws_url.into(),
                ..Default::default()
            },
            chain_id,
        )
    }

    /// Creates a Base Flashblocks backend with explicit provider settings.
    ///
    /// Flashblocks are provisional transport data. The normalizer exposes
    /// them as realtime updates, while the embedded HTTP backend remains the
    /// canonical recovery path.
    pub fn with_config(rpc: RpcHttpClient, config: BaseFlashblocksConfig, chain_id: u64) -> Self {
        Self {
            http: RpcHttpBackend::new(rpc, Network::Base, chain_id, "finalized"),
            config: Arc::new(config),
        }
    }

    /// Returns the immutable Flashblocks transport configuration.
    pub fn config(&self) -> &BaseFlashblocksConfig {
        &self.config
    }
}

#[async_trait]
impl NormalizedBackend for BaseFlashblocksBackend {
    async fn snapshot_cursor(&self, network: Network) -> Result<ChainCursor, SourceError> {
        self.http.snapshot_cursor(network).await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.http.backfill(request).await
    }

    async fn subscribe(
        &self,
        network: Network,
        filter: ContractFilter,
    ) -> Result<SourceStream, SourceError> {
        if network != Network::Base || network != self.http.network() {
            return Err(SourceError::NetworkMismatch);
        }
        self.config.validate()?;
        let (socket, _) = connect_async(&self.config.ws_url).await.map_err(|error| {
            SourceError::Unavailable(format!("Base Flashblocks connect failed: {error}"))
        })?;
        let (mut writer, mut reader) = socket.split();
        writer
            .send(Message::Text(pending_logs_request(1, &filter)))
            .await
            .map_err(|error| {
                SourceError::Unavailable(format!("Base pendingLogs subscribe failed: {error}"))
            })?;
        writer
            .send(Message::Text(
                json!({
                    "jsonrpc":"2.0",
                    "id":2,
                    "method":"eth_subscribe",
                    "params":["newFlashblocks"]
                })
                .to_string(),
            ))
            .await
            .map_err(|error| {
                SourceError::Unavailable(format!("Base newFlashblocks subscribe failed: {error}"))
            })?;

        let chain_id = self.http.chain_id();
        let config = self.config.clone();
        let output = stream! {
            let mut pending_subscription = None::<String>;
            let mut flashblocks_subscription = None::<String>;
            let mut normalizer = BaseFlashblocksNormalizer::new(chain_id);
            let mut headers = BTreeMap::<u64, Vec<FlashblockHeader>>::new();
            let mut reorder = match CursorReorderBuffer::new(config.reorder_capacity) {
                Ok(value) => value,
                Err(error) => { yield Err(error); return; }
            };
            loop {
                let Some(message) = reader.next().await else {
                    yield Ok(ChainUpdate::Gap { cursor: None, reason: "Base Flashblocks socket closed; canonical recovery required".into() });
                    break;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        yield Ok(ChainUpdate::Gap { cursor: None, reason: format!("Base Flashblocks socket failed; canonical recovery required: {error}") });
                        break;
                    }
                };
                let payload = match flashblocks_payload(message, &mut writer).await {
                    Ok(Some(payload)) => payload,
                    Ok(None) => continue,
                    Err(error) => { yield Ok(ChainUpdate::Gap { cursor: None, reason: error.to_string() }); break; }
                };
                if payload.len() > config.max_frame_bytes {
                    yield Ok(ChainUpdate::Gap { cursor: None, reason: "Base Flashblocks frame exceeded configured bound".into() });
                    break;
                }
                let value: Value = match serde_json::from_slice(&payload) {
                    Ok(value) => value,
                    Err(error) => { yield Ok(ChainUpdate::Gap { cursor: None, reason: format!("invalid Base Flashblocks JSON: {error}") }); break; }
                };
                if let Some(error) = value.get("error") {
                    yield Err(SourceError::Unavailable(format!("Base Flashblocks subscription error: {error}")));
                    break;
                }
                if let (Some(id), Some(result)) = (value.get("id").and_then(Value::as_u64), value.get("result").and_then(Value::as_str)) {
                    match id { 1 => pending_subscription = Some(result.to_owned()), 2 => flashblocks_subscription = Some(result.to_owned()), _ => {} }
                    continue;
                }
                if value.get("method").and_then(Value::as_str) != Some("eth_subscription") { continue; }
                let Some(params) = value.get("params").and_then(Value::as_object) else { continue; };
                let Some(subscription) = params.get("subscription").and_then(Value::as_str) else {
                    yield Ok(ChainUpdate::Gap { cursor: None, reason: "Base Flashblocks notification has no subscription id".into() }); break;
                };
                let Some(result) = params.get("result") else { continue; };

                if flashblocks_subscription.as_deref() == Some(subscription) {
                    let payload_id = match result.get("payload_id").and_then(Value::as_str).ok_or_else(|| RpcError::Invalid("Flashblock payload_id is missing".into())).and_then(parse_payload_id) {
                        Ok(payload_id) => payload_id,
                        Err(error) => { yield Ok(ChainUpdate::Gap { cursor: None, reason: format!("invalid Base Flashblock payload: {error}") }); break; }
                    };
                    let previous_block = headers.values().flatten().find(|header| header.payload_id == payload_id).map(|header| header.block_number);
                    let header = match parse_flashblock_header(result, previous_block) {
                        Ok(header) => header,
                        Err(error) => { yield Ok(ChainUpdate::Gap { cursor: None, reason: format!("invalid Base Flashblock payload: {error}") }); break; }
                    };
                    if let Some(previous_block) = headers.keys().next_back().copied() {
                        if header.block_number > previous_block.saturating_add(1) {
                            yield Ok(ChainUpdate::Gap {
                                cursor: None,
                                reason: "Base Flashblocks skipped one or more block payloads; canonical recovery required".into(),
                            });
                            break;
                        }
                    }
                    let block_headers = headers.entry(header.block_number).or_default();
                    block_headers.push(header.clone());
                    if block_headers.len() > 128 { block_headers.remove(0); }
                    while headers.len() > 64 { headers.pop_first(); }
                    if let Some(update) = normalizer.normalize_header(header.clone()).map_err(|error| SourceError::Gap(error.to_string()))? {
                        if let Err(error) = reorder.push(update) { yield Err(error); break; }
                        let watermark = ChainCursor::block(chain_id, header.block_number, header.block_hash, Commitment::Realtime);
                        for update in reorder.drain_through(&watermark) { yield Ok(update); }
                    }
                    continue;
                }

                if pending_subscription.as_deref() == Some(subscription) {
                    let log = match parse_pending_log(result, chain_id) {
                        Ok(log) => log,
                        Err(error) => { yield Ok(ChainUpdate::Gap { cursor: None, reason: format!("invalid Base pending log: {error}") }); break; }
                    };
                    let Some(header) = select_header(headers.get(&log.cursor.block_number), log.cursor.block_hash) else {
                        yield Ok(ChainUpdate::Gap { cursor: Some(log.cursor), reason: "Base pending log arrived without a matching Flashblock header".into() });
                        break;
                    };
                    let flashblock_log = FlashblockLog {
                        header,
                        transaction_index: log.cursor.transaction_index.unwrap_or_default(),
                        log_index: log.cursor.log_index.unwrap_or_default(),
                        address: log.address,
                        topics: log.topics,
                        data: log.data,
                        removed: log.removed,
                    };
                    for update in normalizer.normalize_log(flashblock_log).map_err(|error| SourceError::Gap(error.to_string()))? {
                        if let Err(error) = reorder.push(update) { yield Err(error); break; }
                    }
                    if let Some(header) = headers.get(&log.cursor.block_number).and_then(|items| items.last()) {
                        let watermark = ChainCursor::block(chain_id, header.block_number, header.block_hash, Commitment::Realtime);
                        for update in reorder.drain_through(&watermark) { yield Ok(update); }
                    }
                }
            }
        };
        Ok(Box::pin(output))
    }
}

fn pending_logs_request(id: u64, filter: &ContractFilter) -> String {
    let mut options = serde_json::Map::new();
    options.insert("address".into(), Value::String(filter.address.to_hex()));
    if !filter.topics.is_empty() {
        options.insert(
            "topics".into(),
            Value::Array(
                filter
                    .topics
                    .iter()
                    .map(|topic| Value::String(word_hex(*topic)))
                    .collect(),
            ),
        );
    }
    json!({"jsonrpc":"2.0","id":id,"method":"eth_subscribe","params":["pendingLogs",Value::Object(options)]}).to_string()
}

fn parse_flashblock_header(
    value: &Value,
    previous_block_number: Option<u64>,
) -> Result<FlashblockHeader, RpcError> {
    let object = value
        .as_object()
        .ok_or_else(|| RpcError::Invalid("Flashblock result is not an object".into()))?;
    let payload_id = parse_payload_id(
        object
            .get("payload_id")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::Invalid("Flashblock payload_id is missing".into()))?,
    )?;
    let index = parse_u64_value(object.get("index"), "Flashblock index")?;
    let diff = object
        .get("diff")
        .and_then(Value::as_object)
        .ok_or_else(|| RpcError::Invalid("Flashblock diff is missing".into()))?;
    let block_hash = parse_hash(
        diff.get("block_hash")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::Invalid("Flashblock diff.block_hash is missing".into()))?,
    )?;
    let block_number = if let Some(base) = object.get("base").and_then(Value::as_object) {
        parse_hex_u64(
            base.get("block_number")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    RpcError::Invalid("Flashblock base.block_number is missing".into())
                })?,
            "Flashblock base.block_number",
        )?
    } else {
        previous_block_number.ok_or_else(|| {
            RpcError::Invalid(
                "Flashblock index > 0 requires a previously observed block number".into(),
            )
        })?
    };
    Ok(FlashblockHeader {
        payload_id,
        block_number,
        block_hash: Some(block_hash),
        index,
    })
}

fn parse_pending_log(value: &Value, chain_id: u64) -> Result<ContractLog, RpcError> {
    parse_rpc_log(value, chain_id, Commitment::Realtime)
}

fn select_header(
    headers: Option<&Vec<FlashblockHeader>>,
    block_hash: Option<[u8; 32]>,
) -> Option<FlashblockHeader> {
    let headers = headers?;
    block_hash
        .and_then(|hash| {
            headers
                .iter()
                .rev()
                .find(|header| header.block_hash == Some(hash))
                .cloned()
        })
        .or_else(|| headers.last().cloned())
}

fn parse_payload_id(value: &str) -> Result<[u8; 32], RpcError> {
    let value = value
        .strip_prefix("0x")
        .ok_or_else(|| RpcError::Invalid("payload_id is missing 0x prefix".into()))?;
    if value.is_empty() || value.len() > 64 || value.len() % 2 != 0 {
        return Err(RpcError::Invalid(
            "payload_id must be an even hex value up to 32 bytes".into(),
        ));
    }
    let mut output = [0u8; 32];
    let offset = 32 - value.len() / 2;
    for index in 0..value.len() / 2 {
        output[offset + index] = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| RpcError::Invalid("payload_id is invalid hex".into()))?;
    }
    Ok(output)
}

fn parse_hash(value: &str) -> Result<[u8; 32], RpcError> {
    let value = value
        .strip_prefix("0x")
        .ok_or_else(|| RpcError::Invalid("hash is missing 0x prefix".into()))?;
    if value.len() != 64 {
        return Err(RpcError::Invalid("hash is not 32 bytes".into()));
    }
    let mut output = [0u8; 32];
    for index in 0..32 {
        output[index] = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| RpcError::Invalid("hash is invalid hex".into()))?;
    }
    Ok(output)
}

fn parse_hex_u64(value: &str, field: &str) -> Result<u64, RpcError> {
    let value = value
        .strip_prefix("0x")
        .ok_or_else(|| RpcError::Invalid(format!("{field} is missing 0x prefix")))?;
    u64::from_str_radix(value, 16).map_err(|_| RpcError::Invalid(format!("{field} is invalid")))
}

fn parse_u64_value(value: Option<&Value>, field: &str) -> Result<u64, RpcError> {
    let value = value.ok_or_else(|| RpcError::Invalid(format!("{field} is missing")))?;
    if let Some(number) = value.as_u64() {
        return Ok(number);
    }
    value
        .as_str()
        .ok_or_else(|| RpcError::Invalid(format!("{field} is invalid")))
        .and_then(|text| {
            if text.starts_with("0x") {
                parse_hex_u64(text, field)
            } else {
                text.parse()
                    .map_err(|_| RpcError::Invalid(format!("{field} is invalid")))
            }
        })
}

async fn flashblocks_payload<S>(
    message: Message,
    writer: &mut S,
) -> Result<Option<Vec<u8>>, SourceError>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    match message {
        Message::Text(text) => Ok(Some(text.as_bytes().to_vec())),
        Message::Binary(bytes) => Ok(Some(bytes.to_vec())),
        Message::Ping(bytes) => {
            writer.send(Message::Pong(bytes)).await.map_err(|error| {
                SourceError::Unavailable(format!("Base Flashblocks pong failed: {error}"))
            })?;
            Ok(None)
        }
        Message::Pong(_) => Ok(None),
        Message::Close(_) => Err(SourceError::Gap(
            "Base Flashblocks socket closed; canonical recovery required".into(),
        )),
        _ => Ok(None),
    }
}

fn word_hex(value: U256) -> String {
    format!(
        "0x{}",
        value
            .to_be_bytes::<32>()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use lunarbase_math::Address;

    #[test]
    fn pending_logs_subscription_uses_base_method() {
        let address = Address::from_hex("0x0000000000000000000000000000000000000001").unwrap();
        let request = pending_logs_request(
            1,
            &ContractFilter {
                address,
                topics: vec![U256::ONE],
            },
        );
        let value: Value = serde_json::from_str(&request).unwrap();
        assert_eq!(value["params"][0], "pendingLogs");
        assert_eq!(value["params"][1]["address"], address.to_hex());
    }

    #[test]
    fn parses_index_zero_flashblock_without_metadata() {
        let value = json!({"payload_id":"0x0102","index":0,"base":{"block_number":"0x2a"},"diff":{"block_hash":format!("0x{}", "11".repeat(32))}});
        let header = parse_flashblock_header(&value, None).unwrap();
        assert_eq!(header.payload_id[30..], [1, 2]);
        assert_eq!(header.block_number, 42);
        assert_eq!(header.index, 0);
    }

    #[test]
    fn rejects_nonzero_index_without_block_base() {
        let value = json!({"payload_id":"0x0102","index":1,"diff":{"block_hash":format!("0x{}", "11".repeat(32))}});
        assert!(parse_flashblock_header(&value, None).is_err());
    }
}
