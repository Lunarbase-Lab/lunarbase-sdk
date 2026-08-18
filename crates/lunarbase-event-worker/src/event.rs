//! Versioned Redis Stream and block-journal representations.

#[path = "event/reorg.rs"]
mod reorg;

pub(crate) use reorg::ReorgCorrection;

use alloy_primitives::{Address, B256, Keccak256, keccak256};
use lunarbase_client::model::{BlockRef, ChainCursor, Commitment, ContractLog};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub(crate) const STREAM_SCHEMA_VERSION: u16 = 2;
const REORG_ID_DOMAIN: &[u8] = b"lunarbase-durable-reorg-v3";
pub(crate) const ID_DOMAIN_VERSION: &str = "lunarbase-durable-v3";

const LOG_ID_DOMAIN: &[u8] = b"lunarbase-durable-log-v3";
const RECORD_ID_DOMAIN: &[u8] = b"lunarbase-durable-record-v3";

#[derive(Clone, Debug)]
pub(crate) struct DurableEvent {
    pub record_id: String,
    pub logical_log_id: String,
    pub block_hash: String,
    pub cursor_json: String,
    pub cursor_order: String,
    pub fields: Vec<(&'static str, String)>,
}

#[derive(Clone, Debug)]
pub(crate) struct DurableHead {
    pub header_json: String,
    pub block_hash: String,
    pub parent_hash: String,
    pub block_number: String,
    pub commitment: &'static str,
    pub cursor_json: String,
    pub cursor_order: String,
}

#[derive(Debug, Error)]
pub(crate) enum EventError {
    #[error("event JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("durable cursor belongs to another deployment")]
    CursorIdentity,
    #[error("durable log has no stable EVM identity: {0}")]
    StableIdentity(&'static str),
    #[error("durable head has incomplete block identity")]
    HeadIdentity,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CursorEnvelope {
    schema_version: u16,
    chain_id: u64,
    core: String,
    cursor: ChainCursor,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeaderEnvelope {
    schema_version: u16,
    chain_id: u64,
    block_number: u64,
    execution_block_number: u64,
    block_hash: B256,
    parent_hash: B256,
}

struct StableLogIdentity {
    block_hash: B256,
    transaction_hash: B256,
    transaction_index: u32,
    log_index: u32,
}

impl DurableEvent {
    pub(crate) fn retained_bytes(&self) -> usize {
        self.record_id
            .len()
            .saturating_add(self.logical_log_id.len())
            .saturating_add(self.block_hash.len())
            .saturating_add(self.cursor_json.len())
            .saturating_add(self.cursor_order.len())
            .saturating_add(self.fields.iter().fold(0_usize, |total, (name, value)| {
                total.saturating_add(name.len()).saturating_add(value.len())
            }))
            .saturating_add(std::mem::size_of::<Self>())
    }

    pub(crate) fn journal_reference_bytes(&self) -> usize {
        self.cursor_order
            .len()
            .saturating_add(self.logical_log_id.len())
            .saturating_add(self.record_id.len())
            .saturating_add(32)
            .saturating_add(3)
    }

    pub(crate) fn from_log(log: &ContractLog) -> Result<Self, EventError> {
        if log.removed {
            return Err(EventError::StableIdentity(
                "provider removals require resolved fork correction",
            ));
        }
        if log.topics.is_empty() || log.topics.len() > 4 {
            return Err(EventError::StableIdentity("topic0 is absent or invalid"));
        }
        let identity = stable_log_identity(log)?;
        let logical_log_id = logical_log_id(log, &identity);
        let record_id = origin_record_id(&logical_log_id, log);
        let core = format!("{:#x}", log.address);
        let cursor_json = encode_cursor(&log.cursor, &core)?;
        let mut fields = Vec::with_capacity(12);
        fields.extend([
            ("chainId", log.cursor.chain_id.to_string()),
            ("core", core),
            ("commitment", commitment_name(log.cursor.commitment).into()),
            ("blockNumber", log.cursor.block_number.to_string()),
            (
                "executionBlockNumber",
                log.cursor.execution_block_number.to_string(),
            ),
            (
                "transactionHash",
                format!("{:#x}", identity.transaction_hash),
            ),
            ("transactionIndex", identity.transaction_index.to_string()),
            ("logIndex", identity.log_index.to_string()),
        ]);
        if let Some(sequence) = log.cursor.source_sequence {
            fields.push(("sourceSequence", sequence.to_string()));
        }
        if let Some(sub_index) = log.cursor.source_sub_index {
            fields.push(("sourceSubIndex", sub_index.to_string()));
        }
        fields.extend([
            ("topics", serde_json::to_string(&log.topics)?),
            ("data", format!("{:#x}", log.data)),
        ]);
        let cursor_order = cursor_order(&log.cursor);
        Ok(Self {
            record_id,
            logical_log_id,
            block_hash: format!("{:#x}", identity.block_hash),
            cursor_json,
            cursor_order,
            fields,
        })
    }
}

impl DurableHead {
    pub(crate) fn retained_bytes(&self) -> usize {
        self.header_json
            .len()
            .saturating_add(self.block_hash.len())
            .saturating_add(self.parent_hash.len())
            .saturating_add(self.block_number.len())
            .saturating_add(self.cursor_json.len())
            .saturating_add(self.cursor_order.len())
            .saturating_add(std::mem::size_of::<Self>())
    }

    pub(crate) fn from_block(block: &BlockRef, core: Address) -> Result<Self, EventError> {
        let block_hash = block.cursor.block_hash.ok_or(EventError::HeadIdentity)?;
        let parent_hash = block.parent_hash.ok_or(EventError::HeadIdentity)?;
        if block_hash == B256::ZERO
            || block.cursor.transaction_index.is_some()
            || block.cursor.log_index.is_some()
        {
            return Err(EventError::HeadIdentity);
        }
        let header_json = serde_json::to_string(&HeaderEnvelope {
            schema_version: STREAM_SCHEMA_VERSION,
            chain_id: block.cursor.chain_id,
            block_number: block.cursor.block_number,
            execution_block_number: block.cursor.execution_block_number,
            block_hash,
            parent_hash,
        })?;
        Ok(Self {
            header_json,
            block_hash: format!("{block_hash:#x}"),
            parent_hash: format!("{parent_hash:#x}"),
            block_number: block.cursor.block_number.to_string(),
            commitment: commitment_name(block.cursor.commitment),
            cursor_json: encode_cursor(&block.cursor, &format!("{core:#x}"))?,
            cursor_order: cursor_order(&block.cursor),
        })
    }
}

pub(crate) fn decode_header(payload: &str, expected_chain_id: u64) -> Result<BlockRef, EventError> {
    let envelope: HeaderEnvelope = serde_json::from_str(payload)?;
    if envelope.schema_version != STREAM_SCHEMA_VERSION
        || envelope.chain_id != expected_chain_id
        || envelope.block_hash == B256::ZERO
        || envelope.parent_hash == B256::ZERO
    {
        return Err(EventError::HeadIdentity);
    }
    let mut cursor = ChainCursor::block(
        envelope.chain_id,
        envelope.block_number,
        Some(envelope.block_hash),
        Commitment::Canonical,
    );
    cursor.execution_block_number = envelope.execution_block_number;
    Ok(BlockRef::new(cursor, Some(envelope.parent_hash)))
}

pub(crate) fn decode_cursor(
    payload: &[u8],
    expected_chain_id: u64,
    expected_core: Address,
) -> Result<ChainCursor, EventError> {
    let envelope: CursorEnvelope = serde_json::from_slice(payload)?;
    if envelope.schema_version != STREAM_SCHEMA_VERSION
        || envelope.chain_id != expected_chain_id
        || envelope.core != format!("{expected_core:#x}")
        || envelope.cursor.chain_id != expected_chain_id
    {
        return Err(EventError::CursorIdentity);
    }
    Ok(envelope.cursor)
}

fn encode_cursor(cursor: &ChainCursor, core: &str) -> Result<String, serde_json::Error> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BorrowedCursor<'a> {
        schema_version: u16,
        chain_id: u64,
        core: &'a str,
        cursor: &'a ChainCursor,
    }

    serde_json::to_string(&BorrowedCursor {
        schema_version: STREAM_SCHEMA_VERSION,
        chain_id: cursor.chain_id,
        core,
        cursor,
    })
}

fn stable_log_identity(log: &ContractLog) -> Result<StableLogIdentity, EventError> {
    let block_hash = log
        .cursor
        .block_hash
        .filter(|hash| *hash != B256::ZERO)
        .ok_or(EventError::StableIdentity("block hash is absent"))?;
    let transaction_hash = log
        .transaction_hash
        .filter(|hash| *hash != B256::ZERO)
        .ok_or(EventError::StableIdentity("transaction hash is absent"))?;
    let transaction_index = log
        .cursor
        .transaction_index
        .ok_or(EventError::StableIdentity("transaction index is absent"))?;
    let log_index = log
        .cursor
        .log_index
        .ok_or(EventError::StableIdentity("log index is absent"))?;
    Ok(StableLogIdentity {
        block_hash,
        transaction_hash,
        transaction_index,
        log_index,
    })
}

fn logical_log_id(log: &ContractLog, identity: &StableLogIdentity) -> String {
    let mut preimage = [0_u8; LOG_ID_DOMAIN.len() + 100];
    let mut offset = 0;
    append_bytes(&mut preimage, &mut offset, LOG_ID_DOMAIN);
    append_bytes(
        &mut preimage,
        &mut offset,
        &log.cursor.chain_id.to_be_bytes(),
    );
    append_bytes(&mut preimage, &mut offset, log.address.as_slice());
    append_bytes(&mut preimage, &mut offset, identity.block_hash.as_slice());
    append_bytes(
        &mut preimage,
        &mut offset,
        identity.transaction_hash.as_slice(),
    );
    append_bytes(
        &mut preimage,
        &mut offset,
        &identity.transaction_index.to_be_bytes(),
    );
    append_bytes(
        &mut preimage,
        &mut offset,
        &identity.log_index.to_be_bytes(),
    );
    debug_assert_eq!(offset, preimage.len());
    encode_id(keccak256(preimage))
}

fn origin_record_id(logical_log_id: &str, log: &ContractLog) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(RECORD_ID_DOMAIN);
    hasher.update(logical_log_id.as_bytes());
    hasher.update(b"origin");
    hasher.update(log.cursor.execution_block_number.to_be_bytes());
    hasher.update((log.topics.len() as u64).to_be_bytes());
    for topic in &log.topics {
        hasher.update(topic.as_slice());
    }
    hasher.update((log.data.len() as u64).to_be_bytes());
    hasher.update(log.data.as_ref());
    encode_id(hasher.finalize())
}

fn append_bytes(target: &mut [u8], offset: &mut usize, value: &[u8]) {
    let end = offset.saturating_add(value.len());
    target[*offset..end].copy_from_slice(value);
    *offset = end;
}

fn encode_id(digest: B256) -> String {
    format!("v3:{digest:#x}")
}

fn cursor_order(cursor: &ChainCursor) -> String {
    let (block, transaction, log, sequence, sub_index) = cursor.event_order();
    format!("{block:020}:{transaction:010}:{log:010}:{sequence:020}:{sub_index:010}")
}

pub(crate) const fn commitment_name(commitment: Commitment) -> &'static str {
    match commitment {
        Commitment::Realtime => "realtime",
        Commitment::Canonical => "block-ordered",
        Commitment::Finalized => "finalized",
    }
}

#[cfg(test)]
mod tests {
    use super::{DurableEvent, DurableHead, decode_cursor};
    use alloy_primitives::{Address, B256, Bytes};
    use lunarbase_client::model::{BlockRef, ChainCursor, Commitment, ContractLog};

    #[test]
    fn logical_identity_is_operation_and_commitment_independent() {
        let canonical = log(Commitment::Canonical, 1);
        let mut realtime = canonical.clone();
        realtime.cursor.commitment = Commitment::Realtime;
        let canonical = DurableEvent::from_log(&canonical).unwrap();
        let realtime = DurableEvent::from_log(&realtime).unwrap();
        assert_eq!(canonical.logical_log_id, realtime.logical_log_id);
        assert_eq!(canonical.record_id, realtime.record_id);
        assert_eq!(canonical.logical_log_id.len(), 69);
    }

    #[test]
    fn same_log_position_with_altered_payload_gets_a_distinct_record_id() {
        let original = log(Commitment::Realtime, 1);
        let mut altered = original.clone();
        altered.cursor.execution_block_number = 99;
        altered.topics[0] = B256::new([9; 32]);
        altered.data = Bytes::from(vec![8; 64]);
        let original = DurableEvent::from_log(&original).unwrap();
        let altered = DurableEvent::from_log(&altered).unwrap();
        assert_eq!(original.logical_log_id, altered.logical_log_id);
        assert_ne!(original.record_id, altered.record_id);
    }

    #[test]
    fn schema_v2_omits_duplicate_payload_and_rejects_unstable_logs() {
        let log = log(Commitment::Realtime, 1);
        let event = DurableEvent::from_log(&log).unwrap();
        let names = event
            .fields
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>();
        assert!(!names.contains(&"rawLog"));
        assert!(!names.contains(&"eventName"));
        assert!(!names.contains(&"logicalLogId"));
        assert_eq!(
            decode_cursor(event.cursor_json.as_bytes(), 8453, log.address).unwrap(),
            log.cursor
        );

        let mut removed = log;
        removed.removed = true;
        assert!(DurableEvent::from_log(&removed).is_err());
    }

    #[test]
    fn head_journal_identity_excludes_commitment_promotion() {
        let core = Address::new([4; 20]);
        let block = BlockRef::new(
            ChainCursor::block(8453, 41, Some(B256::new([2; 32])), Commitment::Canonical),
            Some(B256::new([1; 32])),
        );
        let mut finalized = block.clone();
        finalized.cursor.commitment = Commitment::Finalized;
        let canonical = DurableHead::from_block(&block, core).unwrap();
        let finalized = DurableHead::from_block(&finalized, core).unwrap();
        assert_eq!(canonical.header_json, finalized.header_json);
        assert_ne!(canonical.commitment, finalized.commitment);
    }

    fn log(commitment: Commitment, payload: u8) -> ContractLog {
        ContractLog {
            address: Address::new([4; 20]),
            transaction_hash: Some(B256::new([3; 32])),
            topics: vec![B256::new([payload; 32])],
            data: Bytes::from(vec![payload; 64]),
            removed: false,
            cursor: ChainCursor {
                chain_id: 8453,
                block_number: 41,
                execution_block_number: 41,
                block_hash: Some(B256::new([2; 32])),
                transaction_index: Some(2),
                log_index: Some(3),
                source_sequence: Some(7),
                source_sub_index: Some(1),
                commitment,
            },
        }
    }
}
