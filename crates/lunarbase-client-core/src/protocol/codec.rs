//! Core versioned binary encoding for checkpoints and normalized updates.

use crate::{ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractLog};
use lunarbase_math::{Address, QuoteState, U256};

const CHECKPOINT_CODEC_MAGIC: [u8; 4] = *b"LBQ1";

pub(crate) fn bytes32_hex(value: [u8; 32]) -> String {
    let mut result = String::with_capacity(66);
    result.push_str("0x");
    for byte in value {
        result.push_str(&format!("{byte:02x}"));
    }
    result
}

pub(crate) fn decode_fixed_hex32(value: &str) -> Result<[u8; 32], String> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 64 {
        return Err("expected 32-byte hex value".into());
    }
    let mut result = [0u8; 32];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "invalid 32-byte hex value")?;
    }
    Ok(result)
}

struct BinaryEncoder {
    bytes: Vec<u8>,
}

impl BinaryEncoder {
    fn new() -> Self {
        Self {
            bytes: CHECKPOINT_CODEC_MAGIC.to_vec(),
        }
    }
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }
    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }
    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
    fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }
    fn address(&mut self, value: Address) {
        self.bytes(&value.0);
    }
    fn u256(&mut self, value: U256) {
        self.bytes(&value.to_be_bytes::<32>());
    }
    fn string(&mut self, value: &str) {
        self.u32(value.len() as u32);
        self.bytes(value.as_bytes());
    }
    fn optional<T>(&mut self, value: Option<T>, write: impl FnOnce(&mut Self, T)) {
        self.bool(value.is_some());
        if let Some(value) = value {
            write(self, value);
        }
    }
    fn cursor(&mut self, cursor: &ChainCursor) {
        self.u64(cursor.chain_id);
        self.u64(cursor.block_number);
        self.optional(cursor.block_hash, Self::bytes32);
        self.optional(cursor.transaction_index, Self::u32);
        self.optional(cursor.log_index, Self::u32);
        self.optional(cursor.source_sequence, Self::u64);
        self.optional(cursor.source_sub_index, Self::u32);
        self.u8(match cursor.commitment {
            Commitment::Realtime => 0,
            Commitment::Canonical => 1,
            Commitment::Finalized => 2,
        });
    }
    fn bytes32(&mut self, value: [u8; 32]) {
        self.bytes(&value);
    }
    fn state(&mut self, state: &QuoteState) -> Result<(), String> {
        self.address(state.cash);
        self.u64(state.state_version);
        self.u256(state.blacklist_fee_multiplier);
        self.u32(state.lanes.len() as u32);
        for (asset, lane) in &state.lanes {
            self.address(*asset);
            self.u256(lane.slot0);
            self.bool(lane.exists);
            self.bool(lane.paused);
            self.u8(lane.block_delay);
            self.u32(lane.slippage_k_bps);
        }
        self.u32(state.total_principal_amount.len() as u32);
        for (asset, amount) in &state.total_principal_amount {
            self.address(*asset);
            self.u256(*amount);
        }
        self.u32(state.whitelist.len() as u32);
        for (router, whitelisted) in &state.whitelist {
            self.address(*router);
            self.bool(*whitelisted);
        }
        self.u32(state.partner_fee_bps.len() as u32);
        for ((router, asset), fee) in &state.partner_fee_bps {
            self.address(*router);
            self.address(*asset);
            self.u256(*fee);
        }
        Ok(())
    }
}

struct BinaryReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BinaryReader<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, String> {
        if bytes.get(..4) != Some(&CHECKPOINT_CODEC_MAGIC) {
            return Err("invalid checkpoint codec magic".into());
        }
        Ok(Self { bytes, offset: 4 })
    }
    fn raw(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or("checkpoint length overflow")?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or("truncated checkpoint")?;
        self.offset = end;
        Ok(result)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(*self.take(1)?.first().ok_or("truncated u8")?)
    }
    fn bool(&mut self) -> Result<bool, String> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("invalid boolean".into()),
        }
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| "invalid u16")?,
        ))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| "invalid u32")?,
        ))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| "invalid u64")?,
        ))
    }
    fn address(&mut self) -> Result<Address, String> {
        Ok(Address(
            self.take(20)?.try_into().map_err(|_| "invalid address")?,
        ))
    }
    fn u256(&mut self) -> Result<U256, String> {
        Ok(U256::from_be_bytes::<32>(
            self.take(32)?.try_into().map_err(|_| "invalid u256")?,
        ))
    }
    fn string(&mut self) -> Result<String, String> {
        let length = self.u32()? as usize;
        String::from_utf8(self.take(length)?.to_vec()).map_err(|_| "invalid utf8".into())
    }
    fn optional<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T, String>,
    ) -> Result<Option<T>, String> {
        if self.bool()? {
            Ok(Some(read(self)?))
        } else {
            Ok(None)
        }
    }
    fn cursor(&mut self) -> Result<ChainCursor, String> {
        let chain_id = self.u64()?;
        let block_number = self.u64()?;
        let block_hash = self.optional(|reader| {
            Ok(reader
                .take(32)?
                .try_into()
                .map_err(|_| "invalid block hash")?)
        })?;
        let transaction_index = self.optional(Self::u32)?;
        let log_index = self.optional(Self::u32)?;
        let source_sequence = self.optional(Self::u64)?;
        let source_sub_index = self.optional(Self::u32)?;
        let commitment = match self.u8()? {
            0 => Commitment::Realtime,
            1 => Commitment::Canonical,
            2 => Commitment::Finalized,
            _ => return Err("invalid commitment".into()),
        };
        Ok(ChainCursor {
            chain_id,
            block_number,
            block_hash,
            transaction_index,
            log_index,
            source_sequence,
            source_sub_index,
            commitment,
        })
    }
    fn state(&mut self) -> Result<QuoteState, String> {
        let mut state = QuoteState {
            cash: self.address()?,
            state_version: self.u64()?,
            blacklist_fee_multiplier: self.u256()?,
            ..Default::default()
        };
        for _ in 0..self.u32()? {
            let asset = self.address()?;
            let slot0 = self.u256()?;
            state.lanes.insert(
                asset,
                lunarbase_math::LaneState {
                    slot0,
                    exists: self.bool()?,
                    paused: self.bool()?,
                    block_delay: self.u8()?,
                    slippage_k_bps: self.u32()?,
                },
            );
        }
        for _ in 0..self.u32()? {
            state
                .total_principal_amount
                .insert(self.address()?, self.u256()?);
        }
        for _ in 0..self.u32()? {
            state.whitelist.insert(self.address()?, self.bool()?);
        }
        for _ in 0..self.u32()? {
            state
                .partner_fee_bps
                .insert((self.address()?, self.address()?), self.u256()?);
        }
        Ok(state)
    }
    fn done(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

/// Encode a checkpoint with fixed-width U256/address fields. This format is
/// shared by the Redis store and can be implemented by non-Rust clients.
///
/// The `LBQ1` header, explicit option flags, fixed-width integers, and sorted
/// state maps make the result stable across Rust and TypeScript processes.
///
/// # Errors
///
/// Returns an error if a string/collection length or fixed-width field cannot
/// be represented by the schema.
pub fn encode_checkpoint(checkpoint: &Checkpoint) -> Result<Vec<u8>, String> {
    let mut encoder = BinaryEncoder::new();
    encoder.u16(checkpoint.schema_version);
    encoder.string(&checkpoint.math_compatibility_version);
    encoder.bytes(&checkpoint.expected_runtime_code_hash);
    encoder.cursor(&checkpoint.cursor);
    encoder.state(&checkpoint.state)?;
    Ok(encoder.bytes)
}

/// Decodes and validates one `LBQ1` checkpoint payload.
///
/// Trailing bytes, invalid enum tags, malformed UTF-8, and truncated fields
/// are rejected so a partially written payload cannot become ready state.
pub fn decode_checkpoint(bytes: &[u8]) -> Result<Checkpoint, String> {
    let mut reader = BinaryReader::new(bytes)?;
    let schema_version = reader.u16()?;
    let math_compatibility_version = reader.string()?;
    let expected_runtime_code_hash = reader
        .take(32)?
        .try_into()
        .map_err(|_| "invalid code hash")?;
    let cursor = reader.cursor()?;
    let state = reader.state()?;
    if !reader.done() {
        return Err("trailing checkpoint bytes".into());
    }
    Ok(Checkpoint {
        schema_version,
        math_compatibility_version,
        expected_runtime_code_hash,
        cursor,
        state,
    })
}

/// Encodes one normalized update without JSON numbers or floating point.
///
/// U256 values are big-endian 32-byte fields, addresses are 20-byte fields,
/// and all optional cursor components carry explicit presence flags.
pub fn encode_update(update: &ChainUpdate) -> Vec<u8> {
    let mut encoder = BinaryEncoder { bytes: Vec::new() };
    match update {
        ChainUpdate::Head(cursor) => {
            encoder.u8(0);
            encoder.cursor(cursor);
        }
        ChainUpdate::Log(log) => {
            encoder.u8(1);
            encoder.address(log.address);
            encoder.u32(log.topics.len() as u32);
            for topic in &log.topics {
                encoder.u256(*topic);
            }
            encoder.u32(log.data.len() as u32);
            encoder.bytes(&log.data);
            encoder.bool(log.removed);
            encoder.cursor(&log.cursor);
        }
        ChainUpdate::Reorg { old_head, new_head } => {
            encoder.u8(2);
            encoder.cursor(old_head);
            encoder.cursor(new_head);
        }
        ChainUpdate::Gap { cursor, reason } => {
            encoder.u8(3);
            encoder.optional(cursor.clone(), |encoder, cursor| encoder.cursor(&cursor));
            encoder.string(reason);
        }
        ChainUpdate::SourceHealth { healthy, detail } => {
            encoder.u8(4);
            encoder.bool(*healthy);
            encoder.string(detail);
        }
    }
    encoder.bytes
}

/// Decodes one normalized update and rejects malformed/trailing payload data.
pub fn decode_update(bytes: &[u8]) -> Result<ChainUpdate, String> {
    let mut reader = BinaryReader::raw(bytes);
    let update = match reader.u8()? {
        0 => ChainUpdate::Head(reader.cursor()?),
        1 => {
            let address = reader.address()?;
            let mut topics = Vec::new();
            for _ in 0..reader.u32()? {
                topics.push(reader.u256()?);
            }
            let data_length = reader.u32()? as usize;
            let data = reader.take(data_length)?.to_vec();
            let removed = reader.bool()?;
            ChainUpdate::Log(ContractLog {
                address,
                topics,
                data,
                removed,
                cursor: reader.cursor()?,
            })
        }
        2 => ChainUpdate::Reorg {
            old_head: reader.cursor()?,
            new_head: reader.cursor()?,
        },
        3 => ChainUpdate::Gap {
            cursor: reader.optional(|reader| reader.cursor())?,
            reason: reader.string()?,
        },
        4 => ChainUpdate::SourceHealth {
            healthy: reader.bool()?,
            detail: reader.string()?,
        },
        _ => return Err("invalid update variant".into()),
    };
    if !reader.done() {
        return Err("trailing update bytes".into());
    }
    Ok(update)
}
