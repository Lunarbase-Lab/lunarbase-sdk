//! Bit-exact packing and decoding for the protocol's `Lane.slot0` word.

use crate::types::{MathError, U256};

const PRICE_BITS: usize = 112;
const FEE_BITS: usize = 20;
const THRESHOLD_BITS: usize = 7;
const BLOCK_BITS: usize = 40;
const RESERVED_BITS: usize = 56;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Native-width boundary view of the packed `Lane.slot0` word.
///
/// The hot path retains the raw `U256`; this view is used only for explicit
/// pack/unpack operations and therefore does not spread `U256` through fields
/// whose Solidity widths are known.
pub struct LaneSlot0 {
    /// WAD-denominated lane price stored in the low 112 bits.
    pub price: u128,
    /// Exact-in fee applied when converting cash into the lane asset.
    pub ask_fee_bps: u32,
    /// Exact-in fee applied when converting the lane asset into cash.
    pub bid_fee_bps: u32,
    /// Seven-bit price movement threshold copied from the Solidity layout.
    pub price_push_threshold: u8,
    /// Whether `price_push_threshold` participates in on-chain update policy.
    pub threshold_enabled: bool,
    /// EVM block at which the packed lane price was last updated.
    pub latest_update_block: u64,
    /// Uninterpreted upper 56 bits preserved across decode/encode operations.
    pub reserved_high_bits: u64,
}

#[inline(always)]
fn field_mask(bits: usize) -> U256 {
    (U256::ONE << bits) - U256::ONE
}

#[inline(always)]
fn read_field(word: U256, shift: usize, bits: usize) -> U256 {
    (word >> shift) & field_mask(bits)
}

fn validate_native(value: u128, bits: usize, field: &'static str) -> Result<(), MathError> {
    if bits < 128 && value >= (1u128 << bits) {
        return Err(MathError::FieldOverflow {
            field,
            bits: bits as u16,
        });
    }
    Ok(())
}

fn validate_word(value: U256, bits: usize, field: &'static str) -> Result<(), MathError> {
    if value > field_mask(bits) {
        return Err(MathError::FieldOverflow {
            field,
            bits: bits as u16,
        });
    }
    Ok(())
}

/// Decodes the exact Solidity storage layout into native-width fields.
pub fn decode_lane_slot0(word: U256) -> LaneSlot0 {
    LaneSlot0 {
        price: read_field(word, 0, PRICE_BITS)
            .try_into()
            .expect("112-bit price fits u128"),
        ask_fee_bps: read_field(word, 112, FEE_BITS)
            .try_into()
            .expect("20-bit fee fits u32"),
        bid_fee_bps: read_field(word, 132, FEE_BITS)
            .try_into()
            .expect("20-bit fee fits u32"),
        price_push_threshold: read_field(word, 152, THRESHOLD_BITS)
            .try_into()
            .expect("7-bit threshold fits u8"),
        threshold_enabled: read_field(word, 159, 1) == U256::ONE,
        latest_update_block: read_field(word, 160, BLOCK_BITS)
            .try_into()
            .expect("40-bit block fits u64"),
        reserved_high_bits: read_field(word, 200, RESERVED_BITS)
            .try_into()
            .expect("56-bit reserved field fits u64"),
    }
}

#[inline(always)]
/// Reads the 112-bit pushed price as a word ready for quote arithmetic.
pub fn lane_slot0_price(word: U256) -> U256 {
    read_field(word, 0, PRICE_BITS)
}

#[inline(always)]
/// Reads the 20-bit ask fee as a word ready for quote arithmetic.
pub fn lane_slot0_ask_fee_bps(word: U256) -> U256 {
    read_field(word, 112, FEE_BITS)
}

#[inline(always)]
/// Reads the 20-bit bid fee as a word ready for quote arithmetic.
pub fn lane_slot0_bid_fee_bps(word: U256) -> U256 {
    read_field(word, 132, FEE_BITS)
}

#[inline(always)]
/// Reads the EVM block at which the lane price was last updated.
pub fn lane_slot0_latest_update_block(word: U256) -> u64 {
    read_field(word, 160, BLOCK_BITS)
        .try_into()
        .expect("40-bit block fits u64")
}

/// Encodes a native-width view into the exact 256-bit storage word.
///
/// # Errors
///
/// Returns [`MathError::FieldOverflow`] when a native value exceeds its
/// narrower Solidity storage field.
pub fn encode_lane_slot0(fields: &LaneSlot0) -> Result<U256, MathError> {
    validate_native(fields.price, PRICE_BITS, "price")?;
    validate_native(fields.ask_fee_bps.into(), FEE_BITS, "askFeeBps")?;
    validate_native(fields.bid_fee_bps.into(), FEE_BITS, "bidFeeBps")?;
    validate_native(
        fields.price_push_threshold.into(),
        THRESHOLD_BITS,
        "pricePushThreshold",
    )?;
    validate_native(
        fields.latest_update_block.into(),
        BLOCK_BITS,
        "latestUpdateBlock",
    )?;
    validate_native(
        fields.reserved_high_bits.into(),
        RESERVED_BITS,
        "reservedHighBits",
    )?;
    let mut word = U256::from(fields.price)
        | (U256::from(fields.ask_fee_bps) << 112)
        | (U256::from(fields.bid_fee_bps) << 132)
        | (U256::from(fields.price_push_threshold) << 152);
    if fields.threshold_enabled {
        word |= U256::ONE << 159;
    }
    word |= U256::from(fields.latest_update_block) << 160;
    word |= U256::from(fields.reserved_high_bits) << 200;
    Ok(word)
}

/// Packs two `uint20` fees into the contract's `uint40` calldata field.
///
/// # Errors
///
/// Returns [`MathError::FieldOverflow`] when either fee exceeds `uint20`.
pub fn encode_update_fees(ask_fee_bps: u32, bid_fee_bps: u32) -> Result<u64, MathError> {
    validate_native(ask_fee_bps.into(), FEE_BITS, "askFeeBps")?;
    validate_native(bid_fee_bps.into(), FEE_BITS, "bidFeeBps")?;
    Ok(u64::from(ask_fee_bps) | (u64::from(bid_fee_bps) << FEE_BITS))
}

/// Unpacks a contract `uint40` fee field into native fee widths.
///
/// # Errors
///
/// Returns [`MathError::FieldOverflow`] when bits above `uint40` are set.
pub fn decode_update_fees(fees: u64) -> Result<(u32, u32), MathError> {
    validate_native(fees.into(), 40, "fees")?;
    let mask = (1u64 << FEE_BITS) - 1;
    Ok(((fees & mask) as u32, (fees >> FEE_BITS) as u32))
}

/// Applies the Solidity lane update while preserving threshold/reserved bits.
///
/// # Errors
///
/// Returns [`MathError::FieldOverflow`] for values outside the corresponding
/// Solidity widths.
pub fn apply_lane_update_slot0(
    previous: U256,
    price: u128,
    fees: u64,
    block_number: u64,
) -> Result<U256, MathError> {
    validate_native(price, PRICE_BITS, "price")?;
    validate_native(fees.into(), 40, "fees")?;
    validate_native(block_number.into(), BLOCK_BITS, "blockNumber")?;
    let (ask_fee_bps, bid_fee_bps) = decode_update_fees(fees)?;
    let mut fields = decode_lane_slot0(previous);
    fields.price = price;
    fields.ask_fee_bps = ask_fee_bps;
    fields.bid_fee_bps = bid_fee_bps;
    fields.latest_update_block = block_number;
    encode_lane_slot0(&fields)
}

/// Validates and converts a word received from an ABI boundary to `uint112`.
pub fn lane_price_from_word(value: U256) -> Result<u128, MathError> {
    validate_word(value, PRICE_BITS, "price")?;
    Ok(value.try_into().expect("validated uint112 fits u128"))
}
