//! Bit-exact packing and decoding for the protocol's `Lane.slot0` word.

use crate::types::{MathError, U256};

const PRICE_BITS: usize = 112;
const FEE_BITS: usize = 20;
const THRESHOLD_BITS: usize = 7;
const BLOCK_BITS: usize = 40;
const BLOCK_DELAY_BITS: usize = 8;
const SLIPPAGE_K_BITS: usize = 32;
const RESERVED_BITS: usize = 14;

const PRICE_SHIFT: usize = 0;
const ASK_FEE_SHIFT: usize = 112;
const BID_FEE_SHIFT: usize = 132;
const THRESHOLD_SHIFT: usize = 152;
const THRESHOLD_ENABLED_SHIFT: usize = 159;
const UPDATE_BLOCK_SHIFT: usize = 160;
const EXISTS_SHIFT: usize = 200;
const PAUSED_SHIFT: usize = 201;
const BLOCK_DELAY_SHIFT: usize = 202;
const SLIPPAGE_K_SHIFT: usize = 210;
const RESERVED_SHIFT: usize = 242;

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
    /// Whether Core currently exposes this lane.
    pub exists: bool,
    /// Whether swaps through this lane are disabled.
    pub paused: bool,
    /// Inclusive quote TTL in execution blocks after a price update.
    pub block_delay: u8,
    /// Lane-specific slippage coefficient in protocol BPS.
    pub slippage_k_bps: u32,
    /// Uninterpreted upper 14 bits preserved across decode/encode operations.
    pub reserved_high_bits: u16,
}

#[inline(always)]
fn field_mask(bits: usize) -> U256 {
    (U256::ONE << bits) - U256::ONE
}

#[inline(always)]
fn read_field(word: U256, shift: usize, bits: usize) -> U256 {
    (word >> shift) & field_mask(bits)
}

#[inline(always)]
fn write_field(word: U256, value: U256, shift: usize, bits: usize) -> U256 {
    let shifted_mask = field_mask(bits) << shift;
    (word & !shifted_mask) | ((value & field_mask(bits)) << shift)
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

/// Decodes the exact Solidity storage layout into native-width fields.
pub fn decode_lane_slot0(word: U256) -> LaneSlot0 {
    LaneSlot0 {
        price: read_field(word, PRICE_SHIFT, PRICE_BITS)
            .try_into()
            .expect("112-bit price fits u128"),
        ask_fee_bps: read_field(word, ASK_FEE_SHIFT, FEE_BITS)
            .try_into()
            .expect("20-bit fee fits u32"),
        bid_fee_bps: read_field(word, BID_FEE_SHIFT, FEE_BITS)
            .try_into()
            .expect("20-bit fee fits u32"),
        price_push_threshold: read_field(word, THRESHOLD_SHIFT, THRESHOLD_BITS)
            .try_into()
            .expect("7-bit threshold fits u8"),
        threshold_enabled: read_field(word, THRESHOLD_ENABLED_SHIFT, 1) == U256::ONE,
        latest_update_block: read_field(word, UPDATE_BLOCK_SHIFT, BLOCK_BITS)
            .try_into()
            .expect("40-bit block fits u64"),
        exists: read_field(word, EXISTS_SHIFT, 1) == U256::ONE,
        paused: read_field(word, PAUSED_SHIFT, 1) == U256::ONE,
        block_delay: read_field(word, BLOCK_DELAY_SHIFT, BLOCK_DELAY_BITS)
            .try_into()
            .expect("8-bit block delay fits u8"),
        slippage_k_bps: read_field(word, SLIPPAGE_K_SHIFT, SLIPPAGE_K_BITS)
            .try_into()
            .expect("32-bit slippage K fits u32"),
        reserved_high_bits: read_field(word, RESERVED_SHIFT, RESERVED_BITS)
            .try_into()
            .expect("14-bit reserved field fits u16"),
    }
}

#[inline(always)]
/// Reads the 112-bit pushed price as a word ready for quote arithmetic.
pub fn lane_slot0_price(word: U256) -> U256 {
    read_field(word, PRICE_SHIFT, PRICE_BITS)
}

#[inline(always)]
/// Reads the 20-bit ask fee as a word ready for quote arithmetic.
pub fn lane_slot0_ask_fee_bps(word: U256) -> U256 {
    read_field(word, ASK_FEE_SHIFT, FEE_BITS)
}

#[inline(always)]
/// Reads the 20-bit bid fee as a word ready for quote arithmetic.
pub fn lane_slot0_bid_fee_bps(word: U256) -> U256 {
    read_field(word, BID_FEE_SHIFT, FEE_BITS)
}

#[inline(always)]
/// Reads the EVM block at which the lane price was last updated.
pub fn lane_slot0_latest_update_block(word: U256) -> u64 {
    read_field(word, UPDATE_BLOCK_SHIFT, BLOCK_BITS)
        .try_into()
        .expect("40-bit block fits u64")
}

#[inline(always)]
/// Reads whether the packed lane exists.
pub fn lane_slot0_exists(word: U256) -> bool {
    read_field(word, EXISTS_SHIFT, 1) == U256::ONE
}

#[inline(always)]
/// Reads whether the packed lane is paused.
pub fn lane_slot0_paused(word: U256) -> bool {
    read_field(word, PAUSED_SHIFT, 1) == U256::ONE
}

#[inline(always)]
/// Reads the packed inclusive quote TTL in execution blocks.
pub fn lane_slot0_block_delay(word: U256) -> u8 {
    read_field(word, BLOCK_DELAY_SHIFT, BLOCK_DELAY_BITS)
        .try_into()
        .expect("8-bit block delay fits u8")
}

#[inline(always)]
/// Reads the packed lane slippage coefficient.
pub fn lane_slot0_slippage_k_bps(word: U256) -> u32 {
    read_field(word, SLIPPAGE_K_SHIFT, SLIPPAGE_K_BITS)
        .try_into()
        .expect("32-bit slippage K fits u32")
}

#[inline(always)]
/// Replaces the packed existence bit.
pub fn set_lane_slot0_exists(word: U256, exists: bool) -> U256 {
    write_field(word, U256::from(exists), EXISTS_SHIFT, 1)
}

#[inline(always)]
/// Replaces the packed lane pause bit.
pub fn set_lane_slot0_paused(word: U256, paused: bool) -> U256 {
    write_field(word, U256::from(paused), PAUSED_SHIFT, 1)
}

/// Replaces the packed price-push threshold and its enable bit.
///
/// # Errors
///
/// Returns [`MathError::FieldOverflow`] when `price_push_threshold` exceeds
/// the contract's seven-bit field.
pub fn set_lane_slot0_price_push_threshold(
    word: U256,
    price_push_threshold: u8,
    enabled: bool,
) -> Result<U256, MathError> {
    validate_native(
        price_push_threshold.into(),
        THRESHOLD_BITS,
        "pricePushThreshold",
    )?;
    let word = write_field(
        word,
        U256::from(price_push_threshold),
        THRESHOLD_SHIFT,
        THRESHOLD_BITS,
    );
    Ok(write_field(
        word,
        U256::from(enabled),
        THRESHOLD_ENABLED_SHIFT,
        1,
    ))
}

#[inline(always)]
/// Replaces the packed block-delay field.
pub fn set_lane_slot0_block_delay(word: U256, block_delay: u8) -> U256 {
    write_field(
        word,
        U256::from(block_delay),
        BLOCK_DELAY_SHIFT,
        BLOCK_DELAY_BITS,
    )
}

#[inline(always)]
/// Replaces the packed slippage coefficient.
pub fn set_lane_slot0_slippage_k_bps(word: U256, slippage_k_bps: u32) -> U256 {
    write_field(
        word,
        U256::from(slippage_k_bps),
        SLIPPAGE_K_SHIFT,
        SLIPPAGE_K_BITS,
    )
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
        | (U256::from(fields.ask_fee_bps) << ASK_FEE_SHIFT)
        | (U256::from(fields.bid_fee_bps) << BID_FEE_SHIFT)
        | (U256::from(fields.price_push_threshold) << THRESHOLD_SHIFT);
    if fields.threshold_enabled {
        word |= U256::ONE << THRESHOLD_ENABLED_SHIFT;
    }
    word |= U256::from(fields.latest_update_block) << UPDATE_BLOCK_SHIFT;
    if fields.exists {
        word |= U256::ONE << EXISTS_SHIFT;
    }
    if fields.paused {
        word |= U256::ONE << PAUSED_SHIFT;
    }
    word |= U256::from(fields.block_delay) << BLOCK_DELAY_SHIFT;
    word |= U256::from(fields.slippage_k_bps) << SLIPPAGE_K_SHIFT;
    word |= U256::from(fields.reserved_high_bits) << RESERVED_SHIFT;
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
fn decode_update_fees(fees: u64) -> Result<(u32, u32), MathError> {
    validate_native(fees.into(), 40, "fees")?;
    let mask = (1u64 << FEE_BITS) - 1;
    Ok(((fees & mask) as u32, (fees >> FEE_BITS) as u32))
}

/// Applies the Solidity operator update, including threshold-triggered pause.
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
    let delta = price.abs_diff(fields.price);
    let exceeds_threshold = fields.threshold_enabled
        && fields.price != 0
        && delta * 100 > fields.price * u128::from(fields.price_push_threshold);
    fields.price = price;
    fields.ask_fee_bps = ask_fee_bps;
    fields.bid_fee_bps = bid_fee_bps;
    fields.latest_update_block = block_number;
    if exceeds_threshold {
        fields.paused = true;
    }
    encode_lane_slot0(&fields)
}
