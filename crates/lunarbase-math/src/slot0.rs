use crate::{MathError, U256};

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Boundary view of the packed `Lane.slot0` word.
///
/// The struct is intended for decoding, validation, and explicit updates. The
/// hot quote path should normally keep the raw `U256` word and use the masked
/// accessors below to avoid allocating a view for every lane.
pub struct LaneSlot0 {
    pub price: U256,
    pub ask_fee_bps: U256,
    pub bid_fee_bps: U256,
    pub price_push_threshold: U256,
    pub threshold_enabled: bool,
    pub latest_update_block: U256,
    pub reserved_high_bits: U256,
}
impl Default for LaneSlot0 {
    fn default() -> Self {
        Self {
            price: U256::ZERO,
            ask_fee_bps: U256::ZERO,
            bid_fee_bps: U256::ZERO,
            price_push_threshold: U256::ZERO,
            threshold_enabled: false,
            latest_update_block: U256::ZERO,
            reserved_high_bits: U256::ZERO,
        }
    }
}
#[inline(always)]
fn field_mask(bits: usize) -> U256 {
    (U256::ONE << bits) - U256::ONE
}
#[inline(always)]
fn read_field(word: U256, shift: usize, bits: usize) -> U256 {
    (word >> shift) & field_mask(bits)
}
fn validate_field(value: U256, bits: usize, field: &'static str) -> Result<(), MathError> {
    if value > field_mask(bits) {
        return Err(MathError::FieldOverflow {
            field,
            bits: bits as u16,
        });
    }
    Ok(())
}
/// Decodes the packed `Lane.slot0` storage word without changing any bits.
///
/// The fields follow the Solidity layout: price `[0,112)`, ask fee `[112,132)`,
/// bid fee `[132,152)`, threshold `[152,160)`, latest update block
/// `[160,200)`, and preserved high bits `[200,256)`. Reserved bits are exposed
/// so a read-modify-write update can preserve them exactly.
pub fn decode_lane_slot0(word: U256) -> LaneSlot0 {
    LaneSlot0 {
        price: read_field(word, 0, 112),
        ask_fee_bps: read_field(word, 112, 20),
        bid_fee_bps: read_field(word, 132, 20),
        price_push_threshold: read_field(word, 152, 7),
        threshold_enabled: read_field(word, 159, 1) == U256::ONE,
        latest_update_block: read_field(word, 160, 40),
        reserved_high_bits: word >> 200,
    }
}
#[inline(always)]
/// Reads the 112-bit pushed price from a packed slot word.
pub fn lane_slot0_price(word: U256) -> U256 {
    read_field(word, 0, 112)
}
#[inline(always)]
/// Reads the 20-bit ask fee from a packed slot word.
pub fn lane_slot0_ask_fee_bps(word: U256) -> U256 {
    read_field(word, 112, 20)
}
#[inline(always)]
/// Reads the 20-bit bid fee from a packed slot word.
pub fn lane_slot0_bid_fee_bps(word: U256) -> U256 {
    read_field(word, 132, 20)
}
#[inline(always)]
/// Reads the 40-bit EVM block number of the last lane update.
pub fn lane_slot0_latest_update_block(word: U256) -> U256 {
    read_field(word, 160, 40)
}
/// Encodes a [`LaneSlot0`] view into the exact 256-bit storage word.
///
/// Every bounded field is validated before shifting. In particular, the
/// reserved high field is not discarded; callers can round-trip a word and
/// update only the fields that Solidity's lane update writes.
///
/// # Errors
///
/// Returns [`MathError::FieldOverflow`] if any field exceeds its Solidity
/// storage width.
pub fn encode_lane_slot0(fields: &LaneSlot0) -> Result<U256, MathError> {
    validate_field(fields.price, 112, "price")?;
    validate_field(fields.ask_fee_bps, 20, "askFeeBps")?;
    validate_field(fields.bid_fee_bps, 20, "bidFeeBps")?;
    validate_field(fields.price_push_threshold, 7, "pricePushThreshold")?;
    validate_field(fields.latest_update_block, 40, "latestUpdateBlock")?;
    validate_field(fields.reserved_high_bits, 56, "reservedHighBits")?;
    let mut word = fields.price
        | (fields.ask_fee_bps << 112)
        | (fields.bid_fee_bps << 132)
        | (fields.price_push_threshold << 152);
    if fields.threshold_enabled {
        word |= U256::ONE << 159;
    }
    word |= fields.latest_update_block << 160;
    word |= fields.reserved_high_bits << 200;
    Ok(word)
}
/// Packs the two `uint20` update fees into the contract's `uint40` calldata
/// field: ask fee occupies the low 20 bits and bid fee the high 20 bits.
///
/// # Errors
///
/// Returns [`MathError::FieldOverflow`] when either fee does not fit `uint20`.
pub fn encode_update_fees(ask_fee_bps: U256, bid_fee_bps: U256) -> Result<U256, MathError> {
    validate_field(ask_fee_bps, 20, "askFeeBps")?;
    validate_field(bid_fee_bps, 20, "bidFeeBps")?;
    Ok(ask_fee_bps | (bid_fee_bps << 20))
}
/// Unpacks a `uint40` fee field into `(ask_fee_bps, bid_fee_bps)`.
///
/// # Errors
///
/// Returns [`MathError::FieldOverflow`] when `fees` contains bits above the
/// declared 40-bit calldata width.
pub fn decode_update_fees(fees: U256) -> Result<(U256, U256), MathError> {
    validate_field(fees, 40, "fees")?;
    Ok((fees & field_mask(20), fees >> 20))
}
/// Applies the Solidity `update_0x01e44214` write to a previous slot word.
///
/// Price, ask fee, bid fee, and `latestUpdateBlock` are replaced. Threshold
/// fields and all reserved high bits are copied from `previous`, matching the
/// contract's packed read-modify-write behavior.
///
/// # Errors
///
/// Returns [`MathError::FieldOverflow`] for values outside the `uint112`,
/// `uint40`, or `uint40` block-number widths.
pub fn apply_lane_update_slot0(
    previous: U256,
    price: U256,
    fees: U256,
    block_number: U256,
) -> Result<U256, MathError> {
    validate_field(price, 112, "price")?;
    validate_field(fees, 40, "fees")?;
    validate_field(block_number, 40, "blockNumber")?;
    let (ask_fee_bps, bid_fee_bps) = decode_update_fees(fees)?;
    let mut fields = decode_lane_slot0(previous);
    fields.price = price;
    fields.ask_fee_bps = ask_fee_bps;
    fields.bid_fee_bps = bid_fee_bps;
    fields.latest_update_block = block_number;
    encode_lane_slot0(&fields)
}
