use crate::{MathError, U256};

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
pub fn lane_slot0_price(word: U256) -> U256 {
    read_field(word, 0, 112)
}
#[inline(always)]
pub fn lane_slot0_ask_fee_bps(word: U256) -> U256 {
    read_field(word, 112, 20)
}
#[inline(always)]
pub fn lane_slot0_bid_fee_bps(word: U256) -> U256 {
    read_field(word, 132, 20)
}
#[inline(always)]
pub fn lane_slot0_latest_update_block(word: U256) -> U256 {
    read_field(word, 160, 40)
}
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
pub fn encode_update_fees(ask_fee_bps: U256, bid_fee_bps: U256) -> Result<U256, MathError> {
    validate_field(ask_fee_bps, 20, "askFeeBps")?;
    validate_field(bid_fee_bps, 20, "bidFeeBps")?;
    Ok(ask_fee_bps | (bid_fee_bps << 20))
}
pub fn decode_update_fees(fees: U256) -> Result<(U256, U256), MathError> {
    validate_field(fees, 40, "fees")?;
    Ok((fees & field_mask(20), fees >> 20))
}
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
