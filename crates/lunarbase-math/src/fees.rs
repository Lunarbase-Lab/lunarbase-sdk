use crate::arithmetic::{
    ceil_div, checked_add, checked_sub, full_mul_div_down, full_mul_div_up, mul_div_down_256, BPS,
    MAX_SLIPPAGE_BPS, SLIPPAGE_SCALE, WAD,
};
use crate::{MathError, U256};

pub fn quote_lane_exact_in(
    price: U256,
    amount_in: U256,
    cash_to_asset: bool,
) -> Result<U256, MathError> {
    if price == U256::ZERO {
        return Ok(U256::ZERO);
    }
    if cash_to_asset {
        full_mul_div_down(amount_in, WAD, price)
    } else {
        full_mul_div_down(amount_in, price, WAD)
    }
}
pub fn quote_lane_exact_out(
    price: U256,
    amount_out: U256,
    cash_to_asset: bool,
) -> Result<U256, MathError> {
    if price == U256::ZERO {
        return Ok(U256::ZERO);
    }
    if cash_to_asset {
        full_mul_div_up(amount_out, price, WAD)
    } else {
        full_mul_div_up(amount_out, WAD, price)
    }
}
pub fn quote_lane_slippage_bps(
    swap_cash_value: U256,
    principal_cash_value: U256,
    slippage_k_bps: U256,
) -> Result<U256, MathError> {
    if swap_cash_value == U256::ZERO
        || principal_cash_value == U256::ZERO
        || slippage_k_bps == U256::ZERO
    {
        return Ok(U256::ZERO);
    }
    let raw = full_mul_div_up(swap_cash_value, slippage_k_bps, principal_cash_value)?;
    Ok(ceil_div(raw, SLIPPAGE_SCALE)?.min(MAX_SLIPPAGE_BPS))
}
pub fn quote_lane_weighted_slippage_k_bps(
    first_principal: U256,
    first_k: U256,
    second_principal: U256,
    second_k: U256,
) -> Result<U256, MathError> {
    let total = checked_add(first_principal, second_principal)?;
    if total == U256::ZERO {
        return Ok(U256::ZERO);
    }
    let first = full_mul_div_up(first_principal, first_k, total)?;
    let second = full_mul_div_up(second_principal, second_k, total)?;
    Ok(checked_add(first, second)?.min(BPS))
}
pub fn calculate_fee_bps_for_router(
    whitelisted: bool,
    blacklist_fee_multiplier: U256,
    fee_bps: U256,
) -> Result<U256, MathError> {
    let fee = fee_bps.min(BPS);
    if whitelisted {
        return Ok(fee);
    }
    if blacklist_fee_multiplier != U256::ZERO && fee > BPS / blacklist_fee_multiplier {
        return Ok(BPS);
    }
    Ok(checked_mul_saturating(fee, blacklist_fee_multiplier)?.min(BPS))
}
fn checked_mul_saturating(x: U256, y: U256) -> Result<U256, MathError> {
    x.checked_mul(y).ok_or(MathError::Overflow)
}
pub fn quote_lane_exact_in_fee(anchor: U256, fee_bps: U256) -> Result<U256, MathError> {
    if anchor == U256::ZERO || fee_bps == U256::ZERO {
        return Ok(U256::ZERO);
    }
    full_mul_div_up(anchor, fee_bps, checked_add(BPS, fee_bps)?)
}
pub fn quote_lane_exact_out_fee(anchor: U256, fee_bps: U256) -> Result<U256, MathError> {
    if anchor == U256::ZERO || fee_bps == U256::ZERO {
        return Ok(U256::ZERO);
    }
    full_mul_div_up(anchor, fee_bps, BPS)
}
pub fn quote_lane_route_exact_in_fee(
    anchor: U256,
    bid_fee_bps: U256,
    ask_fee_bps: U256,
) -> Result<U256, MathError> {
    quote_lane_exact_in_fee(anchor, checked_add(bid_fee_bps, ask_fee_bps)?)
}
pub fn quote_lane_route_exact_out_fee(
    anchor: U256,
    bid_fee_bps: U256,
    ask_fee_bps: U256,
) -> Result<U256, MathError> {
    quote_lane_exact_out_fee(anchor, checked_add(bid_fee_bps, ask_fee_bps)?)
}
pub fn split_fee(
    anchor: U256,
    fee_amount: U256,
    partner_fee_bps: U256,
) -> Result<(U256, U256), MathError> {
    if fee_amount == U256::ZERO {
        return Ok((U256::ZERO, U256::ZERO));
    }
    let candidate = if partner_fee_bps == U256::ZERO {
        U256::ZERO
    } else {
        mul_div_down_256(anchor, partner_fee_bps, BPS)?
    };
    let partner = candidate.min(fee_amount);
    Ok((partner, checked_sub(fee_amount, partner)?))
}
