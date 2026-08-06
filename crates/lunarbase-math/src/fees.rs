//! Pure fee, spread, and slippage formulas used by the quote engine.

use crate::arithmetic::{
    BPS, MAX_SLIPPAGE_BPS, SLIPPAGE_SCALE, WAD, ceil_div, checked_add, checked_sub,
    full_mul_div_down, full_mul_div_up, mul_div_down_256,
};
use crate::types::{MathError, U256};

/// Converts an exact-in amount through one lane using the pushed WAD price.
///
/// For `cash_to_asset`, computes `floor(amount_in * WAD / price)`; for the
/// reverse direction, computes `floor(amount_in * price / WAD)`. A zero price
/// returns zero before division, matching the quote engine's unavailable-path
/// convention.
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
/// Converts an exact-out target through one lane with upward rounding.
///
/// For `cash_to_asset`, computes `ceil(amount_out * price / WAD)`; for the
/// reverse direction, computes `ceil(amount_out * WAD / price)`. A zero price
/// returns zero before division.
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
/// Computes one lane's principal-based slippage in protocol BPS.
///
/// The contract first performs a full-width ceil, then rounds up by
/// `SLIPPAGE_SCALE`, and finally caps the result at `MAX_SLIPPAGE_BPS`. Any
/// zero input short-circuits to zero.
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
/// Computes the weighted route slippage K using two independent ceils.
///
/// The two terms are intentionally not combined before rounding. The result
/// is capped at `BPS`, and a zero total principal returns zero.
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
/// Applies whitelist and blacklist multiplier rules to a raw lane fee.
///
/// Raw fees are first clamped to `BPS`. Whitelisted routers keep that value;
/// other routers receive the checked multiplier, also capped at `BPS`. A zero
/// blacklist multiplier therefore produces a zero effective fee.
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
/// Computes the exact-in fee from an anchor using upward full-width rounding.
///
/// The denominator is `BPS + fee_bps`, which preserves the contract's fee
/// convention where the fee is taken from the grossed-up input.
pub fn quote_lane_exact_in_fee(anchor: U256, fee_bps: U256) -> Result<U256, MathError> {
    if anchor == U256::ZERO || fee_bps == U256::ZERO {
        return Ok(U256::ZERO);
    }
    full_mul_div_up(anchor, fee_bps, checked_add(BPS, fee_bps)?)
}
/// Computes the exact-out fee as `ceil(anchor * fee_bps / BPS)`.
pub fn quote_lane_exact_out_fee(anchor: U256, fee_bps: U256) -> Result<U256, MathError> {
    if anchor == U256::ZERO || fee_bps == U256::ZERO {
        return Ok(U256::ZERO);
    }
    full_mul_div_up(anchor, fee_bps, BPS)
}
/// Computes the exact-in route fee after adding the bid and ask fee legs.
pub fn quote_lane_route_exact_in_fee(
    anchor: U256,
    bid_fee_bps: U256,
    ask_fee_bps: U256,
) -> Result<U256, MathError> {
    quote_lane_exact_in_fee(anchor, checked_add(bid_fee_bps, ask_fee_bps)?)
}
/// Computes the exact-out route fee after adding the bid and ask fee legs.
pub fn quote_lane_route_exact_out_fee(
    anchor: U256,
    bid_fee_bps: U256,
    ask_fee_bps: U256,
) -> Result<U256, MathError> {
    quote_lane_exact_out_fee(anchor, checked_add(bid_fee_bps, ask_fee_bps)?)
}
/// Splits a calculated fee into partner and treasury portions.
///
/// The partner amount is `floor(fee_amount * partner_fee_bps / BPS)` using
/// the checked-256 multiplication primitive. The treasury receives the
/// checked remainder, so both outputs sum to `fee_amount`.
pub fn split_fee(fee_amount: U256, partner_fee_bps: U256) -> Result<(U256, U256), MathError> {
    if fee_amount == U256::ZERO {
        return Ok((U256::ZERO, U256::ZERO));
    }
    let partner = if partner_fee_bps == U256::ZERO {
        U256::ZERO
    } else {
        mul_div_down_256(fee_amount, partner_fee_bps, BPS)?
    };
    Ok((partner, checked_sub(fee_amount, partner)?))
}
