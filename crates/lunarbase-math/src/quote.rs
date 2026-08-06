//! High-level direct and routed quote evaluation.

use crate::arithmetic::{checked_add, checked_sub};
use crate::fees::{
    calculate_fee_bps_for_router, quote_lane_exact_in, quote_lane_exact_in_fee,
    quote_lane_exact_out, quote_lane_exact_out_fee, quote_lane_slippage_bps,
    quote_lane_weighted_slippage_k_bps, split_fee,
};
use crate::slot0::{
    lane_slot0_ask_fee_bps, lane_slot0_bid_fee_bps, lane_slot0_block_delay,
    lane_slot0_latest_update_block, lane_slot0_price, lane_slot0_slippage_k_bps,
};
use crate::state::{
    LaneState, QuoteError, QuoteMode, QuoteOutcome, QuoteRequest, QuoteResult, QuoteState,
    UnavailableReason,
};
use crate::types::{Address, MathError, U256};

fn lane_or_reason(
    state: &QuoteState,
    asset: Address,
    execution_block_number: u64,
) -> Result<&LaneState, UnavailableReason> {
    let lane = state
        .lanes
        .get(&asset)
        .ok_or(UnavailableReason::MissingLane(asset))?;
    if !lane.exists() {
        return Err(UnavailableReason::MissingLane(asset));
    }
    if lane.paused() {
        return Err(UnavailableReason::PausedLane(asset));
    }
    let expires_at = lane_slot0_latest_update_block(lane.slot0)
        .saturating_add(u64::from(lane_slot0_block_delay(lane.slot0)));
    if execution_block_number > expires_at {
        return Err(UnavailableReason::StaleLane(asset));
    }
    Ok(lane)
}
fn principal_cash_value(lane: &LaneState) -> Result<U256, MathError> {
    let principal = U256::from(lane.total_principal_amount);
    if principal == U256::ZERO {
        return Ok(U256::ZERO);
    }
    quote_lane_exact_in(lane_slot0_price(lane.slot0), principal, false)
}
fn lane_spread(
    anchor: U256,
    fee_bps: U256,
    slippage_bps: U256,
    exact_in: bool,
) -> Result<(U256, U256), MathError> {
    let fee = if exact_in {
        quote_lane_exact_in_fee(anchor, fee_bps)?
    } else {
        quote_lane_exact_out_fee(anchor, fee_bps)?
    };
    let total_bps = checked_add(fee_bps, slippage_bps)?;
    let total = if exact_in {
        quote_lane_exact_in_fee(anchor, total_bps)?
    } else {
        quote_lane_exact_out_fee(anchor, total_bps)?
    };
    Ok((fee, checked_sub(total, fee)?))
}
fn assemble_quote(
    state: &QuoteState,
    request: &QuoteRequest,
    fee_asset: Address,
    anchor: U256,
    fee: U256,
    slippage_amount: U256,
) -> Result<QuoteOutcome, MathError> {
    let total_spread = checked_add(fee, slippage_amount)?;
    let (amount_in, amount_out) = match request.mode {
        QuoteMode::ExactIn => {
            if total_spread >= anchor {
                return Ok(QuoteOutcome::Unavailable(
                    UnavailableReason::SpreadConsumesAnchor,
                ));
            }
            (request.amount, checked_sub(anchor, total_spread)?)
        }
        QuoteMode::ExactOut => (checked_add(anchor, total_spread)?, request.amount),
    };
    let output_reserve = if request.asset_out == state.cash {
        state.cash_reserve
    } else {
        match state.lanes.get(&request.asset_out) {
            Some(lane) => lane.asset_reserve,
            None => {
                return Ok(QuoteOutcome::Unavailable(UnavailableReason::MissingLane(
                    request.asset_out,
                )));
            }
        }
    };
    let output_reserve = U256::from(output_reserve);
    let insufficient = amount_out > output_reserve
        || (request.mode == QuoteMode::ExactIn && fee > checked_sub(output_reserve, amount_out)?);
    if insufficient {
        return Ok(QuoteOutcome::Unavailable(
            UnavailableReason::InsufficientOutputReserve(request.asset_out),
        ));
    }
    let (partner, treasury) = split_fee(fee, state.fee_profile.partner_fee_bps(fee_asset))?;
    Ok(QuoteOutcome::Available(QuoteResult {
        amount_in,
        amount_out,
        fee_asset,
        fee_amount: fee,
        partner_fee: partner,
        treasury_fee: treasury,
    }))
}
fn quote_direct(
    state: &QuoteState,
    request: &QuoteRequest,
    lane_asset: Address,
    lane: &LaneState,
    cash_to_asset: bool,
    fee_asset: Address,
) -> Result<QuoteOutcome, MathError> {
    let anchor = match request.mode {
        QuoteMode::ExactIn => {
            quote_lane_exact_in(lane_slot0_price(lane.slot0), request.amount, cash_to_asset)?
        }
        QuoteMode::ExactOut => {
            quote_lane_exact_out(lane_slot0_price(lane.slot0), request.amount, cash_to_asset)?
        }
    };
    if anchor == U256::ZERO {
        return Ok(QuoteOutcome::Unavailable(UnavailableReason::ZeroAnchor));
    }
    let principal = principal_cash_value(lane)?;
    if principal == U256::ZERO {
        return Ok(QuoteOutcome::Unavailable(UnavailableReason::ZeroPrincipal(
            lane_asset,
        )));
    }
    let raw_fee = if cash_to_asset {
        lane_slot0_ask_fee_bps(lane.slot0)
    } else {
        lane_slot0_bid_fee_bps(lane.slot0)
    };
    let fee_bps = calculate_fee_bps_for_router(
        state.fee_profile.whitelisted,
        state.fee_profile.blacklist_fee_multiplier,
        raw_fee,
    )?;
    let swap_cash = if cash_to_asset {
        if request.mode == QuoteMode::ExactIn {
            request.amount
        } else {
            anchor
        }
    } else if request.mode == QuoteMode::ExactIn {
        anchor
    } else {
        request.amount
    };
    let slippage = quote_lane_slippage_bps(
        swap_cash,
        principal,
        U256::from(lane_slot0_slippage_k_bps(lane.slot0)),
    )?;
    let (fee, slippage_amount) = lane_spread(
        anchor,
        fee_bps,
        slippage,
        request.mode == QuoteMode::ExactIn,
    )?;
    assemble_quote(state, request, fee_asset, anchor, fee, slippage_amount)
}
fn quote_route(
    state: &QuoteState,
    request: &QuoteRequest,
    input_lane: &LaneState,
    output_lane: &LaneState,
) -> Result<QuoteOutcome, MathError> {
    let (intermediate_cash, anchor) = match request.mode {
        QuoteMode::ExactIn => {
            let intermediate =
                quote_lane_exact_in(lane_slot0_price(input_lane.slot0), request.amount, false)?;
            (
                intermediate,
                quote_lane_exact_in(lane_slot0_price(output_lane.slot0), intermediate, true)?,
            )
        }
        QuoteMode::ExactOut => {
            let intermediate =
                quote_lane_exact_out(lane_slot0_price(output_lane.slot0), request.amount, true)?;
            (
                intermediate,
                quote_lane_exact_out(lane_slot0_price(input_lane.slot0), intermediate, false)?,
            )
        }
    };
    if anchor == U256::ZERO {
        return Ok(QuoteOutcome::Unavailable(UnavailableReason::ZeroAnchor));
    }
    let first_principal = principal_cash_value(input_lane)?;
    if first_principal == U256::ZERO {
        return Ok(QuoteOutcome::Unavailable(UnavailableReason::ZeroPrincipal(
            request.asset_in,
        )));
    }
    let second_principal = principal_cash_value(output_lane)?;
    if second_principal == U256::ZERO {
        return Ok(QuoteOutcome::Unavailable(UnavailableReason::ZeroPrincipal(
            request.asset_out,
        )));
    }
    let weighted_k = quote_lane_weighted_slippage_k_bps(
        first_principal,
        U256::from(lane_slot0_slippage_k_bps(input_lane.slot0)),
        second_principal,
        U256::from(lane_slot0_slippage_k_bps(output_lane.slot0)),
    )?;
    let slippage = quote_lane_slippage_bps(
        intermediate_cash,
        checked_add(first_principal, second_principal)?,
        weighted_k,
    )?;
    let whitelisted = state.fee_profile.whitelisted;
    let bid = calculate_fee_bps_for_router(
        whitelisted,
        state.fee_profile.blacklist_fee_multiplier,
        lane_slot0_bid_fee_bps(input_lane.slot0),
    )?;
    let ask = calculate_fee_bps_for_router(
        whitelisted,
        state.fee_profile.blacklist_fee_multiplier,
        lane_slot0_ask_fee_bps(output_lane.slot0),
    )?;
    let fee_bps = checked_add(bid, ask)?;
    let fee = if request.mode == QuoteMode::ExactIn {
        quote_lane_exact_in_fee(anchor, fee_bps)?
    } else {
        quote_lane_exact_out_fee(anchor, fee_bps)?
    };
    let total_bps = checked_add(checked_add(bid, slippage)?, checked_add(ask, slippage)?)?;
    let total = if request.mode == QuoteMode::ExactIn {
        quote_lane_exact_in_fee(anchor, total_bps)?
    } else {
        quote_lane_exact_out_fee(anchor, total_bps)?
    };
    let fee_asset = if request.mode == QuoteMode::ExactIn {
        request.asset_out
    } else {
        request.asset_in
    };
    assemble_quote(
        state,
        request,
        fee_asset,
        anchor,
        fee,
        checked_sub(total, fee)?,
    )
}
/// Calculates a complete quote for a direct lane or a two-leg CASH route.
///
/// The function applies the contract's zero/equal-asset sentinels, lane
/// validity predicate, anchor conversion, configured fee profile,
/// principal-based slippage, spread, output-reserve availability, and
/// partner/treasury split.
///
/// `execution_block_number` must be the EVM-visible block number supplied by
/// the runtime's normalized head, not an arbitrary provider sequence.
///
/// # Errors
///
/// Returns [`QuoteError::Arithmetic`] when Solidity's checked arithmetic
/// boundary is exceeded.
pub fn quote(
    request: &QuoteRequest,
    execution_block_number: u64,
    state: &QuoteState,
) -> Result<QuoteOutcome, QuoteError> {
    if request.amount == U256::ZERO {
        return Ok(QuoteOutcome::Unavailable(UnavailableReason::ZeroAmount));
    }
    if request.asset_in == request.asset_out {
        return Ok(QuoteOutcome::Unavailable(UnavailableReason::EqualAssets));
    }
    let fee_asset = if request.mode == QuoteMode::ExactIn {
        request.asset_out
    } else {
        request.asset_in
    };
    if request.asset_out == state.cash {
        let lane = lane_or_reason(state, request.asset_in, execution_block_number);
        return match lane {
            Ok(lane) => Ok(quote_direct(
                state,
                request,
                request.asset_in,
                lane,
                false,
                fee_asset,
            )?),
            Err(reason) => Ok(QuoteOutcome::Unavailable(reason)),
        };
    }
    if request.asset_in == state.cash {
        let lane = lane_or_reason(state, request.asset_out, execution_block_number);
        return match lane {
            Ok(lane) => Ok(quote_direct(
                state,
                request,
                request.asset_out,
                lane,
                true,
                fee_asset,
            )?),
            Err(reason) => Ok(QuoteOutcome::Unavailable(reason)),
        };
    }
    let input_lane = match lane_or_reason(state, request.asset_in, execution_block_number) {
        Ok(lane) => lane,
        Err(reason) => return Ok(QuoteOutcome::Unavailable(reason)),
    };
    let output_lane = match lane_or_reason(state, request.asset_out, execution_block_number) {
        Ok(lane) => lane,
        Err(reason) => return Ok(QuoteOutcome::Unavailable(reason)),
    };
    Ok(quote_route(state, request, input_lane, output_lane)?)
}
/// Converts a rich quote outcome to `Lanes.quoteExactIn`'s public scalar.
///
/// Available quotes return `amount_out`; every unavailable reason maps to zero.
pub fn solidity_exact_in_amount(outcome: &QuoteOutcome) -> U256 {
    match outcome {
        QuoteOutcome::Available(result) => result.amount_out,
        QuoteOutcome::Unavailable(_) => U256::ZERO,
    }
}
/// Converts a rich quote outcome to `Lanes.quoteExactOut`'s public scalar.
///
/// Available quotes return `amount_in`; unavailable results map to the
/// contract's `U256::MAX` sentinel.
pub fn solidity_exact_out_amount(outcome: &QuoteOutcome) -> U256 {
    match outcome {
        QuoteOutcome::Available(result) => result.amount_in,
        QuoteOutcome::Unavailable(_) => U256::MAX,
    }
}
/// Applies the special zero-request override to the exact-out sentinel.
///
/// Solidity returns zero for a zero requested amount, even though a generic
/// unavailable exact-out outcome normally maps to `U256::MAX`.
pub fn solidity_exact_out_amount_for_request(
    request: &QuoteRequest,
    outcome: &QuoteOutcome,
) -> U256 {
    if request.amount == U256::ZERO {
        U256::ZERO
    } else {
        solidity_exact_out_amount(outcome)
    }
}
