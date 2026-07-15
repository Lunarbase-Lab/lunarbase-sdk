use crate::arithmetic::{checked_add, checked_sub};
use crate::fees::{
    calculate_fee_bps_for_router, quote_lane_exact_in, quote_lane_exact_in_fee,
    quote_lane_exact_out, quote_lane_exact_out_fee, quote_lane_slippage_bps,
    quote_lane_weighted_slippage_k_bps, split_fee,
};
use crate::slot0::{
    lane_slot0_ask_fee_bps, lane_slot0_bid_fee_bps, lane_slot0_latest_update_block,
    lane_slot0_price,
};
use crate::{
    Address, LaneState, MathError, QuoteContext, QuoteError, QuoteMode, QuoteOutcome, QuoteRequest,
    QuoteResult, QuoteState, UnavailableReason, U256,
};

fn lane_or_reason<'a>(
    state: &'a QuoteState,
    asset: Address,
    context: &QuoteContext,
) -> Result<&'a LaneState, UnavailableReason> {
    let lane = state
        .lanes
        .get(&asset)
        .ok_or(UnavailableReason::MissingLane(asset))?;
    if !lane.exists {
        return Err(UnavailableReason::MissingLane(asset));
    }
    if lane.paused {
        return Err(UnavailableReason::PausedLane(asset));
    }
    let ready_at = lane_slot0_latest_update_block(lane.slot0)
        .checked_add(U256::from(lane.block_delay))
        .unwrap_or(U256::MAX);
    if context.execution_block_number < ready_at {
        return Err(UnavailableReason::DelayedLane(asset));
    }
    if lane_slot0_price(lane.slot0) == U256::ZERO {
        return Err(UnavailableReason::ZeroPrice(asset));
    }
    Ok(lane)
}
fn principal_cash_value(
    state: &QuoteState,
    asset: Address,
    lane: &LaneState,
) -> Result<U256, MathError> {
    let principal = state
        .total_principal_amount
        .get(&asset)
        .copied()
        .unwrap_or(U256::ZERO);
    if principal == U256::ZERO {
        return Ok(U256::ZERO);
    }
    quote_lane_exact_in(lane_slot0_price(lane.slot0), principal, false)
}
fn partner_fee(state: &QuoteState, router: Address, asset: Address) -> U256 {
    state
        .partner_fee_bps
        .get(&(router, asset))
        .copied()
        .unwrap_or(U256::ZERO)
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
    let (partner, treasury) =
        split_fee(anchor, fee, partner_fee(state, request.router, fee_asset))?;
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
    let principal = principal_cash_value(state, lane_asset, lane)?;
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
        state
            .whitelist
            .get(&request.router)
            .copied()
            .unwrap_or(false),
        state.blacklist_fee_multiplier,
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
    let slippage = quote_lane_slippage_bps(swap_cash, principal, U256::from(lane.slippage_k_bps))?;
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
    let first_principal = principal_cash_value(state, request.asset_in, input_lane)?;
    if first_principal == U256::ZERO {
        return Ok(QuoteOutcome::Unavailable(UnavailableReason::ZeroPrincipal(
            request.asset_in,
        )));
    }
    let second_principal = principal_cash_value(state, request.asset_out, output_lane)?;
    if second_principal == U256::ZERO {
        return Ok(QuoteOutcome::Unavailable(UnavailableReason::ZeroPrincipal(
            request.asset_out,
        )));
    }
    let weighted_k = quote_lane_weighted_slippage_k_bps(
        first_principal,
        U256::from(input_lane.slippage_k_bps),
        second_principal,
        U256::from(output_lane.slippage_k_bps),
    )?;
    let slippage = quote_lane_slippage_bps(
        intermediate_cash,
        checked_add(first_principal, second_principal)?,
        weighted_k,
    )?;
    let whitelisted = state
        .whitelist
        .get(&request.router)
        .copied()
        .unwrap_or(false);
    let bid = calculate_fee_bps_for_router(
        whitelisted,
        state.blacklist_fee_multiplier,
        lane_slot0_bid_fee_bps(input_lane.slot0),
    )?;
    let ask = calculate_fee_bps_for_router(
        whitelisted,
        state.blacklist_fee_multiplier,
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
fn validate_context(state: &QuoteState, context: &QuoteContext) -> Result<(), QuoteError> {
    if state.cash != context.cash {
        return Err(QuoteError::CashMismatch);
    }
    if state.state_version != context.state_version {
        return Err(QuoteError::StateVersionMismatch);
    }
    Ok(())
}
pub fn quote(
    request: &QuoteRequest,
    context: &QuoteContext,
    state: &QuoteState,
) -> Result<QuoteOutcome, QuoteError> {
    validate_context(state, context)?;
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
    if request.asset_out == context.cash {
        let lane = lane_or_reason(state, request.asset_in, context);
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
    if request.asset_in == context.cash {
        let lane = lane_or_reason(state, request.asset_out, context);
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
    let input_lane = match lane_or_reason(state, request.asset_in, context) {
        Ok(lane) => lane,
        Err(reason) => return Ok(QuoteOutcome::Unavailable(reason)),
    };
    let output_lane = match lane_or_reason(state, request.asset_out, context) {
        Ok(lane) => lane,
        Err(reason) => return Ok(QuoteOutcome::Unavailable(reason)),
    };
    Ok(quote_route(state, request, input_lane, output_lane)?)
}
pub fn quote_exact_in(
    request: &QuoteRequest,
    context: &QuoteContext,
    state: &QuoteState,
) -> Result<QuoteOutcome, QuoteError> {
    let mut request = request.clone();
    request.mode = QuoteMode::ExactIn;
    quote(&request, context, state)
}
pub fn quote_exact_out(
    request: &QuoteRequest,
    context: &QuoteContext,
    state: &QuoteState,
) -> Result<QuoteOutcome, QuoteError> {
    let mut request = request.clone();
    request.mode = QuoteMode::ExactOut;
    quote(&request, context, state)
}
pub fn solidity_exact_in_amount(outcome: &QuoteOutcome) -> U256 {
    match outcome {
        QuoteOutcome::Available(result) => result.amount_out,
        QuoteOutcome::Unavailable(_) => U256::ZERO,
    }
}
pub fn solidity_exact_out_amount(outcome: &QuoteOutcome) -> U256 {
    match outcome {
        QuoteOutcome::Available(result) => result.amount_in,
        QuoteOutcome::Unavailable(_) => U256::MAX,
    }
}
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
