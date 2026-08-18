//! Quote-critical event mutations kept separate from cursor publication.

use super::{QuoteReducer, ReducerError};
use crate::model::QuoteEvent;
use lunarbase_math::U256;
use lunarbase_math::arithmetic::BPS;
use lunarbase_math::slot0::{
    set_lane_slot0_block_delay, set_lane_slot0_exists, set_lane_slot0_paused,
    set_lane_slot0_price_push_threshold, set_lane_slot0_slippage_k_bps,
};
use std::sync::Arc;

impl QuoteReducer {
    pub(super) fn apply_event(&mut self, event: QuoteEvent) -> Result<bool, ReducerError> {
        match event {
            QuoteEvent::LaneAdded { asset } => {
                if self.verified_router.is_some() {
                    return Err(ReducerError::VerifiedRouterRefreshRequired);
                }
                let previous = self
                    .state
                    .lanes
                    .get(&asset)
                    .map_or_else(Default::default, |lane| lane.slot0);
                let slot0 = set_lane_slot0_exists(previous, true);
                let next = set_lane_slot0_paused(slot0, true);
                if next == previous {
                    return Ok(false);
                }
                self.state_mut().lanes.entry(asset).or_default().slot0 = next;
            }
            QuoteEvent::LaneRemoved { asset } => {
                let state_changed = self.state.lanes.contains_key(&asset);
                let router_changed = self
                    .verified_router
                    .as_ref()
                    .is_some_and(|verified| verified.partner_fee_bps.contains_key(&asset));
                if state_changed {
                    self.state_mut().lanes.remove(&asset);
                }
                if router_changed {
                    Arc::make_mut(
                        self.verified_router
                            .as_mut()
                            .expect("verified router checked before mutation"),
                    )
                    .partner_fee_bps
                    .remove(&asset);
                }
                return Ok(state_changed || router_changed);
            }
            QuoteEvent::LaneUpdated { asset, slot0 } => {
                let previous = self
                    .state
                    .lanes
                    .get(&asset)
                    .ok_or(ReducerError::UnknownLane)?
                    .slot0;
                if previous == slot0 {
                    return Ok(false);
                }
                self.lane_mut(asset)?.slot0 = slot0;
            }
            QuoteEvent::SlippageKSet { asset, new_k } => {
                if U256::from(new_k) > BPS {
                    return Err(ReducerError::InvalidSlippageK);
                }
                let previous = self
                    .state
                    .lanes
                    .get(&asset)
                    .ok_or(ReducerError::UnknownLane)?
                    .slot0;
                let next = set_lane_slot0_slippage_k_bps(previous, new_k);
                if next == previous {
                    return Ok(false);
                }
                self.lane_mut(asset)?.slot0 = next;
            }
            QuoteEvent::LanePausedSet { asset, paused } => {
                let previous = self
                    .state
                    .lanes
                    .get(&asset)
                    .ok_or(ReducerError::UnknownLane)?
                    .slot0;
                let next = set_lane_slot0_paused(previous, paused);
                if next == previous {
                    return Ok(false);
                }
                self.lane_mut(asset)?.slot0 = next;
            }
            QuoteEvent::PricePushThresholdSet {
                asset,
                price_push_threshold,
                enabled,
            } => {
                let previous = self
                    .state
                    .lanes
                    .get(&asset)
                    .ok_or(ReducerError::UnknownLane)?
                    .slot0;
                let next =
                    set_lane_slot0_price_push_threshold(previous, price_push_threshold, enabled)
                        .map_err(|_| ReducerError::InvalidWidth)?;
                if next == previous {
                    return Ok(false);
                }
                self.lane_mut(asset)?.slot0 = next;
            }
            QuoteEvent::BlockDelaySet { asset, block_delay } => {
                let previous = self
                    .state
                    .lanes
                    .get(&asset)
                    .ok_or(ReducerError::UnknownLane)?
                    .slot0;
                let next = set_lane_slot0_block_delay(previous, block_delay);
                if next == previous {
                    return Ok(false);
                }
                self.lane_mut(asset)?.slot0 = next;
            }
            QuoteEvent::PartnerInfoSet { router, asset, fee }
            | QuoteEvent::PartnerFeeSet { router, asset, fee } => {
                let Some(verified) = self.verified_router.as_ref() else {
                    return Ok(false);
                };
                if router != verified.router
                    || (asset != self.state.cash && !self.state.lanes.contains_key(&asset))
                {
                    return Ok(false);
                }
                if U256::from(fee) > BPS {
                    return Err(ReducerError::InvalidWidth);
                }
                if verified.partner_fee_bps.get(&asset) == Some(&fee) {
                    return Ok(false);
                }
                let previous = Arc::make_mut(
                    self.verified_router
                        .as_mut()
                        .expect("verified router checked before mutation"),
                )
                .partner_fee_bps
                .insert(asset, fee);
                return Ok(previous != Some(fee));
            }
            QuoteEvent::WhitelistSet {
                router,
                whitelisted,
            } => {
                if self
                    .verified_router
                    .as_ref()
                    .is_some_and(|verified| verified.router == router)
                    && whitelisted != self.fee_class.is_whitelisted()
                {
                    return Err(ReducerError::FeeClassMismatch);
                }
                return Ok(false);
            }
            QuoteEvent::BlacklistFeeMultiplierSet { multiplier } => {
                if self.state.blacklist_fee_multiplier == multiplier {
                    return Ok(false);
                }
                self.state_mut().blacklist_fee_multiplier = multiplier;
            }
            QuoteEvent::DepositExecuted { asset, principal } => {
                let previous = self
                    .state
                    .lanes
                    .get(&asset)
                    .ok_or(ReducerError::UnknownLane)?
                    .total_principal_amount;
                let next = previous
                    .checked_add(principal)
                    .ok_or(ReducerError::Arithmetic)?;
                if next == previous {
                    return Ok(false);
                }
                self.lane_mut(asset)?.total_principal_amount = next;
            }
            QuoteEvent::WithdrawalExecuted { asset, principal } => {
                let previous = self
                    .state
                    .lanes
                    .get(&asset)
                    .ok_or(ReducerError::UnknownLane)?
                    .total_principal_amount;
                let next = previous
                    .checked_sub(principal)
                    .ok_or(ReducerError::Arithmetic)?;
                if next == previous {
                    return Ok(false);
                }
                self.lane_mut(asset)?.total_principal_amount = next;
            }
            QuoteEvent::Sync {
                asset,
                asset_reserve,
                cash_reserve,
            } => {
                if asset != self.state.cash && !self.state.lanes.contains_key(&asset) {
                    return Err(ReducerError::UnknownLane);
                }
                let changed = self.state.cash_reserve != cash_reserve
                    || (asset != self.state.cash
                        && self
                            .state
                            .lanes
                            .get(&asset)
                            .is_some_and(|lane| lane.asset_reserve != asset_reserve));
                if !changed {
                    return Ok(false);
                }
                let state = self.state_mut();
                state.cash_reserve = cash_reserve;
                if asset != state.cash {
                    state
                        .lanes
                        .get_mut(&asset)
                        .expect("lane identity validated before state mutation")
                        .asset_reserve = asset_reserve;
                }
            }
            QuoteEvent::ImplementationUpgraded { .. } => {
                return Err(ReducerError::ImplementationUpgraded);
            }
        }
        Ok(true)
    }
}
