//! Ordered single-writer quote-state reducer.

use crate::model::{
    ChainCursor, Checkpoint, DeploymentConfig, MATH_COMPATIBILITY_VERSION, QuoteEvent,
    SCHEMA_VERSION,
};
use lunarbase_math::arithmetic::BPS;
use lunarbase_math::slot0::{
    set_lane_slot0_block_delay, set_lane_slot0_exists, set_lane_slot0_paused,
    set_lane_slot0_price_push_threshold, set_lane_slot0_slippage_k_bps,
};
use lunarbase_math::{Address, U256};
use lunarbase_math::{LaneState, QuoteState};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Failure that revokes quote readiness until canonical recovery.
pub enum ReducerError {
    /// An update cursor belongs to another EIP-155 chain.
    #[error("cursor chain id mismatch")]
    ChainIdMismatch,
    /// An event precedes the last accepted position in deterministic ordering.
    #[error("cursor regression")]
    CursorRegression,
    /// A provider retracted an applied log, requiring canonical state rebuild.
    #[error("removed log requires canonical rebuild")]
    RemovedLog,
    /// Two non-progressive cursors claim different hashes for one block height.
    #[error("block hash mismatch")]
    BlockHashMismatch,
    /// A state delta would underflow, overflow, or otherwise violate arithmetic invariants.
    #[error("arithmetic transition failed")]
    Arithmetic,
    /// A lane slippage coefficient exceeds the contract's basis-point bound.
    #[error("invalid lane slippage K")]
    InvalidSlippageK,
    /// An event value cannot fit the corresponding compact contract field.
    #[error("event value does not fit the contract storage width")]
    InvalidWidth,
    /// A delta references a lane absent from the coherent bootstrap state.
    #[error("event references an unknown lane")]
    UnknownLane,
    /// The ERC-1967 implementation changed and must be revalidated before quoting.
    #[error("Core implementation upgraded")]
    ImplementationUpgraded,
}

#[derive(Clone, Debug)]
/// Single-writer reducer over quote-critical state.
///
/// Every fallible value is validated before state mutation.
pub struct QuoteReducer {
    /// Complete quote-critical state mutated only by the ordered reducer task.
    state: Arc<QuoteState>,
    /// Only router whose partner fee and whitelist changes affect this instance.
    configured_router: Address,
    /// Last normalized head or event position accepted by the reducer.
    cursor: Option<ChainCursor>,
    /// Fail-closed publication flag cleared on gaps, reorgs, and reducer errors.
    ready: bool,
}

impl QuoteReducer {
    /// Creates a not-ready reducer around a block-tagged state.
    pub fn new(state: QuoteState, configured_router: Address) -> Self {
        Self {
            state: Arc::new(state),
            configured_router,
            cursor: None,
            ready: false,
        }
    }

    /// Restores a ready reducer from a prevalidated checkpoint.
    pub fn from_checkpoint(checkpoint: Checkpoint) -> Self {
        Self {
            state: Arc::new(checkpoint.state),
            configured_router: checkpoint.router,
            cursor: Some(checkpoint.cursor),
            ready: true,
        }
    }

    /// Returns the current immutable state view.
    pub fn state(&self) -> &QuoteState {
        self.state.as_ref()
    }

    /// Clones the immutable state handle without cloning the underlying maps.
    pub(crate) fn state_snapshot(&self) -> Arc<QuoteState> {
        Arc::clone(&self.state)
    }

    /// Returns the last accepted cursor.
    pub fn cursor(&self) -> Option<&ChainCursor> {
        self.cursor.as_ref()
    }

    /// Reports whether the state may be used for quotes.
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Revokes readiness until a canonical recovery succeeds.
    pub fn mark_not_ready(&mut self) {
        self.ready = false;
    }

    /// Publishes the current canonical state.
    pub fn publish_ready(&mut self) {
        self.ready = true;
    }

    /// Installs the initial snapshot cursor.
    pub fn bootstrap(&mut self, cursor: ChainCursor) {
        self.cursor = Some(cursor);
        self.ready = true;
    }

    /// Advances a block-level cursor without changing quote state.
    pub fn observe_head(&mut self, head: ChainCursor) -> Result<(), ReducerError> {
        if let Some(current) = &mut self.cursor {
            if current.chain_id != head.chain_id {
                return Err(ReducerError::ChainIdMismatch);
            }
            if current.block_number == head.block_number
                && current.block_hash.is_some()
                && head.block_hash.is_some()
                && current.block_hash != head.block_hash
                && !is_realtime_progression(current, &head)
            {
                return Err(ReducerError::BlockHashMismatch);
            }
            if head.block_number < current.block_number {
                return Ok(());
            }
            if head.block_number == current.block_number {
                if current.block_hash.is_none() || is_realtime_progression(current, &head) {
                    current.block_hash = head.block_hash;
                }
                current.execution_block_number = head.execution_block_number;
                if head.commitment > current.commitment {
                    current.commitment = head.commitment;
                }
                if head.source_sequence > current.source_sequence {
                    current.source_sequence = head.source_sequence;
                    current.source_sub_index = head.source_sub_index;
                }
                return Ok(());
            }
        }
        self.cursor = Some(head);
        Ok(())
    }

    /// Applies one decoded event after validating ordering and storage widths.
    pub fn apply(&mut self, cursor: ChainCursor, event: QuoteEvent) -> Result<(), ReducerError> {
        if let Some(previous) = &self.cursor {
            if previous.chain_id != cursor.chain_id {
                return Err(ReducerError::ChainIdMismatch);
            }
            if previous.block_number == cursor.block_number
                && previous.block_hash.is_some()
                && cursor.block_hash.is_some()
                && previous.block_hash != cursor.block_hash
                && !is_realtime_progression(previous, &cursor)
            {
                return Err(ReducerError::BlockHashMismatch);
            }
            let previous_is_block_head = previous.block_number == cursor.block_number
                && previous.transaction_index.is_none()
                && previous.log_index.is_none()
                && cursor.transaction_index.is_some()
                && cursor.log_index.is_some();
            if !previous_is_block_head && cursor.event_order() < previous.event_order() {
                return Err(ReducerError::CursorRegression);
            }
            if !previous_is_block_head && cursor.event_order() == previous.event_order() {
                return Ok(());
            }
        }
        self.apply_event(event)?;
        self.cursor = Some(cursor);
        Ok(())
    }

    fn apply_event(&mut self, event: QuoteEvent) -> Result<(), ReducerError> {
        match event {
            QuoteEvent::LaneAdded { asset } => {
                let lane = self.state_mut().lanes.entry(asset).or_default();
                let slot0 = set_lane_slot0_exists(lane.slot0, true);
                lane.slot0 = set_lane_slot0_paused(slot0, true);
            }
            QuoteEvent::LaneRemoved { asset } => {
                self.state_mut().lanes.remove(&asset);
            }
            QuoteEvent::LaneUpdated { asset, slot0 } => {
                self.lane_mut(asset)?.slot0 = slot0;
            }
            QuoteEvent::SlippageKSet { asset, new_k } => {
                if U256::from(new_k) > BPS {
                    return Err(ReducerError::InvalidSlippageK);
                }
                let lane = self.lane_mut(asset)?;
                lane.slot0 = set_lane_slot0_slippage_k_bps(lane.slot0, new_k);
            }
            QuoteEvent::LanePausedSet { asset, paused } => {
                let lane = self.lane_mut(asset)?;
                lane.slot0 = set_lane_slot0_paused(lane.slot0, paused);
            }
            QuoteEvent::PricePushThresholdSet {
                asset,
                price_push_threshold,
                enabled,
            } => {
                let lane = self.lane_mut(asset)?;
                lane.slot0 =
                    set_lane_slot0_price_push_threshold(lane.slot0, price_push_threshold, enabled)
                        .map_err(|_| ReducerError::InvalidWidth)?;
            }
            QuoteEvent::BlockDelaySet { asset, block_delay } => {
                let lane = self.lane_mut(asset)?;
                lane.slot0 = set_lane_slot0_block_delay(lane.slot0, block_delay);
            }
            QuoteEvent::PartnerInfoSet { router, asset, fee }
            | QuoteEvent::PartnerFeeSet { router, asset, fee } => {
                if router != self.configured_router {
                    return Ok(());
                }
                if U256::from(fee) > BPS {
                    return Err(ReducerError::InvalidWidth);
                }
                self.state_mut()
                    .fee_profile
                    .partner_fee_bps
                    .insert(asset, fee);
            }
            QuoteEvent::WhitelistSet {
                router,
                whitelisted,
            } => {
                if router == self.configured_router {
                    self.state_mut().fee_profile.whitelisted = whitelisted;
                }
            }
            QuoteEvent::BlacklistFeeMultiplierSet { multiplier } => {
                self.state_mut().fee_profile.blacklist_fee_multiplier = multiplier;
            }
            QuoteEvent::DepositExecuted { asset, principal } => {
                let lane = self.lane_mut(asset)?;
                let next = lane
                    .total_principal_amount
                    .checked_add(principal)
                    .ok_or(ReducerError::Arithmetic)?;
                lane.total_principal_amount = next;
            }
            QuoteEvent::WithdrawalExecuted { asset, principal } => {
                let lane = self.lane_mut(asset)?;
                let next = lane
                    .total_principal_amount
                    .checked_sub(principal)
                    .ok_or(ReducerError::Arithmetic)?;
                lane.total_principal_amount = next;
            }
            QuoteEvent::Sync {
                asset,
                asset_reserve,
                cash_reserve,
            } => {
                self.state_mut().cash_reserve = cash_reserve;
                if asset != self.state.cash {
                    self.lane_mut(asset)?.asset_reserve = asset_reserve;
                }
            }
            QuoteEvent::ImplementationUpgraded { .. } => {
                return Err(ReducerError::ImplementationUpgraded);
            }
        }
        Ok(())
    }

    fn state_mut(&mut self) -> &mut QuoteState {
        Arc::make_mut(&mut self.state)
    }

    fn lane_mut(&mut self, asset: Address) -> Result<&mut LaneState, ReducerError> {
        self.state_mut()
            .lanes
            .get_mut(&asset)
            .ok_or(ReducerError::UnknownLane)
    }

    /// Builds a durable checkpoint. This clone is outside the quote path.
    pub fn checkpoint(&self, deployment: &DeploymentConfig) -> Option<Checkpoint> {
        Some(Checkpoint {
            schema_version: SCHEMA_VERSION,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            expected_implementation: deployment.expected_implementation,
            expected_implementation_code_hash: deployment.expected_implementation_code_hash,
            chain_id: deployment.chain_id,
            network: deployment.network,
            core: deployment.core,
            router: deployment.router,
            deployment_block: deployment.deployment_block,
            expect_whitelisted: deployment.expect_whitelisted,
            explicit_lane_assets: deployment.explicit_lane_assets.clone(),
            cursor: self.cursor.clone()?,
            state: self.state.as_ref().clone(),
        })
    }
}

fn is_realtime_progression(previous: &ChainCursor, next: &ChainCursor) -> bool {
    previous.commitment == crate::model::Commitment::Realtime
        && next.commitment == crate::model::Commitment::Realtime
        && next.source_sequence > previous.source_sequence
}

#[cfg(test)]
mod tests {
    use super::{QuoteReducer, ReducerError};
    use crate::model::QuoteEvent;
    use lunarbase_math::Address;
    use lunarbase_math::slot0::{
        LaneSlot0, decode_lane_slot0, encode_lane_slot0, lane_slot0_block_delay,
        lane_slot0_slippage_k_bps,
    };
    use lunarbase_math::{LaneState, QuoteState};

    fn address(value: u8) -> Address {
        Address::new([value; 20])
    }

    #[test]
    fn applies_packed_controls_and_reserve_sync_without_replacing_the_lane() {
        let cash = address(1);
        let asset = address(2);
        let slot0 = encode_lane_slot0(&LaneSlot0 {
            price: 100,
            ..Default::default()
        })
        .unwrap();
        let mut state = QuoteState {
            cash,
            ..Default::default()
        };
        state.lanes.insert(asset, LaneState::new(slot0, 0, 500));
        let mut reducer = QuoteReducer::new(state, address(3));

        reducer
            .apply_event(QuoteEvent::LaneAdded { asset })
            .unwrap();
        let fields = decode_lane_slot0(reducer.state().lanes[&asset].slot0);
        assert_eq!(fields.price, 100);
        assert_eq!(fields.price_push_threshold, 0);
        assert!(!fields.threshold_enabled);
        assert!(fields.exists);
        assert!(fields.paused);

        reducer
            .apply_event(QuoteEvent::LanePausedSet {
                asset,
                paused: false,
            })
            .unwrap();
        reducer
            .apply_event(QuoteEvent::SlippageKSet {
                asset,
                new_k: 1_000,
            })
            .unwrap();
        reducer
            .apply_event(QuoteEvent::BlockDelaySet {
                asset,
                block_delay: 7,
            })
            .unwrap();
        reducer
            .apply_event(QuoteEvent::Sync {
                asset,
                asset_reserve: 11,
                cash_reserve: 12,
            })
            .unwrap();

        let lane = &reducer.state().lanes[&asset];
        assert_eq!(lane_slot0_slippage_k_bps(lane.slot0), 1_000);
        assert_eq!(lane_slot0_block_delay(lane.slot0), 7);
        assert_eq!(lane.asset_reserve, 11);
        assert_eq!(lane.total_principal_amount, 500);
        assert_eq!(reducer.state().cash_reserve, 12);
        assert!(!decode_lane_slot0(lane.slot0).paused);

        reducer
            .apply_event(QuoteEvent::PricePushThresholdSet {
                asset,
                price_push_threshold: 17,
                enabled: true,
            })
            .unwrap();
        reducer
            .apply_event(QuoteEvent::LanePausedSet {
                asset,
                paused: true,
            })
            .unwrap();
        let fields = decode_lane_slot0(reducer.state().lanes[&asset].slot0);
        assert_eq!(fields.price, 100);
        assert_eq!(fields.price_push_threshold, 17);
        assert!(fields.threshold_enabled);
        assert!(fields.paused);
    }

    #[test]
    fn implementation_upgrade_fails_closed_before_mutating_state() {
        let mut reducer = QuoteReducer::new(QuoteState::default(), address(3));
        assert_eq!(
            reducer.apply_event(QuoteEvent::ImplementationUpgraded {
                implementation: address(4),
            }),
            Err(ReducerError::ImplementationUpgraded)
        );
        assert_eq!(reducer.state().cash_reserve, 0);
        assert!(reducer.state().lanes.is_empty());
    }

    #[test]
    fn state_delta_for_unknown_lane_fails_closed() {
        let asset = address(2);
        let mut reducer = QuoteReducer::new(QuoteState::default(), address(3));
        assert_eq!(
            reducer.apply_event(QuoteEvent::LaneUpdated {
                asset,
                slot0: Default::default(),
            }),
            Err(ReducerError::UnknownLane)
        );
        assert!(reducer.state().lanes.is_empty());
    }

    #[test]
    fn state_snapshot_remains_immutable_after_copy_on_write_update() {
        let cash = address(1);
        let mut reducer = QuoteReducer::new(
            QuoteState {
                cash,
                cash_reserve: 10,
                ..Default::default()
            },
            address(3),
        );
        let snapshot = reducer.state_snapshot();

        reducer
            .apply_event(QuoteEvent::Sync {
                asset: cash,
                asset_reserve: 0,
                cash_reserve: 12,
            })
            .unwrap();

        assert_eq!(snapshot.cash_reserve, 10);
        assert_eq!(reducer.state().cash_reserve, 12);
    }
}
