//! Ordered single-writer quote-state reducer.

use crate::model::{
    ChainCursor, Checkpoint, DeploymentConfig, MATH_COMPATIBILITY_VERSION, QuoteEvent,
    SCHEMA_VERSION,
};
use lunarbase_math::arithmetic::BPS;
use lunarbase_math::state::QuoteState;
use lunarbase_math::types::Address;
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
}

#[derive(Clone, Debug)]
/// Single-writer reducer over quote-critical state.
///
/// Every fallible value is validated before mutation, avoiding the previous
/// full-state clone used for rollback on each event.
pub struct QuoteReducer {
    /// Complete quote-critical state mutated only by the ordered reducer task.
    state: QuoteState,
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
            state,
            configured_router,
            cursor: None,
            ready: false,
        }
    }

    /// Restores a ready reducer from a prevalidated checkpoint.
    pub fn from_checkpoint(checkpoint: Checkpoint) -> Self {
        Self {
            state: checkpoint.state,
            configured_router: checkpoint.router,
            cursor: Some(checkpoint.cursor),
            ready: true,
        }
    }

    /// Returns the current immutable state view.
    pub fn state(&self) -> &QuoteState {
        &self.state
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
                self.state.lanes.entry(asset).or_default().set_exists(true);
            }
            QuoteEvent::LaneRemoved { asset } => {
                self.state.lanes.remove(&asset);
            }
            QuoteEvent::LaneUpdated { asset, slot0 } => {
                self.state.lanes.entry(asset).or_default().slot0 = slot0;
            }
            QuoteEvent::SlippageKSet { asset, new_k } => {
                if new_k > BPS {
                    return Err(ReducerError::InvalidSlippageK);
                }
                let value = u32::try_from(new_k).map_err(|_| ReducerError::InvalidWidth)?;
                self.state.lanes.entry(asset).or_default().slippage_k_bps = value;
            }
            QuoteEvent::PartnerInfoSet { router, asset, fee }
            | QuoteEvent::PartnerFeeSet { router, asset, fee } => {
                if router != self.configured_router {
                    return Ok(());
                }
                if fee > BPS {
                    return Err(ReducerError::InvalidWidth);
                }
                let value = u32::try_from(fee).map_err(|_| ReducerError::InvalidWidth)?;
                self.state.fee_profile.partner_fee_bps.insert(asset, value);
            }
            QuoteEvent::WhitelistSet {
                router,
                whitelisted,
            } => {
                if router == self.configured_router {
                    self.state.fee_profile.whitelisted = whitelisted;
                }
            }
            QuoteEvent::BlacklistFeeMultiplierSet { multiplier } => {
                self.state.fee_profile.blacklist_fee_multiplier = multiplier;
            }
            QuoteEvent::DepositExecuted { asset, principal } => {
                let principal =
                    u128::try_from(principal).map_err(|_| ReducerError::InvalidWidth)?;
                let lane = self.state.lanes.entry(asset).or_default();
                let next = lane
                    .total_principal_amount
                    .checked_add(principal)
                    .ok_or(ReducerError::Arithmetic)?;
                lane.total_principal_amount = next;
            }
            QuoteEvent::WithdrawalExecuted { asset, principal } => {
                let principal =
                    u128::try_from(principal).map_err(|_| ReducerError::InvalidWidth)?;
                let lane = self.state.lanes.entry(asset).or_default();
                let next = lane
                    .total_principal_amount
                    .checked_sub(principal)
                    .ok_or(ReducerError::Arithmetic)?;
                lane.total_principal_amount = next;
            }
        }
        Ok(())
    }

    /// Builds a durable v3 checkpoint. This clone is outside the quote path.
    pub fn checkpoint(&self, deployment: &DeploymentConfig) -> Option<Checkpoint> {
        Some(Checkpoint {
            schema_version: SCHEMA_VERSION,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            expected_runtime_code_hash: deployment.expected_runtime_code_hash,
            chain_id: deployment.chain_id,
            core: deployment.core,
            router: deployment.router,
            cursor: self.cursor.clone()?,
            state: self.state.clone(),
        })
    }
}

fn is_realtime_progression(previous: &ChainCursor, next: &ChainCursor) -> bool {
    previous.commitment == crate::model::Commitment::Realtime
        && next.commitment == crate::model::Commitment::Realtime
        && next.source_sequence > previous.source_sequence
}
