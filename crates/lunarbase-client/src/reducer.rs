use crate::{ChainCursor, Checkpoint, QuoteEvent, MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION};
use lunarbase_math::{QuoteState, U256};
use thiserror::Error;
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ReducerError {
    #[error("cursor chain id mismatch")]
    ChainIdMismatch,
    #[error("cursor regression")]
    CursorRegression,
    #[error("removed log requires canonical rebuild")]
    RemovedLog,
    #[error("block hash mismatch")]
    BlockHashMismatch,
    #[error("arithmetic transition failed: {0}")]
    Arithmetic(#[from] lunarbase_math::MathError),
    #[error("invalid lane slippage K")]
    InvalidSlippageK,
    #[error("event value does not fit the contract storage width")]
    InvalidWidth,
}

/// Single-writer state reducer. Events must arrive in `(block, tx, log)` order.
#[derive(Clone, Debug)]
pub struct QuoteReducer {
    state: QuoteState,
    cursor: Option<ChainCursor>,
    ready: bool,
}

impl QuoteReducer {
    pub fn new(state: QuoteState) -> Self {
        Self {
            state,
            cursor: None,
            ready: false,
        }
    }
    pub fn from_checkpoint(checkpoint: Checkpoint) -> Self {
        Self {
            state: checkpoint.state,
            cursor: Some(checkpoint.cursor),
            ready: true,
        }
    }
    pub fn state(&self) -> &QuoteState {
        &self.state
    }
    pub fn cursor(&self) -> Option<&ChainCursor> {
        self.cursor.as_ref()
    }
    pub fn is_ready(&self) -> bool {
        self.ready
    }
    pub fn mark_not_ready(&mut self) {
        self.ready = false;
    }
    pub fn publish_ready(&mut self) {
        self.ready = true;
    }
    pub fn bootstrap(&mut self, cursor: ChainCursor) {
        self.cursor = Some(cursor);
        self.ready = true;
    }

    /// Observe a source head without mutating quote state. Heads advance the
    /// durable cursor and can promote commitment for the current block, but a
    /// late realtime head must never downgrade a finalized cursor.
    pub fn observe_head(&mut self, head: ChainCursor) -> Result<(), ReducerError> {
        if let Some(current) = &mut self.cursor {
            if current.chain_id != head.chain_id {
                return Err(ReducerError::ChainIdMismatch);
            }
            if current.block_number == head.block_number
                && current.block_hash.is_some()
                && head.block_hash.is_some()
                && current.block_hash != head.block_hash
            {
                return Err(ReducerError::BlockHashMismatch);
            }
            if head.block_number < current.block_number {
                return Ok(());
            }
            if head.block_number == current.block_number {
                if current.block_hash.is_none() {
                    current.block_hash = head.block_hash;
                }
                if head.commitment > current.commitment {
                    current.commitment = head.commitment;
                }
                return Ok(());
            }
        }
        self.cursor = Some(head);
        Ok(())
    }

    pub fn apply(&mut self, cursor: ChainCursor, event: QuoteEvent) -> Result<(), ReducerError> {
        if let Some(previous) = &self.cursor {
            if previous.chain_id != cursor.chain_id {
                return Err(ReducerError::ChainIdMismatch);
            }
            if previous.block_number == cursor.block_number
                && previous.block_hash.is_some()
                && cursor.block_hash.is_some()
                && previous.block_hash != cursor.block_hash
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
        let previous_state = self.state.clone();
        let previous_cursor = self.cursor.clone();
        let result = (|| {
            self.apply_event(event)?;
            self.state.state_version = self
                .state
                .state_version
                .checked_add(1)
                .ok_or(lunarbase_math::MathError::Overflow)?;
            self.cursor = Some(cursor);
            Ok::<(), ReducerError>(())
        })();
        if result.is_err() {
            self.state = previous_state;
            self.cursor = previous_cursor;
        }
        result
    }

    fn apply_event(&mut self, event: QuoteEvent) -> Result<(), ReducerError> {
        match event {
            QuoteEvent::LaneAdded { asset } => {
                self.state.lanes.entry(asset).or_default().exists = true;
            }
            QuoteEvent::LaneRemoved { asset } => {
                self.state.lanes.remove(&asset);
            }
            QuoteEvent::LaneUpdated { asset, slot0 } => {
                self.state.lanes.entry(asset).or_default().slot0 = slot0;
            }
            QuoteEvent::SlippageKSet { asset, new_k } => {
                if new_k > lunarbase_math::BPS {
                    return Err(ReducerError::InvalidSlippageK);
                }
                self.state.lanes.entry(asset).or_default().slippage_k_bps =
                    u32::try_from(new_k).map_err(|_| ReducerError::InvalidWidth)?;
            }
            QuoteEvent::PartnerInfoSet { router, asset, fee }
            | QuoteEvent::PartnerFeeSet { router, asset, fee } => {
                if fee > lunarbase_math::BPS {
                    return Err(ReducerError::InvalidWidth);
                }
                self.state.partner_fee_bps.insert((router, asset), fee);
            }
            QuoteEvent::WhitelistSet {
                router,
                whitelisted,
            } => {
                self.state.whitelist.insert(router, whitelisted);
            }
            QuoteEvent::BlacklistFeeMultiplierSet { multiplier } => {
                self.state.blacklist_fee_multiplier = multiplier;
            }
            QuoteEvent::DepositExecuted { asset, principal } => {
                if principal > lunarbase_math::U128_MAX {
                    return Err(ReducerError::InvalidWidth);
                }
                let current = self
                    .state
                    .total_principal_amount
                    .get(&asset)
                    .copied()
                    .unwrap_or(U256::ZERO);
                self.state.total_principal_amount.insert(
                    asset,
                    current
                        .checked_add(principal)
                        .ok_or(lunarbase_math::MathError::Overflow)?,
                );
                if self.state.total_principal_amount[&asset] > lunarbase_math::U128_MAX {
                    return Err(ReducerError::InvalidWidth);
                }
            }
            QuoteEvent::WithdrawalExecuted { asset, principal } => {
                if principal > lunarbase_math::U128_MAX {
                    return Err(ReducerError::InvalidWidth);
                }
                let current = self
                    .state
                    .total_principal_amount
                    .get(&asset)
                    .copied()
                    .unwrap_or(U256::ZERO);
                self.state.total_principal_amount.insert(
                    asset,
                    current
                        .checked_sub(principal)
                        .ok_or(lunarbase_math::MathError::Overflow)?,
                );
            }
            QuoteEvent::SwapExecuted => {}
        }
        Ok(())
    }

    pub fn checkpoint(&self, code_hash: [u8; 32]) -> Option<Checkpoint> {
        Some(Checkpoint {
            schema_version: SCHEMA_VERSION,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            expected_runtime_code_hash: code_hash,
            cursor: self.cursor.clone()?,
            state: self.state.clone(),
        })
    }
}
