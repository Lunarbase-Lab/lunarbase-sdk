//! Ordered single-writer quote-state reducer.

#[path = "reducer/events.rs"]
mod events;
#[path = "reducer/identity.rs"]
mod identity;
use identity::{
    is_realtime_progression, validate_event_against_head, validate_event_successor,
    validate_head_against_event,
};

use crate::bootstrap::VerifiedRouterSnapshot;
use crate::model::{
    ChainCursor, Checkpoint, DeploymentConfig, MATH_COMPATIBILITY_VERSION, QuoteEvent,
    SCHEMA_VERSION,
};
use lunarbase_math::{Address, FeeClass};
use lunarbase_math::{LaneState, QuoteState};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Failure that revokes quote readiness until canonical recovery.
pub enum ReducerError {
    /// An update cursor belongs to another EIP-155 chain.
    #[error("cursor chain id mismatch")]
    ChainIdMismatch,
    /// A normalized log was emitted by a contract other than the configured Core.
    #[error("contract log address does not match deployment Core")]
    ContractAddressMismatch,
    /// An event precedes the last accepted position in deterministic ordering.
    #[error("cursor regression")]
    CursorRegression,
    /// A provider retracted an applied log, requiring canonical state rebuild.
    #[error("removed log requires canonical rebuild")]
    RemovedLog,
    /// Two non-progressive cursors claim different hashes for one block height.
    #[error("block hash mismatch")]
    BlockHashMismatch,
    /// One canonical log position was reused for a different decoded payload.
    #[error("canonical event position carries a different payload")]
    EventPayloadMismatch,
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
    /// A verified router changed to a class different from the configured policy.
    #[error("verified router fee class changed")]
    FeeClassMismatch,
    /// A newly active lane requires a coherent partner-fee refresh.
    #[error("verified router allocation requires a snapshot refresh")]
    VerifiedRouterRefreshRequired,
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
    /// Test-only count of state CoW clones performed by this reducer lineage.
    #[cfg(test)]
    state_cow_clones: usize,
    /// Runtime-selected economic fee class, independent of chain state.
    fee_class: FeeClass,
    /// Optional exact-router accounting data, separate from quote-critical state.
    verified_router: Option<Arc<VerifiedRouterSnapshot>>,
    /// Last normalized head or event position accepted by the reducer.
    cursor: Option<ChainCursor>,
    /// Last accepted quote event, ordered independently from head delivery.
    event_cursor: Option<ChainCursor>,
    /// Decoded identity at `event_cursor`, used to distinguish retries from corruption.
    event: Option<QuoteEvent>,
    /// Fail-closed publication flag cleared on gaps, reorgs, and reducer errors.
    ready: bool,
}

impl QuoteReducer {
    /// Creates a not-ready reducer around a block-tagged state.
    pub fn new(
        state: QuoteState,
        fee_class: FeeClass,
        verified_router: Option<VerifiedRouterSnapshot>,
    ) -> Self {
        Self {
            state: Arc::new(state),
            #[cfg(test)]
            state_cow_clones: 0,
            fee_class,
            verified_router: verified_router.map(Arc::new),
            cursor: None,
            event_cursor: None,
            event: None,
            ready: false,
        }
    }

    /// Restores a ready reducer from a prevalidated checkpoint.
    pub fn from_checkpoint(checkpoint: Checkpoint, fee_class: FeeClass) -> Self {
        Self {
            state: Arc::new(checkpoint.state),
            #[cfg(test)]
            state_cow_clones: 0,
            fee_class,
            verified_router: None,
            cursor: Some(checkpoint.cursor),
            event_cursor: None,
            event: None,
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

    /// Returns the mandatory economic fee class selected by the runtime.
    pub(crate) const fn fee_class(&self) -> FeeClass {
        self.fee_class
    }

    /// Clones the optional allocation handle without cloning its asset map.
    pub(crate) fn verified_router_snapshot(&self) -> Option<Arc<VerifiedRouterSnapshot>> {
        self.verified_router.as_ref().map(Arc::clone)
    }

    /// Returns the router whose optional allocation is chain-verified.
    pub fn verified_router(&self) -> Option<Address> {
        self.verified_router.as_ref().map(|state| state.router)
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
        self.event_cursor = None;
        self.event = None;
        self.ready = true;
    }

    /// Resets ordering metadata after a fully validated correction restore.
    pub(crate) fn rewind_head(&mut self, head: ChainCursor) -> Result<(), ReducerError> {
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.chain_id != head.chain_id)
            || self
                .event_cursor
                .as_ref()
                .is_some_and(|event| event.chain_id != head.chain_id)
        {
            return Err(ReducerError::ChainIdMismatch);
        }
        self.cursor = Some(head);
        self.event_cursor = None;
        self.event = None;
        Ok(())
    }

    /// Advances a block-level cursor without changing quote state.
    pub fn observe_head(&mut self, head: ChainCursor) -> Result<(), ReducerError> {
        validate_head_against_event(self.event_cursor.as_ref(), self.cursor.as_ref(), &head)?;
        if let Some(current) = &mut self.cursor {
            if current.chain_id != head.chain_id {
                return Err(ReducerError::ChainIdMismatch);
            }
            if head.block_number < current.block_number {
                return Ok(());
            }
            if head.block_number == current.block_number {
                let identity_changed = current.block_hash != head.block_hash;
                let applied_event = self
                    .event_cursor
                    .as_ref()
                    .is_some_and(|event| event.block_number == head.block_number);
                if current.block_hash.is_some() && head.block_hash.is_none() {
                    return Err(ReducerError::BlockHashMismatch);
                }
                if !identity_changed
                    && current.execution_block_number != head.execution_block_number
                {
                    return Err(ReducerError::BlockHashMismatch);
                }
                if identity_changed && (applied_event || !is_realtime_progression(current, &head)) {
                    return Err(ReducerError::BlockHashMismatch);
                }
                if identity_changed {
                    current.block_hash = head.block_hash;
                    current.execution_block_number = head.execution_block_number;
                }
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

    /// Publishes a fully validated correction tip while retaining the last
    /// replacement-event identity used for duplicate detection.
    pub(crate) fn observe_corrected_head(&mut self, head: ChainCursor) -> Result<(), ReducerError> {
        self.observe_head(head.clone())?;
        self.cursor = Some(head);
        Ok(())
    }

    /// Applies one decoded event after validating ordering and storage widths.
    pub fn apply(&mut self, cursor: ChainCursor, event: QuoteEvent) -> Result<(), ReducerError> {
        self.apply_with_effect(cursor, event).map(|_| ())
    }

    /// Applies an event and reports whether quote or verified-router state changed.
    pub(crate) fn apply_with_effect(
        &mut self,
        cursor: ChainCursor,
        event: QuoteEvent,
    ) -> Result<bool, ReducerError> {
        if let Some(head) = &self.cursor {
            if head.chain_id != cursor.chain_id {
                return Err(ReducerError::ChainIdMismatch);
            }
            validate_event_against_head(head, &cursor)?;
        }
        if let Some(previous) = &self.event_cursor {
            if previous.chain_id != cursor.chain_id {
                return Err(ReducerError::ChainIdMismatch);
            }
            validate_event_successor(previous, &cursor)?;
            if cursor.event_order() < previous.event_order() {
                return Err(ReducerError::CursorRegression);
            }
            if cursor.event_order() == previous.event_order() {
                return if self.event.as_ref() == Some(&event) {
                    Ok(false)
                } else {
                    Err(ReducerError::EventPayloadMismatch)
                };
            }
        }
        let retained_event = event.clone();
        let changed = self.apply_event(event)?;
        self.event_cursor = Some(cursor.clone());
        self.event = Some(retained_event);
        match self.cursor.as_mut() {
            Some(head) if cursor.block_number < head.block_number => {
                if cursor.source_sequence > head.source_sequence {
                    head.source_sequence = cursor.source_sequence;
                    head.source_sub_index = cursor.source_sub_index;
                }
            }
            Some(head) if cursor.block_number == head.block_number => {
                let mut published = cursor;
                published.commitment = published.commitment.max(head.commitment);
                *head = published;
            }
            _ => self.cursor = Some(cursor),
        }
        Ok(changed)
    }

    #[cfg(test)]
    pub(crate) fn state_strong_count(&self) -> usize {
        Arc::strong_count(&self.state)
    }

    #[cfg(test)]
    pub(crate) fn state_ptr(&self) -> *const QuoteState {
        Arc::as_ptr(&self.state)
    }

    #[cfg(test)]
    pub(crate) const fn state_cow_clones(&self) -> usize {
        self.state_cow_clones
    }

    fn state_mut(&mut self) -> &mut QuoteState {
        #[cfg(test)]
        if Arc::strong_count(&self.state) > 1 {
            self.state_cow_clones += 1;
        }
        Arc::make_mut(&mut self.state)
    }

    fn lane_mut(&mut self, asset: Address) -> Result<&mut LaneState, ReducerError> {
        self.state_mut()
            .lanes
            .get_mut(&asset)
            .ok_or(ReducerError::UnknownLane)
    }

    /// Conservatively estimates retained bytes for optimistic-history budgets.
    pub(crate) fn retained_bytes(&self) -> usize {
        let lanes = self
            .state
            .lanes
            .capacity()
            .saturating_mul(std::mem::size_of::<(Address, LaneState)>().saturating_add(16));
        let router = self.verified_router.as_ref().map_or(0, |router| {
            std::mem::size_of::<VerifiedRouterSnapshot>().saturating_add(
                router
                    .partner_fee_bps
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(Address, u32)>().saturating_add(16)),
            )
        });
        std::mem::size_of::<Self>()
            .saturating_add(std::mem::size_of::<QuoteState>())
            .saturating_add(lanes)
            .saturating_add(router)
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
            deployment_block: deployment.deployment_block,
            explicit_lane_assets: deployment.explicit_lane_assets.clone(),
            cursor: self.cursor.clone()?,
            state: self.state.as_ref().clone(),
        })
    }
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
    use lunarbase_math::{FeeClass, LaneState, QuoteState};

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
        let mut reducer = QuoteReducer::new(state, FeeClass::Whitelisted, None);

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
        let mut reducer = QuoteReducer::new(QuoteState::default(), FeeClass::Whitelisted, None);
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
        let mut reducer = QuoteReducer::new(QuoteState::default(), FeeClass::Whitelisted, None);
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
            FeeClass::Whitelisted,
            None,
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
