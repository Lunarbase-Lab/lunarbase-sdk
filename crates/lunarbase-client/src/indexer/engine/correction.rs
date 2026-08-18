//! Copy-on-write optimistic history and private correction construction.

#[path = "correction/identity.rs"]
mod identity;

use super::{AppliedCorrection, QuoteIndexer, snapshot_covers, validate_core_log_identity};
use crate::indexer::errors::IndexerError;
use crate::model::{BlockRef, ChainCorrection, ChainCursor, Commitment};
use crate::protocol::abi::decode_core_event;
use crate::state::reducer::{QuoteReducer, ReducerError};
use std::collections::VecDeque;
use std::sync::Arc;

const MAX_OPTIMISTIC_HISTORY_BLOCKS: usize = 128;
const MAX_OPTIMISTIC_HISTORY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct OptimisticJournal {
    inner: Arc<JournalState>,
}

#[derive(Clone, Debug, Default)]
struct JournalState {
    blocks: VecDeque<OptimisticBlock>,
    identities: VecDeque<ChainCursor>,
    retained_bytes: usize,
    rollback_floor: Option<ChainCursor>,
    budget_evictions: u64,
    generation: u64,
}

#[derive(Clone, Debug)]
struct OptimisticBlock {
    cursor: ChainCursor,
    before: QuoteReducer,
    retained_bytes: usize,
}

impl Default for OptimisticJournal {
    fn default() -> Self {
        Self {
            inner: Arc::new(JournalState::default()),
        }
    }
}

impl OptimisticJournal {
    pub(super) fn reset(&mut self, floor: ChainCursor) {
        let budget_evictions = self.inner.budget_evictions;
        let generation = self.inner.generation.saturating_add(1);
        self.inner = Arc::new(JournalState {
            rollback_floor: Some(floor),
            budget_evictions,
            generation,
            ..JournalState::default()
        });
    }

    pub(super) fn needs_capture(&self, cursor: &ChainCursor) -> bool {
        !self.inner.blocks.back().is_some_and(|block| {
            block.cursor.chain_id == cursor.chain_id
                && block.cursor.block_number == cursor.block_number
                && block.cursor.execution_block_number == cursor.execution_block_number
                && block.cursor.block_hash == cursor.block_hash
        })
    }

    pub(super) fn capture_before(&mut self, cursor: &ChainCursor, reducer: QuoteReducer) -> bool {
        if !self.needs_capture(cursor) {
            return false;
        }
        let retained_bytes = reducer.retained_bytes();
        let state = Arc::make_mut(&mut self.inner);
        state.generation = state.generation.saturating_add(1);
        state.retained_bytes = state.retained_bytes.saturating_add(retained_bytes);
        state.blocks.push_back(OptimisticBlock {
            cursor: cursor.clone(),
            before: reducer,
            retained_bytes,
        });
        state.enforce_limits();
        true
    }

    pub(super) fn advance_floor(&mut self, floor: ChainCursor) {
        let state = Arc::make_mut(&mut self.inner);
        state.generation = state.generation.saturating_add(1);
        while state
            .blocks
            .front()
            .is_some_and(|block| block.cursor.block_number <= floor.block_number)
        {
            state.drop_front(false);
        }
        while state
            .identities
            .front()
            .is_some_and(|cursor| cursor.block_number <= floor.block_number)
        {
            state.identities.pop_front();
        }
        state.rollback_floor = Some(floor);
    }

    fn restore_point(
        &self,
        ancestor: &BlockRef,
        old_branch: &[BlockRef],
    ) -> Result<Option<QuoteReducer>, IndexerError> {
        validate_rollback_floor(self.inner.rollback_floor.as_ref(), ancestor)?;
        let retained = self
            .inner
            .identities
            .iter()
            .find(|cursor| cursor.block_number == ancestor.cursor.block_number);
        if self
            .inner
            .rollback_floor
            .as_ref()
            .is_none_or(|floor| ancestor.cursor.block_number > floor.block_number)
            && retained.is_none()
        {
            return Err(correction_gap(
                "correction ancestor identity is not retained",
            ));
        }
        if let Some(retained) = retained
            && (retained.chain_id != ancestor.cursor.chain_id
                || retained.execution_block_number != ancestor.cursor.execution_block_number
                || retained.block_hash.is_none()
                || retained.block_hash != ancestor.cursor.block_hash)
        {
            return Err(correction_gap(
                "correction ancestor identity disagrees with retained optimistic history",
            ));
        }
        self.validate_retained_branch(ancestor, old_branch)?;
        let affected = self
            .inner
            .blocks
            .iter()
            .filter(|block| block.cursor.block_number > ancestor.cursor.block_number);
        let mut first = None;
        for block in affected {
            let branch_offset = block
                .cursor
                .block_number
                .saturating_sub(ancestor.cursor.block_number)
                .saturating_sub(1) as usize;
            let Some(expected) = old_branch.get(branch_offset) else {
                return Err(correction_gap(
                    "journal extends beyond the abandoned branch",
                ));
            };
            if block.cursor.chain_id != expected.cursor.chain_id
                || block.cursor.execution_block_number != expected.cursor.execution_block_number
                || block.cursor.block_hash.is_none()
                || block.cursor.block_hash != expected.cursor.block_hash
            {
                return Err(correction_gap(
                    "journal block identity does not match the abandoned branch",
                ));
            }
            first.get_or_insert_with(|| block.before.clone());
        }
        Ok(first)
    }

    /// Proves every observed block above the ancestor against the abandoned
    /// branch, including head-only blocks which have no reducer before-image.
    /// Both inputs are height ordered, so validation is linear and allocation-free.
    fn validate_retained_branch(
        &self,
        ancestor: &BlockRef,
        old_branch: &[BlockRef],
    ) -> Result<(), IndexerError> {
        for retained in self
            .inner
            .identities
            .iter()
            .filter(|cursor| cursor.block_number > ancestor.cursor.block_number)
        {
            let offset = retained
                .block_number
                .checked_sub(ancestor.cursor.block_number)
                .and_then(|distance| distance.checked_sub(1))
                .and_then(|offset| usize::try_from(offset).ok());
            let Some(expected) = offset.and_then(|offset| old_branch.get(offset)) else {
                return Err(correction_gap(
                    "retained identity extends beyond the abandoned branch",
                ));
            };
            if retained.chain_id != expected.cursor.chain_id
                || retained.execution_block_number != expected.cursor.execution_block_number
                || retained.block_hash.is_none()
                || retained.block_hash != expected.cursor.block_hash
            {
                return Err(correction_gap(
                    "retained identity does not match the abandoned branch",
                ));
            }
        }
        Ok(())
    }

    fn rewind_after(&mut self, ancestor: &BlockRef) {
        let state = Arc::make_mut(&mut self.inner);
        let mut changed = false;
        while state
            .blocks
            .back()
            .is_some_and(|block| block.cursor.block_number > ancestor.cursor.block_number)
        {
            if let Some(block) = state.blocks.pop_back() {
                state.retained_bytes = state.retained_bytes.saturating_sub(block.retained_bytes);
                changed = true;
            }
        }
        if changed {
            state.generation = state.generation.saturating_add(1);
        }
        while state
            .identities
            .back()
            .is_some_and(|cursor| cursor.block_number > ancestor.cursor.block_number)
        {
            state.identities.pop_back();
        }
    }

    pub(super) fn usage(&self) -> (usize, usize, u64, u64) {
        (
            self.inner.blocks.len(),
            self.inner.retained_bytes,
            self.inner.budget_evictions,
            self.inner.generation,
        )
    }
}

impl JournalState {
    fn enforce_limits(&mut self) {
        while self.blocks.len() > MAX_OPTIMISTIC_HISTORY_BLOCKS
            || self.retained_bytes > MAX_OPTIMISTIC_HISTORY_BYTES
        {
            self.drop_front(true);
        }
    }

    fn drop_front(&mut self, budget_eviction: bool) {
        if let Some(block) = self.blocks.pop_front() {
            self.retained_bytes = self.retained_bytes.saturating_sub(block.retained_bytes);
            self.rollback_floor = Some(block.cursor);
            if budget_eviction {
                self.budget_evictions = self.budget_evictions.saturating_add(1);
            }
        }
    }
}

impl QuoteIndexer {
    pub(super) fn observe_covered_correction(
        &mut self,
        correction: &ChainCorrection,
    ) -> Result<(), IndexerError> {
        let fingerprint = correction.fingerprint();
        if let Some(applied) = self.last_correction.as_ref() {
            if correction.new_tip.cursor.block_number < applied.tip.block_number {
                return Ok(());
            }
            if correction.new_tip.cursor.block_number == applied.tip.block_number {
                if applied.fingerprint != fingerprint {
                    return Err(correction_gap(
                        "snapshot-covered correction conflicts with the retained delta identity",
                    ));
                }
                return Ok(());
            }
        }
        self.last_correction = Some(AppliedCorrection {
            fingerprint,
            tip: correction.new_tip.cursor.clone(),
        });
        Ok(())
    }

    /// Builds a fully validated replacement privately; callers publish it by swap.
    pub(crate) fn into_corrected_core(
        mut self,
        correction: &ChainCorrection,
    ) -> Result<(Self, bool), IndexerError> {
        correction.validate()?;
        let fingerprint = correction.fingerprint();
        if correction.common_ancestor.cursor.chain_id != self.deployment.chain_id {
            return Err(ReducerError::ChainIdMismatch.into());
        }
        for log in &correction.replacement_logs {
            validate_core_log_identity(log, self.deployment.core, self.deployment.chain_id)?;
        }
        if let Some(applied) = self.last_correction.as_ref()
            && applied.fingerprint == fingerprint
            && self
                .reducer
                .cursor()
                .is_some_and(|current| snapshot_covers(&applied.tip, current).ok() == Some(true))
        {
            return Ok((self, false));
        }
        if self.reducer.cursor().is_some_and(|current| {
            current.chain_id == correction.new_tip.cursor.chain_id
                && current.block_number == correction.new_tip.cursor.block_number
                && current.execution_block_number
                    == correction.new_tip.cursor.execution_block_number
                && current.block_hash.is_some()
                && current.block_hash == correction.new_tip.cursor.block_hash
        }) {
            if self.last_correction.as_ref().is_some_and(|applied| {
                applied.fingerprint == fingerprint && applied.tip == correction.new_tip.cursor
            }) {
                return Ok((self, false));
            }
            return Err(correction_gap(
                "correction new tip was observed without a matching applied delta",
            ));
        }
        self.validate_correction_identity(correction)?;
        self.validate_finalized_ancestor(&correction.common_ancestor)?;
        let was_ready = self.reducer.is_ready();
        if !was_ready {
            return Err(IndexerError::NotReady);
        }

        let restore = self
            .optimistic_history
            .restore_point(&correction.common_ancestor, &correction.old_branch)?;
        self.optimistic_history
            .rewind_after(&correction.common_ancestor);
        if let Some(reducer) = restore {
            self.reducer = reducer;
        }
        self.reducer
            .rewind_head(correction.common_ancestor.cursor.clone())?;

        for block in &correction.new_branch {
            self.optimistic_history
                .record_head_identity(block.cursor.clone());
        }

        for log in &correction.replacement_logs {
            let event = decode_core_event(log)?;
            self.apply_decoded_log(log.cursor.clone(), event)?;
        }
        self.reducer
            .observe_corrected_head(correction.new_tip.cursor.clone())?;
        self.record_finalized_update(&correction.new_tip.cursor)?;
        self.last_correction = Some(AppliedCorrection {
            fingerprint,
            tip: correction.new_tip.cursor.clone(),
        });
        debug_assert!(self.reducer.is_ready());
        Ok((self, true))
    }

    fn validate_correction_identity(
        &self,
        correction: &ChainCorrection,
    ) -> Result<(), IndexerError> {
        if let Some(floor) = self.canonical_floor.as_ref() {
            validate_rollback_floor(Some(floor), &correction.common_ancestor)?;
        }
        let current = self.reducer.cursor().ok_or(IndexerError::NoCursor)?;
        if current.chain_id != correction.old_tip.cursor.chain_id
            || current.block_number != correction.old_tip.cursor.block_number
            || current.execution_block_number != correction.old_tip.cursor.execution_block_number
            || current.block_hash.is_none()
            || current.block_hash != correction.old_tip.cursor.block_hash
        {
            return Err(correction_gap(
                "correction old tip does not match the published optimistic head",
            ));
        }
        if current.commitment == Commitment::Finalized {
            return Err(correction_gap("correction cannot replace a finalized tip"));
        }
        Ok(())
    }

    pub(super) fn apply_decoded_log(
        &mut self,
        cursor: ChainCursor,
        event: Option<crate::model::QuoteEvent>,
    ) -> Result<(), IndexerError> {
        self.validate_finalized_update(&cursor)?;
        self.optimistic_history.validate_event_identity(&cursor)?;
        let Some(event) = event else {
            self.optimistic_history
                .record_event_identity(cursor.clone());
            self.record_finalized_update(&cursor)?;
            return Ok(());
        };
        let changed = if self.optimistic_history.needs_capture(&cursor) {
            let before = self.reducer.clone();
            let changed = self.reducer.apply_with_effect(cursor.clone(), event)?;
            if changed {
                self.optimistic_history.capture_before(&cursor, before);
            }
            changed
        } else {
            self.reducer.apply_with_effect(cursor.clone(), event)?
        };
        self.optimistic_history
            .record_event_identity(cursor.clone());
        self.record_finalized_update(&cursor)?;
        if changed {
            self.last_correction = None;
        }
        Ok(())
    }

    pub(crate) fn correction_history_usage(&self) -> (usize, usize, u64, u64) {
        self.optimistic_history.usage()
    }

    pub(crate) fn correction_history_generation(&self) -> u64 {
        self.optimistic_history.inner.generation
    }
}

fn validate_rollback_floor(
    floor: Option<&ChainCursor>,
    ancestor: &BlockRef,
) -> Result<(), IndexerError> {
    let Some(floor) = floor else {
        return Ok(());
    };
    if ancestor.cursor.chain_id != floor.chain_id
        || ancestor.cursor.block_number < floor.block_number
        || (ancestor.cursor.block_number == floor.block_number
            && (floor.execution_block_number != ancestor.cursor.execution_block_number
                || floor.block_hash != ancestor.cursor.block_hash))
    {
        return Err(correction_gap(
            "correction ancestor is older than retained optimistic history",
        ));
    }
    Ok(())
}

fn correction_gap(reason: &str) -> IndexerError {
    IndexerError::Gap(reason.into())
}

#[cfg(test)]
mod tests {
    use super::{MAX_OPTIMISTIC_HISTORY_BLOCKS, OptimisticJournal, QuoteIndexer};
    use crate::model::{
        ChainCursor, Commitment, DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network, QuoteEvent,
    };
    use crate::state::reducer::QuoteReducer;
    use lunarbase_math::{Address, B256, FeeClass, QuoteState, U256};

    fn cursor(block: u64) -> ChainCursor {
        ChainCursor::block(
            1,
            block,
            Some(B256::new([block as u8; 32])),
            Commitment::Realtime,
        )
    }

    #[test]
    fn same_block_mutations_reuse_one_before_image_without_repeated_cow() {
        let deployment = DeploymentConfig {
            network: Network::Evm,
            chain_id: 1,
            core: Address::new([1; 20]),
            fee_class: FeeClass::Whitelisted,
            verified_router: None,
            deployment_block: 0,
            expected_implementation: Address::new([2; 20]),
            expected_implementation_code_hash: B256::new([3; 32]),
            contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            explicit_lane_assets: Vec::new(),
        };
        let mut indexer = QuoteIndexer::new(QuoteState::default(), deployment);
        indexer.reducer.bootstrap(cursor(0));

        let mut first = cursor(1);
        first.transaction_index = Some(0);
        first.log_index = Some(0);
        indexer
            .apply_decoded_log(
                first,
                Some(QuoteEvent::BlacklistFeeMultiplierSet {
                    multiplier: U256::from(2),
                }),
            )
            .unwrap();
        let state_ptr = indexer.reducer.state_ptr();
        assert_eq!(indexer.reducer.state_strong_count(), 1);

        for log_index in 1..128 {
            let mut next = cursor(1);
            next.transaction_index = Some(0);
            next.log_index = Some(log_index);
            assert!(!indexer.optimistic_history.needs_capture(&next));
            assert_eq!(indexer.reducer.state_strong_count(), 1);
            indexer
                .apply_decoded_log(
                    next,
                    Some(QuoteEvent::BlacklistFeeMultiplierSet {
                        multiplier: U256::from(log_index + 2),
                    }),
                )
                .unwrap();
            assert_eq!(indexer.reducer.state_ptr(), state_ptr);
            assert_eq!(indexer.reducer.state_strong_count(), 1);
        }
        assert_eq!(indexer.correction_history_usage().0, 1);
    }

    #[test]
    fn journal_prunes_to_its_count_budget() {
        let reducer = QuoteReducer::new(QuoteState::default(), FeeClass::Whitelisted, None);
        let mut journal = OptimisticJournal::default();
        journal.reset(cursor(0));
        for block in 1..=(MAX_OPTIMISTIC_HISTORY_BLOCKS as u64 + 1) {
            assert!(journal.capture_before(&cursor(block), reducer.clone()));
        }
        assert_eq!(journal.usage().0, MAX_OPTIMISTIC_HISTORY_BLOCKS);
        assert_eq!(journal.usage().2, 1);
        assert_eq!(
            journal.inner.rollback_floor.as_ref().unwrap().block_number,
            1
        );
    }
}
