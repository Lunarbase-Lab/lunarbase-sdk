//! Snapshot bootstrap with compact, deferred optimistic-correction notices.

use super::{
    QuoteIndexer, QuoteReducer, snapshot_covers, sort_chain_update_refs, sort_chain_updates,
    update_cursor, validate_verified_router_snapshot,
};
use crate::bootstrap::BootstrapSnapshot;
use crate::indexer::errors::IndexerError;
use crate::model::{ChainCorrection, ChainUpdate};
use crate::state::reducer::ReducerError;
use lunarbase_math::B256;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CorrectionNotice {
    pub(crate) common_ancestor: u64,
    pub(crate) old_tip_hash: B256,
    pub(crate) new_tip_hash: B256,
    pub(crate) replacement_logs: usize,
}

impl CorrectionNotice {
    pub(super) fn from_validated(correction: &ChainCorrection) -> Self {
        Self {
            common_ancestor: correction.common_ancestor.cursor.block_number,
            old_tip_hash: correction
                .old_tip
                .cursor
                .block_hash
                .expect("validated correction old tip has a hash"),
            new_tip_hash: correction
                .new_tip
                .cursor
                .block_hash
                .expect("validated correction new tip has a hash"),
            replacement_logs: correction.replacement_logs.len(),
        }
    }
}

impl QuoteIndexer {
    /// Installs a coherent snapshot and applies buffered post-snapshot data.
    pub fn bootstrap_normalized(
        &mut self,
        snapshot: BootstrapSnapshot,
        mut buffered: Vec<ChainUpdate>,
    ) -> Result<(), IndexerError> {
        sort_chain_updates(&mut buffered);
        self.bootstrap_normalized_ordered(snapshot, buffered.iter())
            .map(drop)
    }

    #[cfg(test)]
    pub(crate) fn bootstrap_normalized_with_notices(
        &mut self,
        snapshot: BootstrapSnapshot,
        mut buffered: Vec<ChainUpdate>,
    ) -> Result<Vec<CorrectionNotice>, IndexerError> {
        sort_chain_updates(&mut buffered);
        self.bootstrap_normalized_ordered(snapshot, buffered.iter())
    }

    pub(crate) fn bootstrap_normalized_borrowed_with_notices(
        &mut self,
        snapshot: BootstrapSnapshot,
        mut buffered: Vec<&ChainUpdate>,
    ) -> Result<Vec<CorrectionNotice>, IndexerError> {
        sort_chain_update_refs(&mut buffered);
        self.bootstrap_normalized_ordered(snapshot, buffered)
    }

    fn bootstrap_normalized_ordered<'a>(
        &mut self,
        snapshot: BootstrapSnapshot,
        buffered: impl IntoIterator<Item = &'a ChainUpdate>,
    ) -> Result<Vec<CorrectionNotice>, IndexerError> {
        if snapshot.cursor.chain_id != self.deployment.chain_id {
            return Err(ReducerError::ChainIdMismatch.into());
        }
        self.validate_finalized_recovery(&snapshot.cursor)?;
        self.validate_finalized_update(&snapshot.cursor)?;
        if snapshot.implementation != self.deployment.expected_implementation
            || snapshot.implementation_code_hash
                != self.deployment.expected_implementation_code_hash
        {
            return Err(IndexerError::CodeHashMismatch);
        }
        validate_verified_router_snapshot(&snapshot, &self.deployment)?;
        let snapshot_cursor = snapshot.cursor.clone();
        let snapshot_chain = snapshot.cursor.chain_id;
        let retained_correction = self
            .last_correction
            .clone()
            .filter(|applied| snapshot_covers(&applied.tip, &snapshot_cursor).ok() == Some(true));
        self.reducer = QuoteReducer::new(
            snapshot.state,
            self.deployment.fee_class,
            snapshot.verified_router,
        );
        self.reducer.bootstrap(snapshot.cursor);
        self.record_finalized_update(&snapshot_cursor)?;
        self.canonical_floor = Some(snapshot_cursor.clone());
        self.stable_checkpoint = self.reducer.checkpoint(&self.deployment).map(Arc::new);
        self.optimistic_history.reset(snapshot_cursor.clone());
        self.last_correction = retained_correction;
        let mut notices = Vec::new();
        for update in buffered {
            self.validate_core_update_identity(update)?;
            if let Some(cursor) = update_cursor(update) {
                if cursor.chain_id != snapshot_chain {
                    self.reducer.mark_not_ready();
                    return Err(ReducerError::ChainIdMismatch.into());
                }
                if snapshot_covers(cursor, &snapshot_cursor)? {
                    if let ChainUpdate::Correction(correction) = update {
                        self.observe_covered_correction(correction)?;
                    }
                    continue;
                }
            }
            match self.apply_validated_core_update_borrowed_with_notice(update) {
                Ok(Some(notice)) => notices.push(notice),
                Ok(None) => {}
                Err(error) => {
                    self.reducer.mark_not_ready();
                    return Err(error);
                }
            }
        }
        self.reducer.publish_ready();
        Ok(notices)
    }
}
