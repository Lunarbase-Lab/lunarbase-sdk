//! Bounded block-identity proof retained independently from state before-images.

use super::{JournalState, MAX_OPTIMISTIC_HISTORY_BLOCKS, OptimisticJournal, correction_gap};
use crate::indexer::errors::IndexerError;
use crate::model::ChainCursor;
use std::sync::Arc;

impl OptimisticJournal {
    pub(super) fn validate_event_identity(&self, cursor: &ChainCursor) -> Result<(), IndexerError> {
        let Some(retained) = self
            .inner
            .identities
            .iter()
            .find(|retained| retained.block_number == cursor.block_number)
        else {
            if self
                .inner
                .rollback_floor
                .as_ref()
                .is_some_and(|floor| cursor.block_number <= floor.block_number)
            {
                return Err(correction_gap(
                    "event identity is older than retained optimistic history",
                ));
            }
            return Ok(());
        };
        let compatible_hash = retained.block_hash == cursor.block_hash
            || (retained.block_hash.is_none() && cursor.block_hash.is_some());
        if retained.chain_id != cursor.chain_id
            || retained.execution_block_number != cursor.execution_block_number
            || !compatible_hash
        {
            return Err(correction_gap(
                "event identity disagrees with retained optimistic history",
            ));
        }
        Ok(())
    }

    pub(super) fn record_event_identity(&mut self, cursor: ChainCursor) {
        self.record_identity(cursor, false);
    }

    pub(in crate::indexer::engine) fn record_head_identity(&mut self, cursor: ChainCursor) {
        self.record_identity(cursor, true);
    }

    fn record_identity(&mut self, cursor: ChainCursor, replace: bool) {
        if self
            .inner
            .rollback_floor
            .as_ref()
            .is_some_and(|floor| cursor.block_number <= floor.block_number)
        {
            return;
        }
        if let Some(index) = self
            .inner
            .identities
            .iter()
            .position(|retained| retained.block_number == cursor.block_number)
        {
            let retained = &self.inner.identities[index];
            if !replace
                && retained.chain_id == cursor.chain_id
                && retained.execution_block_number == cursor.execution_block_number
                && retained.block_hash == cursor.block_hash
            {
                return;
            }
            Arc::make_mut(&mut self.inner).identities[index] = cursor;
            return;
        }
        Arc::make_mut(&mut self.inner).insert_identity(cursor);
    }
}

impl JournalState {
    fn insert_identity(&mut self, cursor: ChainCursor) {
        let index = self
            .identities
            .iter()
            .position(|retained| retained.block_number > cursor.block_number)
            .unwrap_or(self.identities.len());
        self.identities.insert(index, cursor);
        while self.identities.len() > MAX_OPTIMISTIC_HISTORY_BLOCKS {
            let evicted = self
                .identities
                .pop_front()
                .expect("identity window exceeded its nonzero bound");
            let mut pruned = false;
            while self
                .blocks
                .front()
                .is_some_and(|block| block.cursor.block_number <= evicted.block_number)
            {
                self.drop_front(true);
                pruned = true;
            }
            self.rollback_floor = Some(evicted);
            if pruned {
                self.generation = self.generation.saturating_add(1);
            }
        }
    }
}
