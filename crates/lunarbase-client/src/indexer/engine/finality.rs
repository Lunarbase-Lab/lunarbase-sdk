//! Monotonic finalized identity guard independent of the optimistic tip.

use super::QuoteIndexer;
use crate::indexer::errors::IndexerError;
use crate::model::{BlockRef, ChainCursor, Commitment};
use crate::state::reducer::ReducerError;

impl QuoteIndexer {
    pub(super) fn validate_finalized_update(
        &self,
        cursor: &ChainCursor,
    ) -> Result<(), IndexerError> {
        if cursor.commitment != Commitment::Finalized {
            return Ok(());
        }
        require_hash(cursor)?;
        let Some(floor) = self.finalized_floor.as_ref() else {
            return Ok(());
        };
        if cursor.chain_id != floor.chain_id {
            return Err(ReducerError::ChainIdMismatch.into());
        }
        if cursor.block_number == floor.block_number {
            require_same_identity(floor, cursor)?;
        }
        Ok(())
    }

    pub(super) fn record_finalized_update(
        &mut self,
        cursor: &ChainCursor,
    ) -> Result<(), IndexerError> {
        self.validate_finalized_update(cursor)?;
        if cursor.commitment != Commitment::Finalized
            || self
                .finalized_floor
                .as_ref()
                .is_some_and(|floor| floor.block_number >= cursor.block_number)
        {
            return Ok(());
        }
        let mut floor = cursor.clone();
        floor.transaction_index = None;
        floor.log_index = None;
        floor.source_sequence = None;
        floor.source_sub_index = None;
        self.finalized_floor = Some(floor);
        Ok(())
    }

    pub(super) fn validate_finalized_ancestor(
        &self,
        ancestor: &BlockRef,
    ) -> Result<(), IndexerError> {
        let Some(floor) = self.finalized_floor.as_ref() else {
            return Ok(());
        };
        validate_floor_coverage(floor, &ancestor.cursor, "correction")
    }

    pub(super) fn validate_finalized_recovery(
        &self,
        snapshot: &ChainCursor,
    ) -> Result<(), IndexerError> {
        let Some(floor) = self.finalized_floor.as_ref() else {
            return Ok(());
        };
        validate_floor_coverage(floor, snapshot, "recovery snapshot")?;
        if snapshot.block_number == floor.block_number
            && snapshot.commitment != Commitment::Finalized
        {
            return Err(IndexerError::Gap(
                "recovery snapshot weakened the finalized floor commitment".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn finalized_validation_checkpoint(
        &self,
        stable: &crate::model::Checkpoint,
    ) -> Option<crate::model::Checkpoint> {
        let floor = self.finalized_floor.as_ref()?;
        if floor.block_number <= stable.cursor.block_number {
            return None;
        }
        let mut identity_only = stable.clone();
        identity_only.cursor = floor.clone();
        Some(identity_only)
    }
}

fn validate_floor_coverage(
    floor: &ChainCursor,
    candidate: &ChainCursor,
    context: &str,
) -> Result<(), IndexerError> {
    if floor.chain_id != candidate.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if candidate.block_number < floor.block_number {
        return Err(IndexerError::Gap(format!(
            "{context} regressed below the finalized floor"
        )));
    }
    if candidate.block_number == floor.block_number {
        require_same_identity(floor, candidate)?;
    }
    Ok(())
}

fn require_hash(cursor: &ChainCursor) -> Result<(), IndexerError> {
    if cursor.block_hash.is_none() {
        return Err(IndexerError::Gap(
            "finalized cursor has no immutable block hash".into(),
        ));
    }
    Ok(())
}

fn require_same_identity(left: &ChainCursor, right: &ChainCursor) -> Result<(), IndexerError> {
    if left.execution_block_number != right.execution_block_number
        || left.block_hash.is_none()
        || left.block_hash != right.block_hash
    {
        return Err(ReducerError::BlockHashMismatch.into());
    }
    Ok(())
}
