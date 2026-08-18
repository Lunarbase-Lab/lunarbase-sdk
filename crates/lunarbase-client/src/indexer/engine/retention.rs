//! Owned ingestion paths that may retain or forward source payloads.

use super::QuoteIndexer;
use crate::indexer::errors::IndexerError;
use crate::model::{ChainUpdate, ContractLog};

impl QuoteIndexer {
    /// Applies one owned update through the pinned Core ABI decoder.
    pub fn apply_core_update(&mut self, mut update: ChainUpdate) -> Result<(), IndexerError> {
        self.validate_core_update_identity(&update)?;
        let logical_bytes = update.retained_bytes();
        update.normalize_for_retention();
        debug_assert_eq!(logical_bytes, update.retained_bytes());
        self.apply_validated_core_update(update)
    }

    /// Applies a log and tightly owns its payload for event delivery.
    pub(crate) fn apply_core_log_for_delivery(
        &mut self,
        mut log: ContractLog,
    ) -> Result<Option<ContractLog>, IndexerError> {
        let deliver = self.apply_core_log_borrowed(&log)?;
        if !deliver {
            return Ok(None);
        }
        log.normalize_for_retention();
        Ok(Some(log))
    }
}
