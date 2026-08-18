//! Bounded ownership of failed and buffered updates across recovery retries.

use super::{
    ClientRuntimeStats, QueuedChainUpdate, RecoverySignal, ReducerRuntime, SharedQuoteState,
};
use crate::indexer::client::publish;
use crate::indexer::errors::{ClientRuntimeEvent, IndexerError};
use crate::model::{ChainCursor, ChainUpdate, Checkpoint, Commitment};
use crate::state::reducer::ReducerError;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{broadcast, mpsc};

pub(super) struct RecoveryStage {
    queued: Vec<QueuedChainUpdate>,
    published: Option<ChainCursor>,
    required: Option<ChainCursor>,
    needs_canonical_head: bool,
    coverage_conflict: bool,
    control_generation: Option<u64>,
}

impl RecoveryStage {
    #[cfg(test)]
    pub(super) fn new(failed: QueuedChainUpdate, prior: Option<ChainCursor>) -> Self {
        Self::new_segment(vec![failed], 0, prior)
    }

    pub(super) fn new_segment(
        queued: Vec<QueuedChainUpdate>,
        failed_index: usize,
        prior: Option<ChainCursor>,
    ) -> Self {
        assert!(!queued.is_empty(), "recovery stage must retain an update");
        assert!(
            failed_index < queued.len(),
            "failed update index must belong to the retained segment"
        );
        let mut stage = Self {
            queued: Vec::with_capacity(queued.len()),
            published: prior,
            required: None,
            needs_canonical_head: false,
            coverage_conflict: false,
            control_generation: None,
        };
        for (index, update) in queued.into_iter().enumerate() {
            stage.push(update, index == failed_index);
        }
        stage
    }

    pub(super) fn absorb(&mut self, receiver: &mut mpsc::Receiver<QueuedChainUpdate>) {
        while let Ok(update) = receiver.try_recv() {
            self.push(update, false);
        }
    }

    fn push(&mut self, update: QueuedChainUpdate, failed: bool) {
        if failed {
            match update.update() {
                ChainUpdate::Gap { cursor: None, .. } => self.needs_canonical_head = true,
                update => {
                    if let Some(cursor) = update_cursor(update) {
                        self.coverage_conflict |=
                            merge_required(&mut self.required, cursor.clone());
                    }
                }
            }
        }
        if matches!(update.update(), ChainUpdate::Gap { cursor: None, .. }) {
            self.needs_canonical_head = true;
        }
        self.queued.push(update);
    }

    pub(super) const fn needs_canonical_head(&self) -> bool {
        self.needs_canonical_head
    }

    pub(super) fn set_canonical_head(&mut self, head: ChainCursor) {
        for queued in &mut self.queued {
            if let ChainUpdate::Gap { cursor, .. } = queued.update_mut()
                && cursor.is_none()
            {
                *cursor = Some(head.clone());
            }
        }
        self.coverage_conflict |= merge_required(&mut self.required, head);
        self.needs_canonical_head = false;
    }

    pub(super) fn snapshot_covers(&self, snapshot: &ChainCursor) -> Result<bool, IndexerError> {
        if self.coverage_conflict {
            return Err(ReducerError::BlockHashMismatch.into());
        }
        if let Some(published) = self.published.as_ref()
            && !published_snapshot_covers(published, snapshot)?
        {
            return Ok(false);
        }
        self.required.as_ref().map_or(Ok(true), |required| {
            published_snapshot_covers(required, snapshot)
        })
    }

    /// Builds only a pointer-sized attempt view; staged payload ownership and
    /// queue permits remain in this object until installation succeeds.
    pub(super) fn borrowed_updates(&self) -> Vec<&ChainUpdate> {
        self.queued.iter().map(QueuedChainUpdate::update).collect()
    }

    #[cfg(test)]
    pub(super) fn updates(&self) -> impl Iterator<Item = &ChainUpdate> {
        self.queued.iter().map(QueuedChainUpdate::update)
    }

    /// Commits successful recovery ownership without cloning payloads. Queue
    /// permits and counters are released before logs move to the observer.
    pub(super) fn into_owned_updates(mut self) -> Vec<ChainUpdate> {
        std::mem::take(&mut self.queued)
            .into_iter()
            .map(QueuedChainUpdate::dequeue)
            .collect()
    }

    pub(super) fn require_all_updates(&mut self) {
        let required = &mut self.required;
        let mut conflict = self.coverage_conflict;
        for queued in &self.queued {
            match queued.update() {
                ChainUpdate::Gap { cursor: None, .. } => self.needs_canonical_head = true,
                update => {
                    if let Some(cursor) = update_cursor(update) {
                        conflict |= merge_required(required, cursor.clone());
                    }
                }
            }
        }
        self.coverage_conflict = conflict;
    }

    pub(super) fn merge_control(&mut self, signal: &RecoverySignal) {
        if self.control_generation == Some(signal.generation) {
            return;
        }
        self.control_generation = Some(signal.generation);
        self.coverage_conflict |= signal.conflict;
        self.needs_canonical_head |= signal.needs_canonical_head;
        if let Some(required) = signal.required.clone() {
            self.coverage_conflict |= merge_required(&mut self.required, required);
        }
    }

    pub(super) fn control_generation(&self) -> u64 {
        self.control_generation.unwrap_or_default()
    }
}

pub(super) fn merge_required(required: &mut Option<ChainCursor>, candidate: ChainCursor) -> bool {
    let Some(current) = required.as_ref().cloned() else {
        *required = Some(candidate);
        return false;
    };
    if current.chain_id != candidate.chain_id {
        return true;
    }
    if current.block_number == candidate.block_number {
        let same_identity = match (current.block_hash, candidate.block_hash) {
            (Some(current_hash), Some(candidate_hash)) if current_hash == candidate_hash => {
                if current.execution_block_number != candidate.execution_block_number {
                    return true;
                }
                true
            }
            (Some(_), Some(_)) => {
                if current.commitment == Commitment::Finalized
                    || candidate.commitment == Commitment::Finalized
                {
                    return true;
                }
                false
            }
            _ => return true,
        };
        if same_identity && candidate.event_order() < current.event_order() {
            if candidate.commitment > current.commitment {
                let mut promoted = current;
                promoted.commitment = candidate.commitment;
                *required = Some(promoted);
            }
            return false;
        }
    }
    let current_order = current.event_order();
    let mut candidate = candidate;
    let candidate_order = candidate.event_order();
    if candidate_order < current_order {
        return false;
    }
    if current.block_number == candidate.block_number && current.block_hash == candidate.block_hash
    {
        candidate.commitment = candidate.commitment.max(current.commitment);
    }
    if candidate_order == current_order && candidate.commitment < current.commitment {
        return false;
    }
    *required = Some(candidate);
    false
}

fn published_snapshot_covers(
    published: &ChainCursor,
    snapshot: &ChainCursor,
) -> Result<bool, IndexerError> {
    if published.chain_id != snapshot.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if published.block_number < snapshot.block_number {
        return Ok(true);
    }
    if published.block_number > snapshot.block_number {
        return Ok(false);
    }
    match (published.block_hash, snapshot.block_hash) {
        (Some(published_hash), Some(snapshot_hash)) if published_hash == snapshot_hash => {
            if published.execution_block_number != snapshot.execution_block_number {
                return Err(ReducerError::BlockHashMismatch.into());
            }
            Ok(snapshot.commitment >= published.commitment)
        }
        (Some(_), Some(_)) if published.commitment == Commitment::Finalized => {
            Err(ReducerError::BlockHashMismatch.into())
        }
        (Some(_), Some(_)) => Ok(snapshot.commitment >= Commitment::Canonical
            && snapshot.commitment >= published.commitment),
        _ => Err(IndexerError::Gap(
            "same-height recovery barrier has no immutable block identity".into(),
        )),
    }
}

pub(super) fn update_cursor(update: &ChainUpdate) -> Option<&ChainCursor> {
    match update {
        ChainUpdate::Head(head) => Some(&head.cursor),
        ChainUpdate::Log(log) => Some(&log.cursor),
        ChainUpdate::Reorg { new_head, .. } => Some(&new_head.cursor),
        ChainUpdate::Correction(correction) => Some(&correction.new_tip.cursor),
        ChainUpdate::Gap { cursor, .. } => cursor.as_ref(),
    }
}

pub(super) fn record_failure(
    error: IndexerError,
    events: &broadcast::Sender<ClientRuntimeEvent>,
    stats: &ClientRuntimeStats,
) {
    stats.recovery_failures.fetch_add(1, Ordering::Relaxed);
    publish(
        events,
        ClientRuntimeEvent::RecoveryFailed {
            detail: error.to_string(),
        },
    );
}

pub(super) fn stable_checkpoint(
    shared: &SharedQuoteState,
    runtime: &ReducerRuntime,
) -> Option<Arc<Checkpoint>> {
    match shared.load_indexer() {
        Ok(indexer) => indexer.stable_checkpoint_handle().or_else(|| {
            record_failure(IndexerError::NoCursor, &runtime.events, &runtime.stats);
            None
        }),
        Err(_) => {
            record_failure(IndexerError::LockPoisoned, &runtime.events, &runtime.stats);
            None
        }
    }
}

pub(super) fn finalized_validation_checkpoint(
    shared: &SharedQuoteState,
    stable: &Checkpoint,
) -> Result<Option<Checkpoint>, IndexerError> {
    let indexer = shared.load_indexer()?;
    Ok(indexer.finalized_validation_checkpoint(stable))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Commitment, ContractLog};
    use lunarbase_math::{Address, B256, Bytes};

    #[tokio::test]
    async fn recovery_attempt_borrows_large_payload_and_keeps_its_permit() {
        let stats = ClientRuntimeStats::new(4, 1024 * 1024);
        let update = ChainUpdate::Log(ContractLog {
            address: Address::new([1; 20]),
            transaction_hash: Some(B256::new([2; 32])),
            topics: vec![B256::new([3; 32]); 8_192],
            data: Bytes::from(vec![4; 256 * 1024]),
            removed: false,
            cursor: positioned_cursor(101, B256::new([5; 32]), Commitment::Realtime),
        });
        let bytes = QueuedChainUpdate::retained_bytes(&update);
        let charge = bytes.max(stats.queue_item_byte_floor);
        let permit = stats
            .queue_byte_budget
            .clone()
            .acquire_many_owned(charge as u32)
            .await
            .unwrap();
        let queued = QueuedChainUpdate::new(update, bytes, permit, stats.queue_accounting());
        let stage = RecoveryStage::new(queued, None);

        let attempt = stage.borrowed_updates();
        let original = stage.updates().next().unwrap();
        assert!(std::ptr::eq(attempt[0], original));
        let (attempt_topics, attempt_data) = match attempt[0] {
            ChainUpdate::Log(log) => (log.topics.as_ptr(), log.data.as_ptr()),
            _ => unreachable!(),
        };
        let (original_topics, original_data) = match original {
            ChainUpdate::Log(log) => (log.topics.as_ptr(), log.data.as_ptr()),
            _ => unreachable!(),
        };
        assert_eq!(attempt_topics, original_topics);
        assert_eq!(attempt_data, original_data);
        assert_eq!(stats.queue_bytes(), bytes);
        assert_eq!(
            stats.queue_byte_budget.available_permits(),
            stats.queue_byte_capacity.saturating_sub(charge)
        );

        drop(attempt);
        let owned = stage.into_owned_updates();
        assert_eq!(stats.queue_depth(), 0);
        assert_eq!(stats.queue_bytes(), 0);
        assert_eq!(
            stats.queue_byte_budget.available_permits(),
            stats.queue_byte_capacity
        );
        let moved = match &owned[0] {
            ChainUpdate::Log(log) => (log.topics.as_ptr(), log.data.as_ptr()),
            _ => unreachable!(),
        };
        assert_eq!(moved, (original_topics, original_data));
    }

    #[tokio::test]
    async fn equal_order_failed_cursor_cannot_overwrite_a_finalized_barrier() {
        let stats = ClientRuntimeStats::new(4, 1024 * 1024);
        let published = ChainCursor::block(1, 105, Some(B256::new([1; 32])), Commitment::Finalized);
        let replacement =
            ChainCursor::block(1, 105, Some(B256::new([2; 32])), Commitment::Finalized);
        let update = ChainUpdate::Gap {
            cursor: Some(replacement.clone()),
            reason: "conflicting finalized identity".into(),
        };
        let bytes = QueuedChainUpdate::retained_bytes(&update);
        let permit = stats
            .queue_byte_budget
            .clone()
            .acquire_many_owned(bytes.max(stats.queue_item_byte_floor) as u32)
            .await
            .unwrap();
        let stage = RecoveryStage::new(
            QueuedChainUpdate::new(update, bytes, permit, stats.queue_accounting()),
            Some(published),
        );

        assert!(matches!(
            stage.snapshot_covers(&replacement),
            Err(IndexerError::Reducer(ReducerError::BlockHashMismatch))
        ));
    }

    #[test]
    fn nonfinal_branch_replacement_may_change_execution_context() {
        let mut required = Some(ChainCursor::execution_block(
            42161,
            105,
            1_001,
            Some(B256::new([1; 32])),
            Commitment::Realtime,
        ));
        let candidate = ChainCursor::execution_block(
            42161,
            105,
            2_001,
            Some(B256::new([2; 32])),
            Commitment::Canonical,
        );

        assert!(!merge_required(&mut required, candidate.clone()));
        assert_eq!(required, Some(candidate));
    }

    #[tokio::test]
    async fn failed_install_promotes_all_buffered_updates_to_required_coverage() {
        let stats = ClientRuntimeStats::new(4, 1024 * 1024);
        let failed = ChainUpdate::Gap {
            cursor: Some(ChainCursor::block(
                1,
                101,
                Some(B256::new([1; 32])),
                Commitment::Canonical,
            )),
            reason: "initial barrier".into(),
        };
        let later = ChainUpdate::Log(ContractLog {
            address: Address::new([1; 20]),
            transaction_hash: Some(B256::new([2; 32])),
            topics: Vec::new(),
            data: Bytes::new(),
            removed: false,
            cursor: positioned_cursor(106, B256::new([6; 32]), Commitment::Realtime),
        });
        let failed_bytes = QueuedChainUpdate::retained_bytes(&failed);
        let later_bytes = QueuedChainUpdate::retained_bytes(&later);
        let failed_charge = failed_bytes.max(stats.queue_item_byte_floor);
        let later_charge = later_bytes.max(stats.queue_item_byte_floor);
        let failed_permit = stats
            .queue_byte_budget
            .clone()
            .acquire_many_owned(failed_charge as u32)
            .await
            .unwrap();
        let later_permit = stats
            .queue_byte_budget
            .clone()
            .acquire_many_owned(later_charge as u32)
            .await
            .unwrap();
        let mut stage = RecoveryStage::new(
            QueuedChainUpdate::new(
                failed,
                failed_bytes,
                failed_permit,
                stats.queue_accounting(),
            ),
            None,
        );
        stage.push(
            QueuedChainUpdate::new(later, later_bytes, later_permit, stats.queue_accounting()),
            false,
        );
        let stale = ChainCursor::block(1, 101, Some(B256::new([1; 32])), Commitment::Canonical);
        assert!(stage.snapshot_covers(&stale).unwrap());

        stage.require_all_updates();
        assert!(!stage.snapshot_covers(&stale).unwrap());
        assert!(
            stage
                .snapshot_covers(&ChainCursor::block(
                    1,
                    106,
                    Some(B256::new([6; 32])),
                    Commitment::Canonical,
                ))
                .unwrap()
        );
    }

    #[test]
    fn realtime_snapshot_cannot_regress_a_published_canonical_barrier() {
        let published = ChainCursor::block(1, 105, Some(B256::new([1; 32])), Commitment::Canonical);
        let same_identity_realtime =
            ChainCursor::block(1, 105, Some(B256::new([1; 32])), Commitment::Realtime);
        let conflicting_realtime =
            ChainCursor::block(1, 105, Some(B256::new([2; 32])), Commitment::Realtime);
        assert!(!published_snapshot_covers(&published, &same_identity_realtime).unwrap());
        assert!(!published_snapshot_covers(&published, &conflicting_realtime).unwrap());

        let canonical_replacement = ChainCursor {
            commitment: Commitment::Canonical,
            ..conflicting_realtime
        };
        assert!(published_snapshot_covers(&published, &canonical_replacement).unwrap());
    }

    #[test]
    fn required_merge_never_downgrades_commitment_at_one_position() {
        let mut required = Some(ChainCursor::block(
            1,
            105,
            Some(B256::new([1; 32])),
            Commitment::Finalized,
        ));
        assert!(!merge_required(
            &mut required,
            ChainCursor::block(1, 105, Some(B256::new([1; 32])), Commitment::Realtime,),
        ));
        assert_eq!(required.unwrap().commitment, Commitment::Finalized);
    }

    #[test]
    fn required_merge_checks_block_identity_across_different_log_positions() {
        let mut finalized = positioned_cursor(105, B256::new([1; 32]), Commitment::Finalized);
        finalized.log_index = Some(0);
        let mut later = positioned_cursor(105, B256::new([2; 32]), Commitment::Realtime);
        later.log_index = Some(1);
        let mut required = Some(finalized);

        assert!(merge_required(&mut required, later));
        assert_eq!(required.unwrap().block_hash, Some(B256::new([1; 32])));
    }

    fn positioned_cursor(block: u64, hash: B256, commitment: Commitment) -> ChainCursor {
        let mut cursor = ChainCursor::block(1, block, Some(hash), commitment);
        cursor.transaction_index = Some(0);
        cursor.log_index = Some(0);
        cursor
    }
}
