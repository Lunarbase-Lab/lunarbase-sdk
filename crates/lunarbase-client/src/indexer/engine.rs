//! Synchronous quote state machine shared by the connected runtime.

use crate::bootstrap::BootstrapSnapshot;
use crate::indexer::errors::IndexerError;
use crate::indexer::quote_types::{ClientBatchQuote, ClientQuote, IndexerHealth};
use crate::model::{
    ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractLog, DeploymentConfig,
    MATH_COMPATIBILITY_VERSION, QuoteEvent,
};
use crate::protocol::abi::decode_core_event;
use crate::state::reducer::{QuoteReducer, ReducerError};
use lunarbase_math::quote::quote;
use lunarbase_math::state::{QuoteOutcome, QuoteRequest, QuoteState};

#[derive(Clone, Debug)]
/// Synchronous state machine used under the client's short `RwLock` guards.
pub struct QuoteIndexer {
    /// Ordered reducer owning the current quote-critical state and cursor.
    pub reducer: QuoteReducer,
    /// Immutable deployment identity used for compatibility and router checks.
    deployment: DeploymentConfig,
    /// Last canonical snapshot/backfill cursor whose state already includes
    /// every quote-critical log through that block.
    canonical_floor: Option<ChainCursor>,
}

impl QuoteIndexer {
    pub(crate) fn canonical_floor_covers_core_log(
        &self,
        log: &ContractLog,
    ) -> Result<bool, IndexerError> {
        if log.removed {
            return Ok(false);
        }
        self.canonical_floor.as_ref().map_or(Ok(false), |floor| {
            canonical_floor_covers_log(&log.cursor, floor)
        })
    }

    /// Creates a not-ready indexer around an empty or preloaded state.
    pub fn new(state: QuoteState, deployment: DeploymentConfig) -> Self {
        Self {
            reducer: QuoteReducer::new(state, deployment.router),
            deployment,
            canonical_floor: None,
        }
    }

    /// Restores a compatible checkpoint before canonical RPC validation.
    pub fn from_checkpoint(
        checkpoint: Checkpoint,
        deployment: DeploymentConfig,
    ) -> Result<Self, IndexerError> {
        if !checkpoint.is_compatible(&deployment) {
            return Err(IndexerError::CodeHashMismatch);
        }
        let canonical_floor = checkpoint.cursor.clone();
        Ok(Self {
            reducer: QuoteReducer::from_checkpoint(checkpoint),
            deployment,
            canonical_floor: Some(canonical_floor),
        })
    }

    /// Installs a coherent snapshot and applies buffered post-snapshot data.
    pub fn bootstrap_normalized(
        &mut self,
        snapshot: BootstrapSnapshot,
        mut buffered: Vec<ChainUpdate>,
    ) -> Result<(), IndexerError> {
        if snapshot.cursor.chain_id != self.deployment.chain_id {
            return Err(ReducerError::ChainIdMismatch.into());
        }
        if snapshot.implementation != self.deployment.expected_implementation
            || snapshot.implementation_code_hash
                != self.deployment.expected_implementation_code_hash
        {
            return Err(IndexerError::CodeHashMismatch);
        }
        let snapshot_cursor = snapshot.cursor.clone();
        let snapshot_chain = snapshot.cursor.chain_id;
        buffered.sort_by_key(update_order);
        self.reducer = QuoteReducer::new(snapshot.state, self.deployment.router);
        self.reducer.bootstrap(snapshot.cursor);
        self.canonical_floor = Some(snapshot_cursor.clone());
        for update in buffered {
            let cursor = update_cursor(&update);
            if let Some(cursor) = cursor {
                if cursor.chain_id != snapshot_chain {
                    self.reducer.mark_not_ready();
                    return Err(ReducerError::ChainIdMismatch.into());
                }
                if snapshot_covers(cursor, &snapshot_cursor)? {
                    continue;
                }
            }
            if let Err(error) = self.apply_core_update(update) {
                self.reducer.mark_not_ready();
                return Err(error);
            }
        }
        self.reducer.publish_ready();
        Ok(())
    }

    /// Installs a snapshot without a handoff batch.
    pub fn bootstrap(&mut self, snapshot: BootstrapSnapshot) -> Result<(), IndexerError> {
        self.bootstrap_normalized(snapshot, Vec::new())
    }

    /// Applies handoff messages that are strictly newer than the current
    /// cursor, preserving an already restored checkpoint state.
    pub fn apply_handoff(&mut self, mut buffered: Vec<ChainUpdate>) -> Result<(), IndexerError> {
        let current = self
            .reducer
            .cursor()
            .cloned()
            .ok_or(IndexerError::NoCursor)?;
        buffered.sort_by_key(update_order);
        for update in buffered {
            if let Some(cursor) = update_cursor(&update) {
                if cursor.chain_id != current.chain_id {
                    self.reducer.mark_not_ready();
                    return Err(ReducerError::ChainIdMismatch.into());
                }
                if snapshot_covers(cursor, &current)? {
                    continue;
                }
            }
            self.apply_core_update(update)?;
        }
        self.reducer.publish_ready();
        Ok(())
    }

    /// Applies one normalized update with a caller-supplied decoder.
    pub fn apply_update(
        &mut self,
        update: ChainUpdate,
        decoder: &impl Fn(&ContractLog) -> Option<QuoteEvent>,
    ) -> Result<(), IndexerError> {
        match update {
            ChainUpdate::Log(log) => {
                if log.removed {
                    self.reducer.mark_not_ready();
                    return Err(ReducerError::RemovedLog.into());
                }
                if let Some(event) = decoder(&log)
                    && let Err(error) = self.reducer.apply(log.cursor, event)
                {
                    self.reducer.mark_not_ready();
                    return Err(error.into());
                }
            }
            ChainUpdate::Head(cursor) => {
                if let Err(error) = self.reducer.observe_head(cursor) {
                    self.reducer.mark_not_ready();
                    return Err(error.into());
                }
            }
            ChainUpdate::Reorg { .. } => {
                self.reducer.mark_not_ready();
                return Err(IndexerError::Gap(
                    "reorg requires canonical recovery".into(),
                ));
            }
            ChainUpdate::Gap { reason, .. } => {
                self.reducer.mark_not_ready();
                return Err(IndexerError::Gap(reason));
            }
        }
        Ok(())
    }

    /// Applies one update through the pinned Core ABI decoder.
    pub fn apply_core_update(&mut self, update: ChainUpdate) -> Result<(), IndexerError> {
        if matches!(&update, ChainUpdate::Log(log) if log.removed) {
            return self.apply_update(update, &|_| None);
        }
        if let ChainUpdate::Log(log) = &update {
            if let Some(floor) = self.canonical_floor.as_ref()
                && canonical_floor_covers_log(&log.cursor, floor)?
            {
                return Ok(());
            }
            let event = decode_core_event(log)?;
            return self.apply_update(update, &|_| event.clone());
        }
        self.apply_update(update, &|_| None)
    }

    /// Records a completed canonical recovery range. Realtime logs at or
    /// below this cursor may still be released by a source-local reorder
    /// buffer and are already represented by the installed state.
    pub(crate) fn set_canonical_floor(&mut self, cursor: ChainCursor) {
        self.canonical_floor = Some(cursor);
    }

    /// Returns the ready state by reference without cloning it.
    pub fn state(&self) -> Result<&QuoteState, IndexerError> {
        if !self.reducer.is_ready() {
            return Err(IndexerError::NotReady);
        }
        Ok(self.reducer.state())
    }

    /// Evaluates one request against the runtime-owned execution block.
    pub fn quote(&self, request: &QuoteRequest) -> Result<ClientQuote, IndexerError> {
        let state = self.state()?;
        let cursor = self
            .reducer
            .cursor()
            .cloned()
            .ok_or(IndexerError::NoCursor)?;
        let outcome = quote(request, cursor.execution_block_number, state)?;
        Ok(ClientQuote {
            outcome,
            execution_block_number: cursor.execution_block_number,
            cursor,
            implementation_code_hash: self.deployment.expected_implementation_code_hash,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        })
    }

    /// Evaluates a batch under one immutable state/cursor snapshot.
    pub fn quote_many(&self, requests: &[QuoteRequest]) -> Result<ClientBatchQuote, IndexerError> {
        if requests.len() > 256 {
            return Err(IndexerError::InvalidRequest(
                "quote_many accepts at most 256 requests".into(),
            ));
        }
        let state = self.state()?;
        let cursor = self
            .reducer
            .cursor()
            .cloned()
            .ok_or(IndexerError::NoCursor)?;
        let execution_block_number = cursor.execution_block_number;
        let outcomes = requests
            .iter()
            .map(|request| quote(request, execution_block_number, state))
            .collect::<Result<Vec<QuoteOutcome>, _>>()?;
        Ok(ClientBatchQuote {
            outcomes,
            cursor,
            execution_block_number,
            implementation_code_hash: self.deployment.expected_implementation_code_hash,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        })
    }

    /// Reports readiness and current execution context.
    pub fn health(&self) -> IndexerHealth {
        let cursor = self.reducer.cursor().cloned();
        IndexerHealth {
            ready: self.reducer.is_ready(),
            commitment: cursor
                .as_ref()
                .map_or(Commitment::Realtime, |cursor| cursor.commitment),
            execution_block_number: cursor.as_ref().map(|cursor| cursor.execution_block_number),
            cursor,
            implementation_code_hash: self.deployment.expected_implementation_code_hash,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        }
    }

    /// Builds a deployment-bound checkpoint outside the quote path.
    pub fn checkpoint(&self) -> Option<Checkpoint> {
        self.reducer.checkpoint(&self.deployment)
    }

    /// Revokes quote readiness immediately.
    pub fn shutdown(&mut self) {
        self.reducer.mark_not_ready();
    }

    /// Returns the deployment identity used by recovery.
    pub fn deployment(&self) -> &DeploymentConfig {
        &self.deployment
    }
}

fn snapshot_covers(update: &ChainCursor, snapshot: &ChainCursor) -> Result<bool, IndexerError> {
    if update.chain_id != snapshot.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if update.block_number < snapshot.block_number {
        return Ok(true);
    }
    if update.block_number > snapshot.block_number {
        return Ok(false);
    }
    match (update.block_hash, snapshot.block_hash) {
        (Some(update), Some(snapshot)) if update == snapshot => Ok(true),
        (Some(_), Some(_)) => Ok(false),
        _ => Err(IndexerError::Gap(
            "same-block handoff has no hash identity; canonical recovery required".into(),
        )),
    }
}

fn canonical_floor_covers_log(
    update: &ChainCursor,
    floor: &ChainCursor,
) -> Result<bool, IndexerError> {
    if update.chain_id != floor.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if update.block_number < floor.block_number {
        return Ok(true);
    }
    if update.block_number > floor.block_number {
        return Ok(false);
    }
    match (update.block_hash, floor.block_hash) {
        (Some(update_hash), Some(floor_hash)) if update_hash == floor_hash => {
            let floor_is_block_complete =
                floor.transaction_index.is_none() && floor.log_index.is_none();
            Ok(floor_is_block_complete || update.event_order() <= floor.event_order())
        }
        (Some(_), Some(_)) => Err(ReducerError::BlockHashMismatch.into()),
        _ => Err(IndexerError::Gap(
            "same-block realtime log has no canonical hash identity".into(),
        )),
    }
}

fn update_cursor(update: &ChainUpdate) -> Option<&ChainCursor> {
    match update {
        ChainUpdate::Head(cursor) => Some(cursor),
        ChainUpdate::Log(log) => Some(&log.cursor),
        ChainUpdate::Reorg { new_head, .. } => Some(new_head),
        ChainUpdate::Gap { cursor, .. } => cursor.as_ref(),
    }
}

fn update_order(update: &ChainUpdate) -> (u64, u32, u32, u64, u32, u8) {
    let cursor = update_cursor(update);
    let order = cursor.map_or((u64::MAX, 0, 0, 0, 0), ChainCursor::event_order);
    let rank = match update {
        ChainUpdate::Head(_) => 0,
        ChainUpdate::Log(_) => 1,
        ChainUpdate::Reorg { .. } => 2,
        ChainUpdate::Gap { .. } => 3,
    };
    (order.0, order.1, order.2, order.3, order.4, rank)
}

pub(crate) fn sort_chain_updates(updates: &mut [ChainUpdate]) {
    updates.sort_by_key(update_order);
}

#[cfg(test)]
mod canonical_floor_tests {
    use super::{canonical_floor_covers_log, snapshot_covers};
    use crate::model::{ChainCursor, Commitment};
    use lunarbase_math::types::B256;

    #[test]
    fn handoff_never_covers_an_update_from_another_chain() {
        let snapshot = cursor_at(B256::new([1; 32]), 2, 3);
        let mut foreign = cursor_at(B256::new([1; 32]), 2, 2);
        foreign.chain_id = 1;
        foreign.block_number -= 1;

        assert!(matches!(
            snapshot_covers(&foreign, &snapshot),
            Err(crate::indexer::errors::IndexerError::Reducer(
                crate::state::reducer::ReducerError::ChainIdMismatch
            ))
        ));
    }

    #[test]
    fn event_level_checkpoint_covers_only_events_through_its_cursor() {
        let floor = cursor_at(B256::new([1; 32]), 2, 3);
        let covered = cursor_at(B256::new([1; 32]), 2, 2);
        let later = cursor_at(B256::new([1; 32]), 2, 4);

        assert!(canonical_floor_covers_log(&covered, &floor).unwrap());
        assert!(!canonical_floor_covers_log(&later, &floor).unwrap());
    }

    fn cursor(block_hash: B256, source_sequence: Option<u64>) -> ChainCursor {
        ChainCursor {
            chain_id: 8453,
            block_number: 100,
            execution_block_number: 100,
            block_hash: Some(block_hash),
            transaction_index: Some(2),
            log_index: Some(3),
            source_sequence,
            source_sub_index: None,
            commitment: Commitment::Realtime,
        }
    }

    fn cursor_at(block_hash: B256, transaction_index: u32, log_index: u32) -> ChainCursor {
        ChainCursor {
            transaction_index: Some(transaction_index),
            log_index: Some(log_index),
            commitment: Commitment::Canonical,
            ..cursor(block_hash, None)
        }
    }
}
