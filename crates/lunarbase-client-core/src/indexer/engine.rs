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
}

impl QuoteIndexer {
    /// Creates a not-ready indexer around an empty or preloaded state.
    pub fn new(state: QuoteState, deployment: DeploymentConfig) -> Self {
        Self {
            reducer: QuoteReducer::new(state, deployment.router),
            deployment,
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
        Ok(Self {
            reducer: QuoteReducer::from_checkpoint(checkpoint),
            deployment,
        })
    }

    /// Installs a coherent snapshot and applies buffered post-snapshot data.
    pub fn bootstrap_normalized(
        &mut self,
        snapshot: BootstrapSnapshot,
        mut buffered: Vec<ChainUpdate>,
    ) -> Result<(), IndexerError> {
        if snapshot.runtime_code_hash != self.deployment.expected_runtime_code_hash {
            return Err(IndexerError::CodeHashMismatch);
        }
        let snapshot_cursor = snapshot.cursor.clone();
        let snapshot_chain = snapshot.cursor.chain_id;
        buffered.sort_by_key(update_order);
        self.reducer = QuoteReducer::new(snapshot.state, self.deployment.router);
        self.reducer.bootstrap(snapshot.cursor);
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
        if let ChainUpdate::Log(log) = &update {
            let event = decode_core_event(log)?;
            return self.apply_update(update, &|_| event.clone());
        }
        self.apply_update(update, &|_| None)
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
            contract_code_hash: self.deployment.expected_runtime_code_hash,
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
            contract_code_hash: self.deployment.expected_runtime_code_hash,
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
            code_hash: self.deployment.expected_runtime_code_hash,
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
