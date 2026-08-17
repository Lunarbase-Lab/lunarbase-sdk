//! Synchronous quote state machine shared by the connected runtime.

use crate::bootstrap::{BootstrapSnapshot, VerifiedRouterSnapshot};
use crate::indexer::errors::IndexerError;
use crate::indexer::quote_types::{ClientBatchQuote, ClientQuote, IndexerHealth};
use crate::model::{
    ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractLog, DeploymentConfig,
    MATH_COMPATIBILITY_VERSION, QuoteEvent, SourceError,
};
use crate::protocol::abi::decode_core_event;
use crate::state::reducer::{QuoteReducer, ReducerError};
use lunarbase_math::arithmetic::BPS;
use lunarbase_math::{
    Address, B256, FeeClass, QuoteMode, QuotePolicy, QuoteRequest, QuoteState, U256, quote,
};
use std::ops::RangeInclusive;
use std::sync::Arc;

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

const MAX_BATCH_QUOTES: usize = 256;

/// Coherent state/cursor handle evaluated after the shared read lock is released.
pub(crate) struct PreparedQuoteSnapshot {
    state: Arc<QuoteState>,
    cursor: ChainCursor,
    implementation_code_hash: B256,
    fee_class: FeeClass,
    verified_router: Option<Arc<VerifiedRouterSnapshot>>,
}

impl PreparedQuoteSnapshot {
    fn policy_for(&self, request: &QuoteRequest) -> QuotePolicy {
        let fee_asset = match request.mode {
            QuoteMode::ExactIn => request.asset_out,
            QuoteMode::ExactOut => request.asset_in,
        };
        self.verified_router.as_ref().map_or_else(
            || QuotePolicy::base(self.fee_class),
            |verified| {
                QuotePolicy::with_verified_partner_fee(
                    self.fee_class,
                    verified
                        .partner_fee_bps
                        .get(&fee_asset)
                        .copied()
                        .unwrap_or(0),
                )
            },
        )
    }

    /// Evaluates one request against the captured immutable state.
    pub(crate) fn evaluate(self, request: &QuoteRequest) -> Result<ClientQuote, IndexerError> {
        let outcome = quote(
            request,
            self.cursor.execution_block_number,
            self.state.as_ref(),
            self.policy_for(request),
        )?;
        Ok(ClientQuote {
            outcome,
            execution_block_number: self.cursor.execution_block_number,
            cursor: self.cursor,
            implementation_code_hash: self.implementation_code_hash,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION,
            fee_class: self.fee_class,
            verified_router: self.verified_router.as_ref().map(|state| state.router),
        })
    }

    /// Evaluates a complete batch against the same immutable state and cursor.
    pub(crate) fn evaluate_many(
        self,
        requests: &[QuoteRequest],
    ) -> Result<ClientBatchQuote, IndexerError> {
        let execution_block_number = self.cursor.execution_block_number;
        let mut outcomes = Vec::with_capacity(requests.len());
        for request in requests {
            outcomes.push(quote(
                request,
                execution_block_number,
                self.state.as_ref(),
                self.policy_for(request),
            )?);
        }
        Ok(ClientBatchQuote {
            outcomes,
            cursor: self.cursor,
            execution_block_number,
            implementation_code_hash: self.implementation_code_hash,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION,
            fee_class: self.fee_class,
            verified_router: self.verified_router.as_ref().map(|state| state.router),
        })
    }
}

impl QuoteIndexer {
    pub(crate) fn canonical_floor_covers_core_log(
        &mut self,
        log: &ContractLog,
    ) -> Result<bool, IndexerError> {
        if let Err(error) =
            validate_core_log_identity(log, self.deployment.core, self.deployment.chain_id)
        {
            self.reducer.mark_not_ready();
            return Err(error);
        }
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
            reducer: QuoteReducer::new(state, deployment.fee_class, None),
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
        if deployment.verified_router.is_some() {
            return Err(IndexerError::InvalidRequest(
                "verified-router mode requires a fresh chain snapshot".into(),
            ));
        }
        let canonical_floor = checkpoint.cursor.clone();
        Ok(Self {
            reducer: QuoteReducer::from_checkpoint(checkpoint, deployment.fee_class),
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
        validate_verified_router_snapshot(&snapshot, &self.deployment)?;
        let snapshot_cursor = snapshot.cursor.clone();
        let snapshot_chain = snapshot.cursor.chain_id;
        buffered.sort_by_key(update_order);
        self.reducer = QuoteReducer::new(
            snapshot.state,
            self.deployment.fee_class,
            snapshot.verified_router,
        );
        self.reducer.bootstrap(snapshot.cursor);
        self.canonical_floor = Some(snapshot_cursor.clone());
        for update in buffered {
            self.validate_core_update_identity(&update)?;
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
            if let Err(error) = self.apply_validated_core_update(update) {
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
            self.validate_core_update_identity(&update)?;
            if let Some(cursor) = update_cursor(&update) {
                if cursor.chain_id != current.chain_id {
                    self.reducer.mark_not_ready();
                    return Err(ReducerError::ChainIdMismatch.into());
                }
                if snapshot_covers(cursor, &current)? {
                    continue;
                }
            }
            self.apply_validated_core_update(update)?;
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
        self.validate_core_update_identity(&update)?;
        self.apply_validated_update(update, decoder)
    }

    fn apply_validated_update(
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
            ChainUpdate::Head(head) => {
                if let Err(error) = self.reducer.observe_head(head.cursor) {
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

    fn validate_core_update_identity(&mut self, update: &ChainUpdate) -> Result<(), IndexerError> {
        if let ChainUpdate::Log(log) = update
            && let Err(error) =
                validate_core_log_identity(log, self.deployment.core, self.deployment.chain_id)
        {
            self.reducer.mark_not_ready();
            return Err(error);
        }
        Ok(())
    }

    /// Applies one update through the pinned Core ABI decoder.
    pub fn apply_core_update(&mut self, update: ChainUpdate) -> Result<(), IndexerError> {
        self.validate_core_update_identity(&update)?;
        self.apply_validated_core_update(update)
    }

    /// Applies a log and retains its payload allocation for event delivery.
    pub(crate) fn apply_core_log_for_delivery(
        &mut self,
        log: ContractLog,
    ) -> Result<Option<ContractLog>, IndexerError> {
        if let Err(error) =
            validate_core_log_identity(&log, self.deployment.core, self.deployment.chain_id)
        {
            self.reducer.mark_not_ready();
            return Err(error);
        }
        if log.removed {
            self.reducer.mark_not_ready();
            return Err(ReducerError::RemovedLog.into());
        }
        if let Some(floor) = self.canonical_floor.as_ref()
            && canonical_floor_covers_log(&log.cursor, floor)?
        {
            return Ok(None);
        }
        if let Some(event) = decode_core_event(&log)?
            && let Err(error) = self.reducer.apply(log.cursor.clone(), event)
        {
            self.reducer.mark_not_ready();
            return Err(error.into());
        }
        Ok(Some(log))
    }

    fn apply_validated_core_update(&mut self, update: ChainUpdate) -> Result<(), IndexerError> {
        if matches!(&update, ChainUpdate::Log(log) if log.removed) {
            return self.apply_validated_update(update, &|_| None);
        }
        if let ChainUpdate::Log(log) = &update {
            if let Some(floor) = self.canonical_floor.as_ref()
                && canonical_floor_covers_log(&log.cursor, floor)?
            {
                return Ok(());
            }
            let event = decode_core_event(log)?;
            return self.apply_validated_update(update, &|_| event.clone());
        }
        self.apply_validated_update(update, &|_| None)
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

    fn prepare_snapshot(&self) -> Result<PreparedQuoteSnapshot, IndexerError> {
        self.state()?;
        let state = self.reducer.state_snapshot();
        let cursor = self
            .reducer
            .cursor()
            .cloned()
            .ok_or(IndexerError::NoCursor)?;
        Ok(PreparedQuoteSnapshot {
            state,
            cursor,
            implementation_code_hash: self.deployment.expected_implementation_code_hash,
            fee_class: self.reducer.fee_class(),
            verified_router: self.reducer.verified_router_snapshot(),
        })
    }

    /// Captures a coherent immutable state/cursor handle for one quote.
    pub(crate) fn prepare_quote(&self) -> Result<PreparedQuoteSnapshot, IndexerError> {
        self.prepare_snapshot()
    }

    /// Captures a coherent immutable state/cursor handle for a complete batch.
    pub(crate) fn prepare_quote_many(
        &self,
        requests: &[QuoteRequest],
    ) -> Result<PreparedQuoteSnapshot, IndexerError> {
        if requests.len() > MAX_BATCH_QUOTES {
            return Err(IndexerError::InvalidRequest(format!(
                "quote_many accepts at most {MAX_BATCH_QUOTES} requests"
            )));
        }
        self.prepare_snapshot()
    }

    /// Evaluates one request against the runtime-owned execution block.
    pub fn quote(&self, request: &QuoteRequest) -> Result<ClientQuote, IndexerError> {
        self.prepare_quote()?.evaluate(request)
    }

    /// Evaluates a batch under one immutable state/cursor snapshot.
    pub fn quote_many(&self, requests: &[QuoteRequest]) -> Result<ClientBatchQuote, IndexerError> {
        self.prepare_quote_many(requests)?.evaluate_many(requests)
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
            math_compatibility_version: MATH_COMPATIBILITY_VERSION,
            fee_class: self.deployment.fee_class,
            verified_router: self.reducer.verified_router(),
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

fn validate_verified_router_snapshot(
    snapshot: &BootstrapSnapshot,
    deployment: &DeploymentConfig,
) -> Result<(), IndexerError> {
    match (
        deployment.verified_router,
        snapshot.verified_router.as_ref(),
    ) {
        (None, None) => Ok(()),
        (Some(expected), Some(actual))
            if actual.router == expected
                && actual.partner_fee_bps.len() <= snapshot.state.lanes.len().saturating_add(1)
                && actual.partner_fee_bps.iter().all(|(asset, fee)| {
                    (*asset == snapshot.state.cash || snapshot.state.lanes.contains_key(asset))
                        && U256::from(*fee) <= BPS
                }) =>
        {
            Ok(())
        }
        _ => Err(SourceError::Unavailable(
            "snapshot verified-router policy does not match deployment".into(),
        )
        .into()),
    }
}

/// Rejects source/filter violations before ABI decoding or event publication.
pub(crate) fn validate_core_log_identity(
    log: &ContractLog,
    expected_core: Address,
    expected_chain_id: u64,
) -> Result<(), IndexerError> {
    if log.address != expected_core {
        return Err(ReducerError::ContractAddressMismatch.into());
    }
    if log.cursor.chain_id != expected_chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    Ok(())
}

/// Validates canonical backfill identity and bounds before cursor filtering.
pub(crate) fn validate_core_recovery_log(
    log: &ContractLog,
    expected_core: Address,
    expected_chain_id: u64,
    block_range: RangeInclusive<u64>,
) -> Result<(), IndexerError> {
    validate_core_log_identity(log, expected_core, expected_chain_id)?;
    if log.removed
        || log.cursor.block_hash.is_none()
        || !block_range.contains(&log.cursor.block_number)
    {
        return Err(IndexerError::Gap(
            "canonical recovery backfill returned an invalid log".into(),
        ));
    }
    Ok(())
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
        ChainUpdate::Head(head) => Some(&head.cursor),
        ChainUpdate::Log(log) => Some(&log.cursor),
        ChainUpdate::Reorg { new_head, .. } => Some(&new_head.cursor),
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
#[path = "engine_tests.rs"]
mod canonical_floor_tests;
