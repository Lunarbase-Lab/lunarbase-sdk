//! Synchronous quote state machine shared by the connected runtime.

use crate::bootstrap::{BootstrapSnapshot, VerifiedRouterSnapshot};
use crate::indexer::errors::IndexerError;
use crate::indexer::quote_types::{ClientBatchQuote, ClientQuote, IndexerHealth};
use crate::model::{
    ChainCursor, ChainUpdate, Checkpoint, Commitment, ContractLog, DeploymentConfig,
    MATH_COMPATIBILITY_VERSION, QuoteEvent,
};
use crate::protocol::abi::decode_core_event;
use crate::state::reducer::{QuoteReducer, ReducerError};
use lunarbase_math::{B256, FeeClass, QuoteMode, QuotePolicy, QuoteRequest, QuoteState, quote};
use std::sync::Arc;

#[path = "engine/bootstrap.rs"]
mod bootstrap;
mod correction;
mod finality;
mod retention;
mod validation;
pub(crate) use bootstrap::CorrectionNotice;
use correction::OptimisticJournal;
use validation::{canonical_floor_covers_log, update_cursor, validate_verified_router_snapshot};
pub(crate) use validation::{
    snapshot_covers, sort_chain_update_refs, sort_chain_update_refs_with_indices,
    sort_chain_updates, validate_core_log_identity, validate_core_recovery_log,
};

#[derive(Clone, Debug)]
/// Synchronous state machine published as immutable lock-free snapshots.
pub struct QuoteIndexer {
    /// Ordered reducer owning the current quote-critical state and cursor.
    pub(crate) reducer: QuoteReducer,
    /// Immutable deployment identity used for compatibility and router checks.
    deployment: Arc<DeploymentConfig>,
    /// Last canonical snapshot/backfill cursor whose state already includes
    /// every quote-critical log through that block.
    canonical_floor: Option<ChainCursor>,
    /// Stable snapshot used for checkpoints; never points at an optimistic head.
    stable_checkpoint: Option<Arc<Checkpoint>>,
    /// Highest immutable finalized block identity observed from the source.
    finalized_floor: Option<ChainCursor>,
    /// Bounded copy-on-write before-images used only by the ingestion path.
    optimistic_history: OptimisticJournal,
    /// Compact semantic identity and tip of the last published correction.
    last_correction: Option<AppliedCorrection>,
}

#[derive(Clone, Debug)]
struct AppliedCorrection {
    fingerprint: B256,
    tip: ChainCursor,
}

const MAX_BATCH_QUOTES: usize = 256;

/// Coherent state/cursor handle evaluated after its immutable snapshot is released.
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
            deployment: Arc::new(deployment),
            canonical_floor: None,
            stable_checkpoint: None,
            finalized_floor: None,
            optimistic_history: OptimisticJournal::default(),
            last_correction: None,
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
        let finalized_floor =
            (canonical_floor.commitment == Commitment::Finalized).then(|| canonical_floor.clone());
        let reducer = QuoteReducer::from_checkpoint(checkpoint, deployment.fee_class);
        let mut optimistic_history = OptimisticJournal::default();
        optimistic_history.reset(canonical_floor.clone());
        Ok(Self {
            stable_checkpoint: reducer.checkpoint(&deployment).map(Arc::new),
            reducer,
            deployment: Arc::new(deployment),
            canonical_floor: Some(canonical_floor),
            finalized_floor,
            optimistic_history,
            last_correction: None,
        })
    }

    /// Installs a snapshot without a handoff batch.
    pub fn bootstrap(&mut self, snapshot: BootstrapSnapshot) -> Result<(), IndexerError> {
        self.bootstrap_normalized(snapshot, Vec::new())
    }

    /// Applies handoff messages that are strictly newer than the current
    /// cursor, preserving an already restored checkpoint state.
    pub fn apply_handoff(&mut self, mut buffered: Vec<ChainUpdate>) -> Result<(), IndexerError> {
        sort_chain_updates(&mut buffered);
        self.apply_handoff_borrowed_ordered(buffered.iter())
    }

    /// Applies an already ordered handoff while the caller retains ownership
    /// and resource permits for every queued payload.
    pub(crate) fn apply_handoff_borrowed_ordered<'a>(
        &mut self,
        buffered: impl IntoIterator<Item = &'a ChainUpdate>,
    ) -> Result<(), IndexerError> {
        let current = self
            .reducer
            .cursor()
            .cloned()
            .ok_or(IndexerError::NoCursor)?;
        for update in buffered {
            self.validate_core_update_identity(update)?;
            if let Some(cursor) = update_cursor(update) {
                if cursor.chain_id != current.chain_id {
                    self.reducer.mark_not_ready();
                    return Err(ReducerError::ChainIdMismatch.into());
                }
                if snapshot_covers(cursor, &current)? {
                    if let ChainUpdate::Correction(correction) = update {
                        self.observe_covered_correction(correction)?;
                    }
                    continue;
                }
            }
            self.apply_validated_core_update_borrowed_with_notice(update)?;
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
                let event = decoder(&log);
                if let Err(error) = self.apply_decoded_log(log.cursor, event) {
                    self.reducer.mark_not_ready();
                    return Err(error);
                }
            }
            ChainUpdate::Head(head) => {
                let cursor = head.cursor;
                self.validate_finalized_update(&cursor)?;
                if let Err(error) = self.reducer.observe_head(cursor.clone()) {
                    self.reducer.mark_not_ready();
                    return Err(error.into());
                }
                // `observe_head` deliberately ignores stale heads. Do not let a
                // late transport message relabel a retained block below the
                // published tip: only an identity that actually became (or
                // remained) the reducer head is proof for a later correction.
                if let Some(published) = self.reducer.cursor().filter(|published| {
                    published.chain_id == cursor.chain_id
                        && published.block_number == cursor.block_number
                        && published.execution_block_number == cursor.execution_block_number
                        && published.block_hash == cursor.block_hash
                }) {
                    self.optimistic_history
                        .record_head_identity(published.clone());
                }
                self.record_finalized_update(&cursor)?;
            }
            ChainUpdate::Reorg { .. } => {
                self.reducer.mark_not_ready();
                return Err(IndexerError::Gap(
                    "reorg requires canonical recovery".into(),
                ));
            }
            ChainUpdate::Correction(_) => {
                self.reducer.mark_not_ready();
                return Err(IndexerError::Gap(
                    "optimistic correction requires the correction journal".into(),
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
        let result = match update {
            ChainUpdate::Log(log) => {
                validate_core_log_identity(log, self.deployment.core, self.deployment.chain_id)
            }
            ChainUpdate::Correction(correction) => (|| {
                correction.validate()?;
                if correction.common_ancestor.cursor.chain_id != self.deployment.chain_id {
                    return Err(ReducerError::ChainIdMismatch.into());
                }
                for log in &correction.replacement_logs {
                    validate_core_log_identity(
                        log,
                        self.deployment.core,
                        self.deployment.chain_id,
                    )?;
                }
                Ok(())
            })(),
            ChainUpdate::Head(_) | ChainUpdate::Reorg { .. } | ChainUpdate::Gap { .. } => Ok(()),
        };
        if let Err(error) = result {
            self.reducer.mark_not_ready();
            return Err(error);
        }
        Ok(())
    }

    /// Applies a borrowed update so the connected runtime can retain queue
    /// ownership and its count/byte permits until the transition succeeds.
    pub(crate) fn apply_core_update_borrowed(
        &mut self,
        update: &ChainUpdate,
    ) -> Result<(), IndexerError> {
        self.validate_core_update_identity(update)?;
        match update {
            ChainUpdate::Correction(correction) => {
                self.replace_with_correction(correction).map(drop)
            }
            ChainUpdate::Log(log) => self.apply_core_log_borrowed_validated(log).map(drop),
            ChainUpdate::Head(head) => {
                self.apply_validated_update(ChainUpdate::Head(head.clone()), &|_| None)
            }
            ChainUpdate::Reorg { old_head, new_head } => self.apply_validated_update(
                ChainUpdate::Reorg {
                    old_head: old_head.clone(),
                    new_head: new_head.clone(),
                },
                &|_| None,
            ),
            ChainUpdate::Gap { cursor, reason } => self.apply_validated_update(
                ChainUpdate::Gap {
                    cursor: cursor.clone(),
                    reason: reason.clone(),
                },
                &|_| None,
            ),
        }
    }

    /// Applies a borrowed log and reports whether its owned payload should be
    /// forwarded after the caller releases the state lock.
    pub(crate) fn apply_core_log_borrowed(
        &mut self,
        log: &ContractLog,
    ) -> Result<bool, IndexerError> {
        if let Err(error) =
            validate_core_log_identity(log, self.deployment.core, self.deployment.chain_id)
        {
            self.reducer.mark_not_ready();
            return Err(error);
        }
        self.apply_core_log_borrowed_validated(log)
    }

    fn apply_core_log_borrowed_validated(
        &mut self,
        log: &ContractLog,
    ) -> Result<bool, IndexerError> {
        if log.removed {
            self.reducer.mark_not_ready();
            return Err(ReducerError::RemovedLog.into());
        }
        if let Some(floor) = self.canonical_floor.as_ref()
            && canonical_floor_covers_log(&log.cursor, floor)?
        {
            return Ok(false);
        }
        let event = decode_core_event(log)?;
        if let Err(error) = self.apply_decoded_log(log.cursor.clone(), event) {
            self.reducer.mark_not_ready();
            return Err(error);
        }
        Ok(true)
    }

    fn apply_validated_core_update(&mut self, update: ChainUpdate) -> Result<(), IndexerError> {
        if let ChainUpdate::Correction(correction) = update {
            return self.replace_with_correction(&correction).map(drop);
        }
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

    fn apply_validated_core_update_borrowed_with_notice(
        &mut self,
        update: &ChainUpdate,
    ) -> Result<Option<CorrectionNotice>, IndexerError> {
        if let ChainUpdate::Correction(correction) = update {
            let notice = CorrectionNotice::from_validated(correction);
            return self
                .replace_with_correction(correction)
                .map(|applied| applied.then_some(notice));
        }
        self.apply_core_update_borrowed(update)?;
        Ok(None)
    }

    /// Records a completed canonical recovery range. Realtime logs at or
    /// below this cursor may still be released by a source-local reorder
    /// buffer and are already represented by the installed state.
    pub(crate) fn set_canonical_floor(&mut self, cursor: ChainCursor) -> Result<(), IndexerError> {
        let current = self.reducer.cursor().ok_or(IndexerError::NoCursor)?;
        if current.chain_id != cursor.chain_id {
            self.reducer.mark_not_ready();
            return Err(ReducerError::ChainIdMismatch.into());
        }
        if cursor.transaction_index.is_some()
            || cursor.log_index.is_some()
            || current.block_number != cursor.block_number
            || current.execution_block_number != cursor.execution_block_number
            || current.block_hash.is_none()
            || current.block_hash != cursor.block_hash
        {
            self.reducer.mark_not_ready();
            return Err(ReducerError::BlockHashMismatch.into());
        }
        self.canonical_floor = Some(cursor.clone());
        self.optimistic_history.advance_floor(cursor);
        self.stable_checkpoint = self.reducer.checkpoint(&self.deployment).map(Arc::new);
        Ok(())
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
        self.stable_checkpoint.as_deref().cloned()
    }

    pub(crate) fn stable_checkpoint_handle(&self) -> Option<Arc<Checkpoint>> {
        self.stable_checkpoint.as_ref().map(Arc::clone)
    }

    /// Revokes quote readiness immediately.
    pub fn shutdown(&mut self) {
        self.reducer.mark_not_ready();
    }

    /// Returns the deployment identity used by recovery.
    pub fn deployment(&self) -> &DeploymentConfig {
        self.deployment.as_ref()
    }

    fn replace_with_correction(
        &mut self,
        correction: &crate::model::ChainCorrection,
    ) -> Result<bool, IndexerError> {
        match self.clone().into_corrected_core(correction) {
            Ok((_candidate, false)) => Ok(false),
            Ok((candidate, true)) => {
                *self = candidate;
                Ok(true)
            }
            Err(error) => {
                self.reducer.mark_not_ready();
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod canonical_floor_tests;
