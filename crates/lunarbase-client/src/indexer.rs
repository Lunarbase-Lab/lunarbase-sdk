use crate::{
    decode_core_event, BackfillRequest, BootstrapSnapshot, ChainCursor, ChainEventSource,
    ChainUpdate, Checkpoint, ClientQuote, Commitment, ContractFilter, ContractLog,
    DeploymentConfig, FreshnessPolicy, IndexerHealth, LogDecodeError, QuoteEvent, QuoteReducer,
    ReducerError, SnapshotProvider, SourceError, MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION,
};
use lunarbase_math::{
    Address, QuoteContext, QuoteError, QuoteOutcome, QuoteRequest, QuoteState, U256,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use thiserror::Error;
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum IndexerError {
    #[error("not ready")]
    NotReady,
    #[error("source gap: {0}")]
    Gap(String),
    #[error(transparent)]
    Reducer(#[from] ReducerError),
    #[error(transparent)]
    Quote(#[from] QuoteError),
    #[error(transparent)]
    Decode(#[from] LogDecodeError),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("runtime code hash mismatch")]
    CodeHashMismatch,
    #[error("requested freshness cannot be proven")]
    FreshnessUnavailable,
    #[error("no canonical cursor")]
    NoCursor,
}

#[derive(Clone, Debug)]
pub struct QuoteIndexer {
    pub reducer: QuoteReducer,
    pub expected_code_hash: [u8; 32],
    pub math_compatibility_version: String,
    pub last_commitment: Commitment,
}

impl QuoteIndexer {
    pub fn from_checkpoint(
        checkpoint: Checkpoint,
        expected_code_hash: [u8; 32],
    ) -> Result<Self, IndexerError> {
        if checkpoint.schema_version != SCHEMA_VERSION
            || checkpoint.math_compatibility_version != MATH_COMPATIBILITY_VERSION
            || checkpoint.expected_runtime_code_hash != expected_code_hash
        {
            return Err(IndexerError::CodeHashMismatch);
        }
        Ok(Self {
            last_commitment: checkpoint.cursor.commitment,
            reducer: QuoteReducer::from_checkpoint(checkpoint),
            expected_code_hash,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        })
    }

    pub fn new(state: QuoteState, expected_code_hash: [u8; 32]) -> Self {
        Self {
            reducer: QuoteReducer::new(state),
            expected_code_hash,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            last_commitment: Commitment::Realtime,
        }
    }

    pub fn bootstrap(&mut self, snapshot_cursor: ChainCursor) {
        self.reducer.bootstrap(snapshot_cursor);
    }

    pub async fn bootstrap_from_provider<P: SnapshotProvider>(
        &mut self,
        provider: &P,
        config: &DeploymentConfig,
        lane_assets: &[Address],
        routers: &[Address],
        buffered: Vec<ChainUpdate>,
    ) -> Result<(), IndexerError> {
        config.validate()?;
        if config.expected_runtime_code_hash != self.expected_code_hash {
            return Err(IndexerError::CodeHashMismatch);
        }
        let snapshot = provider.snapshot(config, lane_assets, routers).await?;
        if snapshot.runtime_code_hash != self.expected_code_hash {
            return Err(IndexerError::CodeHashMismatch);
        }
        self.bootstrap_normalized(snapshot, buffered)
    }

    /// Atomically installs a block-tagged snapshot and applies only updates
    /// strictly after that block. The caller must stop if the handoff buffer
    /// has overflowed or contains a source gap.
    pub fn bootstrap_normalized(
        &mut self,
        snapshot: BootstrapSnapshot,
        mut buffered: Vec<ChainUpdate>,
    ) -> Result<(), IndexerError> {
        if snapshot.runtime_code_hash != self.expected_code_hash {
            return Err(IndexerError::CodeHashMismatch);
        }
        let snapshot_block = snapshot.cursor.block_number;
        let snapshot_chain = snapshot.cursor.chain_id;
        buffered.sort_by_key(update_order);
        self.reducer = QuoteReducer::new(snapshot.state);
        self.reducer.bootstrap(snapshot.cursor.clone());
        self.last_commitment = snapshot.cursor.commitment;
        for update in buffered {
            match &update {
                ChainUpdate::Gap { reason, .. } => {
                    self.reducer.mark_not_ready();
                    return Err(IndexerError::Gap(reason.clone()));
                }
                ChainUpdate::Reorg { .. } => {
                    self.reducer.mark_not_ready();
                    return Err(IndexerError::Gap("reorg during bootstrap handoff".into()));
                }
                ChainUpdate::Log(log) => {
                    if log.cursor.chain_id != snapshot_chain {
                        self.reducer.mark_not_ready();
                        return Err(ReducerError::ChainIdMismatch.into());
                    }
                    if log.cursor.block_number <= snapshot_block {
                        continue;
                    }
                    self.apply_core_update(update)?;
                }
                ChainUpdate::Head(cursor) => {
                    if cursor.chain_id != snapshot_chain {
                        self.reducer.mark_not_ready();
                        return Err(ReducerError::ChainIdMismatch.into());
                    }
                    if cursor.block_number > snapshot_block {
                        self.apply_core_update(update)?;
                    }
                }
                ChainUpdate::SourceHealth { healthy, detail } => {
                    if !healthy {
                        self.reducer.mark_not_ready();
                        return Err(IndexerError::Gap(detail.clone()));
                    }
                }
            }
        }
        self.reducer.publish_ready();
        Ok(())
    }

    pub fn bootstrap_snapshot(
        &mut self,
        state: QuoteState,
        snapshot_cursor: ChainCursor,
        mut buffered: Vec<(ChainCursor, QuoteEvent)>,
    ) -> Result<(), IndexerError> {
        buffered.sort_by(|left, right| left.0.event_order().cmp(&right.0.event_order()));
        self.reducer = QuoteReducer::new(state);
        self.reducer.bootstrap(snapshot_cursor.clone());
        for (cursor, event) in buffered
            .into_iter()
            .filter(|(cursor, _)| cursor.block_number > snapshot_cursor.block_number)
        {
            if let Err(error) = self.reducer.apply(cursor, event) {
                self.reducer.mark_not_ready();
                return Err(error.into());
            }
        }
        self.last_commitment = self
            .reducer
            .cursor()
            .map_or(snapshot_cursor.commitment, |cursor| cursor.commitment);
        self.reducer.publish_ready();
        Ok(())
    }

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
                if let Some(event) = decoder(&log) {
                    if let Err(error) = self.reducer.apply(log.cursor.clone(), event) {
                        self.reducer.mark_not_ready();
                        return Err(error.into());
                    }
                    self.last_commitment = log.cursor.commitment;
                }
            }
            ChainUpdate::Head(cursor) => {
                if let Err(error) = self.reducer.observe_head(cursor) {
                    self.reducer.mark_not_ready();
                    return Err(error.into());
                }
                if let Some(current) = self.reducer.cursor() {
                    self.last_commitment = current.commitment;
                }
            }
            ChainUpdate::Reorg { .. } => {
                self.reducer.mark_not_ready();
                return Err(IndexerError::Gap(
                    "reorg requires canonical backfill".into(),
                ));
            }
            ChainUpdate::Gap { reason, .. } => {
                self.reducer.mark_not_ready();
                return Err(IndexerError::Gap(reason));
            }
            ChainUpdate::SourceHealth { healthy, .. } => {
                if !healthy {
                    self.reducer.mark_not_ready();
                }
            }
        }
        Ok(())
    }

    /// Apply a normalized Core update using the pinned ABI decoder. This is
    /// the production path; the closure-based method remains useful for
    /// modules with generated ABI bindings or deterministic tests.
    pub fn apply_core_update(&mut self, update: ChainUpdate) -> Result<(), IndexerError> {
        if let ChainUpdate::Log(log) = &update {
            let event = decode_core_event(log)?;
            return self.apply_update(update, &|_| event.clone());
        }
        self.apply_update(update, &|_| None)
    }

    pub fn resync(&mut self, checkpoint: Checkpoint) -> Result<(), IndexerError> {
        *self = Self::from_checkpoint(checkpoint, self.expected_code_hash)?;
        Ok(())
    }

    /// Recover the ordered reducer from its last canonical cursor. This path
    /// is intentionally conservative: the source must provide a canonical
    /// or finalized head, removed logs fail closed, and readiness is restored
    /// only after every backfilled log has been decoded and applied.
    pub async fn recover_from_source<S: ChainEventSource>(
        &mut self,
        source: &S,
        filter: ContractFilter,
    ) -> Result<(), IndexerError> {
        let checkpoint_cursor = self
            .reducer
            .cursor()
            .cloned()
            .ok_or(IndexerError::NoCursor)?;
        self.reducer.mark_not_ready();
        let head = source.snapshot_cursor().await?;
        if head.chain_id != checkpoint_cursor.chain_id {
            return Err(ReducerError::ChainIdMismatch.into());
        }
        if head.commitment < Commitment::Canonical {
            return Err(IndexerError::FreshnessUnavailable);
        }
        if head.block_number < checkpoint_cursor.block_number {
            return Err(IndexerError::Gap(
                "canonical source head regressed below checkpoint".into(),
            ));
        }

        let from_block = if checkpoint_cursor.transaction_index.is_none()
            && checkpoint_cursor.log_index.is_none()
        {
            checkpoint_cursor.block_number.saturating_add(1)
        } else {
            checkpoint_cursor.block_number
        };
        if from_block <= head.block_number {
            let mut logs = source
                .backfill(BackfillRequest {
                    from_block,
                    to_block: head.block_number,
                    filter,
                })
                .await?;
            logs.sort_by_key(|log| log.cursor.event_order());
            for log in logs {
                self.apply_core_update(ChainUpdate::Log(log))?;
            }
        }
        self.apply_update(ChainUpdate::Head(head), &|_| None)?;
        self.reducer.publish_ready();
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Arc<QuoteState>, IndexerError> {
        if !self.reducer.is_ready() {
            return Err(IndexerError::NotReady);
        }
        Ok(Arc::new(self.reducer.state().clone()))
    }

    pub fn quote(
        &self,
        request: &QuoteRequest,
        execution_block_number: U256,
    ) -> Result<QuoteOutcome, IndexerError> {
        let state = self.snapshot()?;
        let context = QuoteContext {
            cash: state.cash,
            execution_block_number,
            state_version: state.state_version,
        };
        Ok(lunarbase_math::quote(request, &context, &state)?)
    }

    pub fn quote_with_policy(
        &self,
        request: &QuoteRequest,
        execution_block_number: U256,
        policy: FreshnessPolicy,
    ) -> Result<ClientQuote, IndexerError> {
        let cursor = self
            .reducer
            .cursor()
            .cloned()
            .ok_or(IndexerError::NoCursor)?;
        if cursor.commitment < policy.minimum_commitment {
            return Err(IndexerError::FreshnessUnavailable);
        }
        if let Some(max_age) = policy.max_age_blocks {
            if execution_block_number > U256::from(cursor.block_number)
                && execution_block_number - U256::from(cursor.block_number) > U256::from(max_age)
            {
                return Err(IndexerError::FreshnessUnavailable);
            }
        }
        let observed_at = SystemTime::now();
        Ok(ClientQuote {
            outcome: self.quote(request, execution_block_number)?,
            cursor: cursor.clone(),
            commitment: cursor.commitment,
            observed_at,
            age: Duration::ZERO,
            stale: false,
            contract_code_hash: self.expected_code_hash,
            math_compatibility_version: self.math_compatibility_version.clone(),
        })
    }

    pub fn state_snapshot(&self) -> Result<Arc<QuoteState>, IndexerError> {
        self.snapshot()
    }

    pub fn quote_exact_in(
        &self,
        mut request: QuoteRequest,
        execution_block_number: U256,
    ) -> Result<ClientQuote, IndexerError> {
        request.mode = lunarbase_math::QuoteMode::ExactIn;
        self.quote_with_policy(&request, execution_block_number, FreshnessPolicy::default())
    }

    pub fn quote_exact_out(
        &self,
        mut request: QuoteRequest,
        execution_block_number: U256,
    ) -> Result<ClientQuote, IndexerError> {
        request.mode = lunarbase_math::QuoteMode::ExactOut;
        self.quote_with_policy(&request, execution_block_number, FreshnessPolicy::default())
    }

    pub fn health(&self) -> IndexerHealth {
        IndexerHealth {
            ready: self.reducer.is_ready(),
            commitment: self.last_commitment,
            cursor: self.reducer.cursor().cloned(),
            code_hash: self.expected_code_hash,
            math_compatibility_version: self.math_compatibility_version.clone(),
        }
    }

    pub fn shutdown(&mut self) {
        self.reducer.mark_not_ready();
    }

    pub fn checkpoint(&self) -> Option<Checkpoint> {
        self.reducer.checkpoint(self.expected_code_hash)
    }
}

fn update_order(update: &ChainUpdate) -> (u64, u32, u32, u8) {
    match update {
        ChainUpdate::Log(log) => {
            let (block, tx, log_index) = log.cursor.event_order();
            (block, tx, log_index, 1)
        }
        ChainUpdate::Head(cursor) => {
            let (block, tx, log_index) = cursor.event_order();
            (block, tx, log_index, 0)
        }
        ChainUpdate::Reorg { new_head, .. } => {
            let (block, tx, log_index) = new_head.event_order();
            (block, tx, log_index, 2)
        }
        ChainUpdate::Gap { cursor, .. } => cursor.as_ref().map_or((u64::MAX, 0, 0, 3), |cursor| {
            let (block, tx, log_index) = cursor.event_order();
            (block, tx, log_index, 3)
        }),
        ChainUpdate::SourceHealth { .. } => (0, 0, 0, 0),
    }
}
