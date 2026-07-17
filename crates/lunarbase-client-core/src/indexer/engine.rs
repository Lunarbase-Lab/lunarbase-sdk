#[derive(Clone, Debug)]
pub struct QuoteIndexer {
    pub reducer: QuoteReducer,
    pub expected_code_hash: [u8; 32],
    pub math_compatibility_version: String,
    pub last_commitment: Commitment,
    last_observed_at: SystemTime,
}

impl QuoteIndexer {
    /// Restores an indexer from a checkpoint after validating schema, math
    /// compatibility, and the expected runtime bytecode hash.
    ///
    /// A mismatch is rejected before any state becomes observable because a
    /// checkpoint produced by a different contract deployment or math build
    /// is not safe to use for quotes.
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
            last_observed_at: SystemTime::now(),
        })
    }

    /// Creates an indexer around an in-memory quote state.
    ///
    /// The resulting reducer is not ready until `bootstrap` or one of the
    /// snapshot handoff methods has established a canonical cursor.
    pub fn new(state: QuoteState, expected_code_hash: [u8; 32]) -> Self {
        Self {
            reducer: QuoteReducer::new(state),
            expected_code_hash,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            last_commitment: Commitment::Realtime,
            last_observed_at: SystemTime::now(),
        }
    }

    /// Marks the reducer as bootstrapped at a block-tagged snapshot cursor.
    ///
    /// This low-level method is useful when the caller has already loaded the
    /// state. It does not replay buffered updates; use
    /// [`Self::bootstrap_normalized`] for the complete handoff protocol.
    pub fn bootstrap(&mut self, snapshot_cursor: ChainCursor) {
        self.last_commitment = snapshot_cursor.commitment;
        self.last_observed_at = SystemTime::now();
        self.reducer.bootstrap(snapshot_cursor);
    }

    /// Fetches a provider snapshot, validates its runtime code hash, and
    /// installs it together with buffered realtime updates.
    ///
    /// The source should be subscribed before calling this method so updates
    /// produced during the snapshot RPC are available in `buffered`.
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
    /// that are strictly after its block.
    ///
    /// Buffered data is ordered before application. Gaps, reorg markers,
    /// chain mismatches, and unhealthy source markers fail closed and leave
    /// the reducer not ready.
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
        self.last_observed_at = SystemTime::now();
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

    /// Installs a quote state and replays already-decoded events after the
    /// snapshot block.
    ///
    /// This variant is intended for callers that already own the ABI decoding
    /// step, such as generated bindings or deterministic tests.
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
        self.last_observed_at = SystemTime::now();
        self.reducer.publish_ready();
        Ok(())
    }

    /// Applies one normalized chain update using a caller-supplied event
    /// decoder.
    ///
    /// Non-quote logs are ignored, while removed logs, gaps, reorgs, reducer
    /// errors, and source health failures make the indexer not ready.
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
                    self.last_observed_at = SystemTime::now();
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
                self.last_observed_at = SystemTime::now();
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

    /// Replaces the current reducer with a compatibility-checked checkpoint.
    pub fn resync(&mut self, checkpoint: Checkpoint) -> Result<(), IndexerError> {
        *self = Self::from_checkpoint(checkpoint, self.expected_code_hash)?;
        Ok(())
    }

    /// Recover the ordered reducer from its last canonical cursor. This path
    /// is intentionally conservative: the source must provide a canonical
    /// or finalized head, removed logs fail closed, and readiness is restored
    /// only after every backfilled log has been decoded and applied.
    pub async fn recover_from_source<S: ChainEventSource + ?Sized>(
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
        self.last_observed_at = SystemTime::now();
        Ok(())
    }

    /// Clones the current quote state only when the reducer has a proven
    /// bootstrap/recovery cursor and is marked ready.
    pub fn snapshot(&self) -> Result<Arc<QuoteState>, IndexerError> {
        if !self.reducer.is_ready() {
            return Err(IndexerError::NotReady);
        }
        Ok(Arc::new(self.reducer.state().clone()))
    }

    /// Computes a quote against the current ready state without applying a
    /// freshness policy.
    ///
    /// `execution_block_number` is passed into the math context and therefore
    /// participates in block-delay and expiry predicates.
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

    /// Computes a quote after enforcing commitment and execution-block age
    /// requirements.
    ///
    /// The returned metadata identifies the exact cursor, code hash, and math
    /// compatibility version used by the quote.
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
        let observed_at = self.last_observed_at;
        let age = observed_at.elapsed().unwrap_or(Duration::ZERO);
        Ok(ClientQuote {
            outcome: self.quote(request, execution_block_number)?,
            cursor: cursor.clone(),
            commitment: cursor.commitment,
            observed_at,
            age,
            stale: false,
            contract_code_hash: self.expected_code_hash,
            math_compatibility_version: self.math_compatibility_version.clone(),
        })
    }

    /// Returns an immutable snapshot of the ready quote state.
    pub fn state_snapshot(&self) -> Result<Arc<QuoteState>, IndexerError> {
        self.snapshot()
    }

    /// Computes an exact-input quote using the default freshness policy.
    pub fn quote_exact_in(
        &self,
        mut request: QuoteRequest,
        execution_block_number: U256,
    ) -> Result<ClientQuote, IndexerError> {
        request.mode = lunarbase_math::QuoteMode::ExactIn;
        self.quote_with_policy(&request, execution_block_number, FreshnessPolicy::default())
    }

    /// Computes an exact-output quote using the default freshness policy.
    pub fn quote_exact_out(
        &self,
        mut request: QuoteRequest,
        execution_block_number: U256,
    ) -> Result<ClientQuote, IndexerError> {
        request.mode = lunarbase_math::QuoteMode::ExactOut;
        self.quote_with_policy(&request, execution_block_number, FreshnessPolicy::default())
    }

    /// Reports readiness, commitment, cursor, and compatibility metadata.
    pub fn health(&self) -> IndexerHealth {
        IndexerHealth {
            ready: self.reducer.is_ready(),
            commitment: self.last_commitment,
            cursor: self.reducer.cursor().cloned(),
            code_hash: self.expected_code_hash,
            math_compatibility_version: self.math_compatibility_version.clone(),
        }
    }

    /// Marks the reducer not ready and stops serving fresh quotes.
    pub fn shutdown(&mut self) {
        self.reducer.mark_not_ready();
    }

    /// Returns a durable checkpoint for the current reducer cursor, if one is
    /// available.
    pub fn checkpoint(&self) -> Option<Checkpoint> {
        self.reducer.checkpoint(self.expected_code_hash)
    }
}
