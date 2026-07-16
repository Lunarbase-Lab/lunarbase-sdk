//! High-level quote indexer lifecycle and asynchronous client facade.
//!
//! This context coordinates source subscription, snapshot handoff, reducer
//! recovery, freshness policy, and optional checkpoint persistence. The math
//! engine itself remains in `lunarbase-math` and is called only with immutable
//! state snapshots.

use crate::{
    decode_core_event, BackfillRequest, BootstrapSnapshot, ChainCursor, ChainEventSource,
    ChainUpdate, Checkpoint, ClientQuote, Commitment, ContractFilter, ContractLog,
    DeploymentConfig, FreshnessPolicy, IndexerHealth, LogDecodeError, QuoteEvent, QuoteReducer,
    ReducerError, SharedCheckpointStore, SnapshotProvider, SourceError, MATH_COMPATIBILITY_VERSION,
    SCHEMA_VERSION,
};
use futures_util::StreamExt;
use lunarbase_math::{
    Address, QuoteContext, QuoteError, QuoteOutcome, QuoteRequest, QuoteState, U256,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::sleep;
#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Errors returned while bootstrapping, recovering, or quoting from the
/// stateful client.
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

/// Parameters shared by the high-level client lifecycle. The source is
/// started before the block-tagged snapshot so updates cannot be lost during
/// bootstrap. All queue and reconnect bounds are explicit.
#[derive(Clone, Debug)]
pub struct ClientConnectConfig {
    pub deployment: DeploymentConfig,
    pub filter: ContractFilter,
    pub lane_assets: Vec<Address>,
    pub routers: Vec<Address>,
    pub buffer_capacity: usize,
    pub reconnect_delay: Duration,
}

impl ClientConnectConfig {
    /// Validates deployment identity, source filtering, and lifecycle bounds
    /// before any background task is spawned.
    pub fn validate(&self) -> Result<(), IndexerError> {
        self.deployment.validate()?;
        if self.filter.address != self.deployment.core {
            return Err(SourceError::NetworkMismatch.into());
        }
        if self.buffer_capacity == 0 || self.reconnect_delay.is_zero() {
            return Err(SourceError::Unavailable(
                "client buffer and reconnect bounds must be non-zero".into(),
            )
            .into());
        }
        Ok(())
    }
}

/// Fully connected asynchronous client. The reducer remains single-writer;
/// quote callers only receive cloned/immutable state snapshots.
pub struct ConnectedQuoteClient {
    indexer: Arc<Mutex<QuoteIndexer>>,
    source: Arc<dyn ChainEventSource>,
    filter: ContractFilter,
    checkpoint_store: Option<SharedCheckpointStore>,
    ready: Arc<Notify>,
    stop: Option<JoinHandle<()>>,
    pump: Option<JoinHandle<()>>,
}

impl ConnectedQuoteClient {
    /// Starts the source pump, obtains a block-tagged snapshot, performs the
    /// buffered handoff, and launches the single-writer reducer loop.
    ///
    /// This convenience constructor keeps checkpoint persistence disabled;
    /// use [`Self::connect_with_store`] when durable recovery is required.
    pub async fn connect<P, S>(
        provider: &P,
        source: Arc<S>,
        config: ClientConnectConfig,
    ) -> Result<Self, IndexerError>
    where
        P: SnapshotProvider,
        S: ChainEventSource + 'static,
    {
        Self::connect_with_store(provider, source, config, None).await
    }

    /// Connect the client and optionally publish every accepted transition to
    /// a shared checkpoint store. Redis is deliberately injected at this
    /// boundary so the pure reducer remains independent of the transport.
    /// Connects the asynchronous client and optionally persists every accepted
    /// transition through a shared checkpoint store.
    ///
    /// The realtime subscription is established before the snapshot request,
    /// preventing updates produced during snapshot acquisition from being
    /// lost at the handoff boundary.
    pub async fn connect_with_store<P, S>(
        provider: &P,
        source: Arc<S>,
        config: ClientConnectConfig,
        checkpoint_store: Option<SharedCheckpointStore>,
    ) -> Result<Self, IndexerError>
    where
        P: SnapshotProvider,
        S: ChainEventSource + 'static,
    {
        config.validate()?;
        if source.network() != config.deployment.network {
            return Err(SourceError::NetworkMismatch.into());
        }
        let mut initial = QuoteIndexer::new(
            QuoteState::default(),
            config.deployment.expected_runtime_code_hash,
        );
        let (updates_tx, mut updates_rx) = mpsc::channel(config.buffer_capacity);
        let pump_source = source.clone();
        let pump_filter = config.filter.clone();
        let reconnect_delay = config.reconnect_delay;
        let pump = tokio::spawn(async move {
            source_pump(pump_source, pump_filter, updates_tx, reconnect_delay).await;
        });

        let snapshot = match provider
            .snapshot(&config.deployment, &config.lane_assets, &config.routers)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                pump.abort();
                return Err(error.into());
            }
        };
        let mut buffered = Vec::new();
        while let Ok(update) = updates_rx.try_recv() {
            buffered.push(update);
        }
        let initial_updates = buffered.clone();
        let handoff_block = snapshot.cursor.block_number;
        if let Err(error) = initial.bootstrap_normalized(snapshot, buffered) {
            pump.abort();
            return Err(error);
        }
        if let Some(store) = &checkpoint_store {
            let persisted = match initial.checkpoint() {
                Some(checkpoint) => store
                    .lock()
                    .await
                    .commit(checkpoint, initial_updates)
                    .map_err(|error| {
                        IndexerError::Source(SourceError::Unavailable(format!(
                            "checkpoint commit failed: {error}"
                        )))
                    }),
                None => Err(IndexerError::NoCursor),
            };
            if let Err(error) = persisted {
                pump.abort();
                return Err(error);
            }
        }

        let indexer = Arc::new(Mutex::new(initial));
        let ready = Arc::new(Notify::new());
        ready.notify_waiters();
        let run_indexer = indexer.clone();
        let run_source: Arc<dyn ChainEventSource> = source;
        let run_filter = config.filter;
        let run_ready = ready.clone();
        let loop_source = run_source.clone();
        let loop_filter = run_filter.clone();
        let loop_store = checkpoint_store.clone();
        let stop = tokio::spawn(async move {
            source_reducer_loop(
                run_indexer,
                loop_source,
                loop_filter,
                &mut updates_rx,
                run_ready,
                handoff_block,
                loop_store,
            )
            .await;
        });
        Ok(Self {
            indexer,
            source: run_source,
            filter: run_filter,
            checkpoint_store,
            ready,
            stop: Some(stop),
            pump: Some(pump),
        })
    }

    /// Waits until the reducer is ready at least at `minimum` commitment.
    pub async fn await_ready(&self, minimum: Commitment) -> Result<(), IndexerError> {
        loop {
            let notified = self.ready.notified();
            let health = self.health().await;
            if health.ready && health.commitment >= minimum {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Returns an immutable snapshot from the background reducer.
    pub async fn state_snapshot(&self) -> Result<Arc<QuoteState>, IndexerError> {
        self.indexer.lock().await.snapshot()
    }

    /// Computes a quote after enforcing the requested freshness policy.
    pub async fn quote_with_policy(
        &self,
        request: &QuoteRequest,
        execution_block_number: U256,
        policy: FreshnessPolicy,
    ) -> Result<ClientQuote, IndexerError> {
        self.indexer
            .lock()
            .await
            .quote_with_policy(request, execution_block_number, policy)
    }

    /// Computes an exact-input quote using the default freshness policy.
    pub async fn quote_exact_in(
        &self,
        mut request: QuoteRequest,
        execution_block_number: U256,
    ) -> Result<ClientQuote, IndexerError> {
        request.mode = lunarbase_math::QuoteMode::ExactIn;
        self.quote_with_policy(&request, execution_block_number, FreshnessPolicy::default())
            .await
    }

    /// Computes an exact-output quote using the default freshness policy.
    pub async fn quote_exact_out(
        &self,
        mut request: QuoteRequest,
        execution_block_number: U256,
    ) -> Result<ClientQuote, IndexerError> {
        request.mode = lunarbase_math::QuoteMode::ExactOut;
        self.quote_with_policy(&request, execution_block_number, FreshnessPolicy::default())
            .await
    }

    /// Returns current readiness and compatibility metadata.
    pub async fn health(&self) -> IndexerHealth {
        self.indexer.lock().await.health()
    }

    /// Returns the latest checkpoint, if the reducer has a cursor.
    pub async fn checkpoint(&self) -> Option<Checkpoint> {
        self.indexer.lock().await.checkpoint()
    }

    /// Performs canonical backfill from the current cursor and republishes a
    /// checkpoint after successful recovery.
    pub async fn resync(&self) -> Result<(), IndexerError> {
        let mut indexer = self.indexer.lock().await;
        indexer.reducer.mark_not_ready();
        let result = indexer
            .recover_from_source(self.source.as_ref(), self.filter.clone())
            .await;
        drop(indexer);
        if result.is_ok() {
            if let Err(error) =
                persist_checkpoint(&self.indexer, self.checkpoint_store.as_ref(), Vec::new()).await
            {
                self.indexer.lock().await.reducer.mark_not_ready();
                return Err(
                    SourceError::Unavailable(format!("checkpoint commit failed: {error}")).into(),
                );
            }
            self.ready.notify_waiters();
        }
        result
    }

    /// Stops background tasks and marks the client unavailable for fresh
    /// quotes.
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.stop.take() {
            handle.abort();
        }
        if let Some(handle) = self.pump.take() {
            handle.abort();
        }
        self.indexer.lock().await.shutdown();
        self.ready.notify_waiters();
    }
}

async fn source_pump(
    source: Arc<dyn ChainEventSource>,
    filter: ContractFilter,
    sender: mpsc::Sender<ChainUpdate>,
    reconnect_delay: Duration,
) {
    loop {
        let stream = match source.subscribe(filter.clone()).await {
            Ok(stream) => stream,
            Err(error) => {
                if sender
                    .send(ChainUpdate::Gap {
                        cursor: None,
                        reason: format!("source subscribe failed: {error}"),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                sleep(reconnect_delay).await;
                continue;
            }
        };
        futures_util::pin_mut!(stream);
        let mut ended_with_gap = false;
        while let Some(item) = stream.next().await {
            let update = match item {
                Ok(update) => update,
                Err(error) => ChainUpdate::Gap {
                    cursor: None,
                    reason: format!("source stream failed: {error}"),
                },
            };
            let terminal = matches!(&update, ChainUpdate::Gap { .. });
            if sender.send(update).await.is_err() {
                return;
            }
            if terminal {
                ended_with_gap = true;
                break;
            }
        }
        if !ended_with_gap
            && sender
                .send(ChainUpdate::Gap {
                    cursor: None,
                    reason: "source stream closed; canonical recovery required".into(),
                })
                .await
                .is_err()
        {
            return;
        }
        sleep(reconnect_delay).await;
    }
}

async fn source_reducer_loop(
    indexer: Arc<Mutex<QuoteIndexer>>,
    source: Arc<dyn ChainEventSource>,
    filter: ContractFilter,
    updates: &mut mpsc::Receiver<ChainUpdate>,
    ready: Arc<Notify>,
    handoff_block: u64,
    checkpoint_store: Option<SharedCheckpointStore>,
) {
    while let Some(update) = updates.recv().await {
        let skip = match &update {
            ChainUpdate::Log(log) => log.cursor.block_number <= handoff_block,
            ChainUpdate::Head(cursor) => cursor.block_number < handoff_block,
            _ => false,
        };
        if skip {
            continue;
        }
        let persisted_update = update.clone();
        let apply_result = {
            let mut indexer = indexer.lock().await;
            indexer.apply_core_update(update)
        };
        if apply_result.is_ok() {
            let persisted =
                persist_checkpoint(&indexer, checkpoint_store.as_ref(), vec![persisted_update])
                    .await;
            if persisted.is_ok() {
                ready.notify_waiters();
            } else {
                indexer.lock().await.reducer.mark_not_ready();
            }
            continue;
        }

        let recovered = {
            let mut indexer = indexer.lock().await;
            indexer
                .recover_from_source(source.as_ref(), filter.clone())
                .await
                .is_ok()
        };
        if recovered {
            if persist_checkpoint(&indexer, checkpoint_store.as_ref(), Vec::new())
                .await
                .is_ok()
            {
                ready.notify_waiters();
            } else {
                indexer.lock().await.reducer.mark_not_ready();
            }
        } else {
            indexer.lock().await.reducer.mark_not_ready();
        }
    }
    indexer.lock().await.reducer.mark_not_ready();
    ready.notify_waiters();
}

async fn persist_checkpoint(
    indexer: &Arc<Mutex<QuoteIndexer>>,
    store: Option<&SharedCheckpointStore>,
    updates: Vec<ChainUpdate>,
) -> Result<(), String> {
    let Some(store) = store else {
        return Ok(());
    };
    let checkpoint = indexer
        .lock()
        .await
        .checkpoint()
        .ok_or("cannot persist checkpoint without a cursor")?;
    store.lock().await.commit(checkpoint, updates)
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
