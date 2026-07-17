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
        let (cancel, pump_cancel) = watch::channel(false);
        let (runtime_events, _) = broadcast::channel(RUNTIME_EVENT_CAPACITY);
        let stats = Arc::new(ClientRuntimeStats::new(config.buffer_capacity));
        let pump_source = source.clone();
        let pump_filter = config.filter.clone();
        let reconnect_delay = config.reconnect_delay;
        let pump_events = runtime_events.clone();
        let pump_stats = stats.clone();
        let pump = AbortOnDrop::new(tokio::spawn(async move {
            source_pump(
                pump_source,
                pump_filter,
                updates_tx,
                reconnect_delay,
                pump_cancel,
                pump_events,
                pump_stats,
            )
            .await;
        }));

        let snapshot = match provider
            .snapshot(&config.deployment, &config.lane_assets, &config.routers)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => return Err(error.into()),
        };
        let mut buffered = Vec::new();
        while let Ok(update) = updates_rx.try_recv() {
            stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
            buffered.push(update);
        }
        let initial_updates = buffered.clone();
        let handoff_block = snapshot.cursor.block_number;
        initial.bootstrap_normalized(snapshot, buffered)?;
        if let Some(store) = &checkpoint_store {
            let persisted = match initial.checkpoint() {
                Some(checkpoint) => commit_checkpoint(store, checkpoint, initial_updates, &stats)
                    .await
                    .map_err(|error| {
                        IndexerError::Source(SourceError::Unavailable(format!(
                            "checkpoint commit failed: {error}"
                        )))
                    }),
                None => Err(IndexerError::NoCursor),
            };
            persisted?;
        }

        let indexer = Arc::new(Mutex::new(initial));
        let ready = Arc::new(Notify::new());
        let available = Arc::new(AtomicBool::new(true));
        ready.notify_waiters();
        let run_indexer = indexer.clone();
        let run_source: Arc<dyn ChainEventSource> = source;
        let run_filter = config.filter;
        let run_ready = ready.clone();
        let run_available = available.clone();
        let loop_source = run_source.clone();
        let loop_filter = run_filter.clone();
        let loop_store = checkpoint_store.clone();
        let loop_cancel = cancel.subscribe();
        let loop_events = runtime_events.clone();
        let loop_stats = stats.clone();
        let stop = tokio::spawn(async move {
            source_reducer_loop(
                ReducerLoopContext {
                    indexer: run_indexer,
                    source: loop_source,
                    filter: loop_filter,
                    ready: run_ready,
                    available: run_available,
                    handoff_block,
                    checkpoint_store: loop_store,
                    runtime_events: loop_events,
                    stats: loop_stats,
                },
                &mut updates_rx,
                loop_cancel,
            )
            .await;
        });
        Ok(Self {
            indexer,
            source: run_source,
            filter: run_filter,
            checkpoint_store,
            ready,
            available,
            cancel,
            runtime_events,
            stats,
            stop: Mutex::new(Some(stop)),
            pump: Mutex::new(Some(pump.disarm())),
        })
    }

    /// Subscribes to bounded operational events without affecting indexing.
    ///
    /// A slow receiver may observe [`broadcast::error::RecvError::Lagged`];
    /// callers should alert on that condition because some diagnostics were
    /// dropped, but reducer correctness remains fail-closed.
    pub fn subscribe_runtime_events(&self) -> broadcast::Receiver<ClientRuntimeEvent> {
        self.runtime_events.subscribe()
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
        if !self.is_ready() {
            return Err(IndexerError::NotReady);
        }
        self.indexer.lock().await.snapshot()
    }

    /// Computes a quote after enforcing the requested freshness policy.
    pub async fn quote_with_policy(
        &self,
        request: &QuoteRequest,
        execution_block_number: U256,
        policy: FreshnessPolicy,
    ) -> Result<ClientQuote, IndexerError> {
        if !self.is_ready() {
            return Err(IndexerError::NotReady);
        }
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
        let mut health = self.indexer.lock().await.health();
        health.ready &= self.is_ready();
        health
    }

    /// Returns the lock-free quote availability gate used by HTTP probes.
    pub fn is_ready(&self) -> bool {
        self.available.load(Ordering::Acquire)
    }

    /// Returns the latest checkpoint, if the reducer has a cursor.
    pub async fn checkpoint(&self) -> Option<Checkpoint> {
        self.indexer.lock().await.checkpoint()
    }

    /// Samples lock-free queue, recovery, and persistence counters.
    pub fn runtime_stats(&self) -> ClientRuntimeStatsSnapshot {
        self.stats.snapshot()
    }

    /// Performs canonical backfill from the current cursor and republishes a
    /// checkpoint after successful recovery.
    pub async fn resync(&self) -> Result<(), IndexerError> {
        self.available.store(false, Ordering::Release);
        let mut indexer = self.indexer.lock().await;
        indexer.reducer.mark_not_ready();
        let result = indexer
            .recover_from_source(self.source.as_ref(), self.filter.clone())
            .await;
        drop(indexer);
        if result.is_ok() {
            if let Err(error) = persist_checkpoint(
                &self.indexer,
                self.checkpoint_store.as_ref(),
                Vec::new(),
                &self.stats,
            )
            .await
            {
                self.indexer.lock().await.reducer.mark_not_ready();
                return Err(
                    SourceError::Unavailable(format!("checkpoint commit failed: {error}")).into(),
                );
            }
            self.available.store(true, Ordering::Release);
            self.ready.notify_waiters();
        } else {
            self.stats.recovery_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Cooperatively stops background tasks using a conservative default
    /// deadline. Call [`Self::shutdown_gracefully`] when the caller needs to
    /// observe timeout, task panic, or final-checkpoint failures.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_gracefully(DEFAULT_SHUTDOWN_TIMEOUT).await;
    }

    /// Marks the client unavailable, requests cooperative cancellation, waits
    /// for both background tasks, and persists one final checkpoint.
    ///
    /// If the deadline expires, remaining tasks are aborted and joined before
    /// this method returns an error. This prevents detached workers from
    /// continuing to consume source events after process shutdown has begun.
    pub async fn shutdown_gracefully(&self, deadline: Duration) -> Result<(), IndexerError> {
        self.shutdown_inner(deadline, true).await
    }

    /// Stops all workers after distributed lease ownership has already been
    /// lost, without publishing a final checkpoint.
    ///
    /// Writing after lease loss could overwrite state produced by a newly
    /// elected writer. The last checkpoint committed while this client still
    /// owned the lease remains the only safe recovery point.
    pub async fn shutdown_after_lease_loss(&self, deadline: Duration) -> Result<(), IndexerError> {
        self.shutdown_inner(deadline, false).await
    }

    async fn shutdown_inner(
        &self,
        deadline: Duration,
        persist_final_checkpoint: bool,
    ) -> Result<(), IndexerError> {
        let started_at = Instant::now();
        let mut failures = Vec::new();
        self.available.store(false, Ordering::Release);
        let _ = self.cancel.send(true);
        match timeout(remaining_timeout(started_at, deadline), async {
            self.indexer.lock().await.shutdown();
        })
        .await
        {
            Ok(()) => {}
            Err(_) => {
                publish_runtime_event(&self.runtime_events, ClientRuntimeEvent::ShutdownTimedOut);
                failures.push("marking the reducer unavailable timed out".into());
            }
        }
        self.ready.notify_waiters();

        let mut stop = self.stop.lock().await.take();
        let mut pump = self.pump.lock().await.take();
        let joined = timeout(remaining_timeout(started_at, deadline), async {
            let stop_result = match stop.as_mut() {
                Some(handle) => Some(handle.await),
                None => None,
            };
            let pump_result = match pump.as_mut() {
                Some(handle) => Some(handle.await),
                None => None,
            };
            (stop_result, pump_result)
        })
        .await;

        match joined {
            Ok((stop_result, pump_result)) => {
                collect_join_failure("reducer", stop_result, &self.runtime_events, &mut failures);
                collect_join_failure(
                    "source-pump",
                    pump_result,
                    &self.runtime_events,
                    &mut failures,
                );
            }
            Err(_) => {
                publish_runtime_event(&self.runtime_events, ClientRuntimeEvent::ShutdownTimedOut);
                if let Some(handle) = &stop {
                    handle.abort();
                }
                if let Some(handle) = &pump {
                    handle.abort();
                }
                let forced_join = async {
                    if let Some(handle) = stop.as_mut() {
                        let _ = handle.await;
                    }
                    if let Some(handle) = pump.as_mut() {
                        let _ = handle.await;
                    }
                };
                if timeout(remaining_timeout(started_at, deadline), forced_join)
                    .await
                    .is_err()
                {
                    failures.push(
                        "aborted background tasks did not join within the shutdown deadline".into(),
                    );
                }
                failures.push("graceful shutdown timed out; remaining tasks were aborted".into());
            }
        }

        if persist_final_checkpoint {
            match timeout(
                remaining_timeout(started_at, deadline),
                persist_checkpoint(
                    &self.indexer,
                    self.checkpoint_store.as_ref(),
                    Vec::new(),
                    &self.stats,
                ),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    publish_runtime_event(
                        &self.runtime_events,
                        ClientRuntimeEvent::CheckpointFailed {
                            detail: format!("final checkpoint failed: {error}"),
                        },
                    );
                    failures.push(format!("final checkpoint failed: {error}"));
                }
                Err(_) => {
                    publish_runtime_event(
                        &self.runtime_events,
                        ClientRuntimeEvent::CheckpointFailed {
                            detail: "final checkpoint exceeded the shutdown deadline".into(),
                        },
                    );
                    failures.push("final checkpoint exceeded the shutdown deadline".into());
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(SourceError::Unavailable(failures.join("; ")).into())
        }
    }
}

impl Drop for ConnectedQuoteClient {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Ok(stop) = self.stop.try_lock() {
            if let Some(handle) = stop.as_ref() {
                handle.abort();
            }
        }
        if let Ok(pump) = self.pump.try_lock() {
            if let Some(handle) = pump.as_ref() {
                handle.abort();
            }
        }
    }
}
