struct TestProvider;

#[async_trait]
impl SnapshotProvider for TestProvider {
    async fn snapshot(
        &self,
        config: &DeploymentConfig,
        _lane_assets: &[Address],
        _routers: &[Address],
    ) -> Result<BootstrapSnapshot, SourceError> {
        Ok(BootstrapSnapshot {
            state: QuoteState {
                cash: config.core,
                ..Default::default()
            },
            cursor: ChainCursor::block(config.chain_id, 10, None, Commitment::Finalized),
            runtime_code_hash: config.expected_runtime_code_hash,
        })
    }
}

struct PendingSource {
    core: Address,
}

#[async_trait]
impl ChainEventSource for PendingSource {
    fn network(&self) -> Network {
        Network::Base
    }

    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        Ok(ChainCursor::block(8453, 10, None, Commitment::Finalized))
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        Ok(Vec::new())
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        if filter.address != self.core {
            return Err(SourceError::NetworkMismatch);
        }
        Ok(Box::pin(stream::pending()))
    }
}

struct PendingSnapshotProvider;

#[async_trait]
impl SnapshotProvider for PendingSnapshotProvider {
    async fn snapshot(
        &self,
        _config: &DeploymentConfig,
        _lane_assets: &[Address],
        _routers: &[Address],
    ) -> Result<BootstrapSnapshot, SourceError> {
        std::future::pending().await
    }
}

struct TrackedPendingSource {
    core: Address,
    subscribed: Arc<Notify>,
    stream_dropped: Arc<AtomicBool>,
}

struct StreamDropFlag(Arc<AtomicBool>);

impl Drop for StreamDropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ChainEventSource for TrackedPendingSource {
    fn network(&self) -> Network {
        Network::Base
    }

    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        std::future::pending().await
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        std::future::pending().await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        if filter.address != self.core {
            return Err(SourceError::NetworkMismatch);
        }
        self.subscribed.notify_one();
        let dropped = self.stream_dropped.clone();
        Ok(Box::pin(async_stream::stream! {
            let _drop_flag = StreamDropFlag(dropped);
            let update = std::future::pending::<Result<ChainUpdate, SourceError>>().await;
            yield update;
        }))
    }
}

struct ControlledClosingSource {
    core: Address,
    close: Arc<Notify>,
}

#[async_trait]
impl ChainEventSource for ControlledClosingSource {
    fn network(&self) -> Network {
        Network::Base
    }

    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        Ok(ChainCursor::block(8453, 10, None, Commitment::Finalized))
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        Ok(Vec::new())
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        if filter.address != self.core {
            return Err(SourceError::NetworkMismatch);
        }
        let close = self.close.clone();
        Ok(Box::pin(async_stream::stream! {
            close.notified().await;
            if false {
                yield Ok(ChainUpdate::Head(ChainCursor::block(
                    8453,
                    10,
                    None,
                    Commitment::Realtime,
                )));
            }
        }))
    }
}

fn connected_client_config(core: Address) -> ClientConnectConfig {
    ClientConnectConfig {
        deployment: DeploymentConfig {
            network: Network::Base,
            chain_id: 8453,
            core,
            deployment_block: 1,
            expected_runtime_code_hash: [0; 32],
            contract_compatibility_version: "test".into(),
            http_rpc_url: "http://127.0.0.1:8545".into(),
            realtime_source: "test".into(),
            redis: RedisConfig::default(),
            explicit_lane_assets: Vec::new(),
            eager_routers: Vec::new(),
        },
        filter: ContractFilter {
            address: core,
            topics: Vec::new(),
        },
        lane_assets: Vec::new(),
        routers: Vec::new(),
        buffer_capacity: 8,
        reconnect_delay: Duration::from_millis(10),
    }
}

#[tokio::test]
async fn connected_client_bootstraps_with_source_started_first() {
    let core = address(1);
    let source = Arc::new(PendingSource { core });
    let config = connected_client_config(core);
    let client = ConnectedQuoteClient::connect(&TestProvider, source, config)
        .await
        .unwrap();
    client.await_ready(Commitment::Finalized).await.unwrap();
    assert!(client.health().await.ready);
    client.shutdown().await;
    assert!(!client.health().await.ready);
}

#[tokio::test]
async fn connected_client_publishes_initial_checkpoint_to_store() {
    let core = address(1);
    let source = Arc::new(PendingSource { core });
    let config = connected_client_config(core);
    let store: SharedCheckpointStore = Arc::new(tokio::sync::Mutex::new(Box::new(
        InMemoryRedisStore::new(8),
    )));
    let client = ConnectedQuoteClient::connect_with_store(
        &TestProvider,
        source,
        config,
        Some(store.clone()),
    )
    .await
    .unwrap();
    assert!(store.lock().await.load().is_some());
    client
        .shutdown_gracefully(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(store.lock().await.load().is_some());
}

#[tokio::test]
async fn cancelled_bootstrap_does_not_leave_a_detached_source_pump() {
    let core = address(1);
    let subscribed = Arc::new(Notify::new());
    let stream_dropped = Arc::new(AtomicBool::new(false));
    let source = Arc::new(TrackedPendingSource {
        core,
        subscribed: subscribed.clone(),
        stream_dropped: stream_dropped.clone(),
    });
    let connect = tokio::spawn(async move {
        ConnectedQuoteClient::connect(
            &PendingSnapshotProvider,
            source,
            connected_client_config(core),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), subscribed.notified())
        .await
        .unwrap();
    connect.abort();
    let _ = connect.await;

    tokio::time::timeout(Duration::from_secs(1), async {
        while !stream_dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn source_closure_emits_an_operational_runtime_event() {
    let core = address(1);
    let close = Arc::new(Notify::new());
    let source = Arc::new(ControlledClosingSource {
        core,
        close: close.clone(),
    });
    let client =
        ConnectedQuoteClient::connect(&TestProvider, source, connected_client_config(core))
            .await
            .unwrap();
    let mut events = client.subscribe_runtime_events();
    close.notify_one();

    let event = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = events.recv().await.unwrap();
            if event == ClientRuntimeEvent::SourceStreamClosed {
                break event;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(event.code(), "source_stream_closed");
    client
        .shutdown_gracefully(Duration::from_secs(1))
        .await
        .unwrap();
}

