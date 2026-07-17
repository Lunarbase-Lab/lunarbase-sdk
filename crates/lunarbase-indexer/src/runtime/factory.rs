/// Connects the selected source, snapshot provider, and persistence store.
pub async fn connect(
    config: &ValidatedConfig,
    store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    let rpc = RpcHttpClient::new(config.deployment.http_rpc_url.clone());
    let provider = RpcSnapshotProvider::new(rpc.clone(), config.snapshot_tag.clone());
    let connect = ClientConnectConfig {
        deployment: config.deployment.clone(),
        filter: ContractFilter {
            address: config.deployment.core,
            topics: Vec::new(),
        },
        lane_assets: config.deployment.explicit_lane_assets.clone(),
        routers: config.deployment.eager_routers.clone(),
        buffer_capacity: config.runtime.buffer_capacity,
        reconnect_delay: Duration::from_millis(config.runtime.reconnect_delay_milliseconds),
    };

    match config.deployment.network {
        lunarbase_client_core::Network::Base => {
            connect_base(config, rpc, &provider, connect, store).await
        }
        lunarbase_client_core::Network::Monad => {
            connect_monad(config, &provider, connect, store).await
        }
        lunarbase_client_core::Network::Arbitrum => {
            connect_arbitrum(config, rpc, &provider, connect, store).await
        }
    }
}

/// Opens and compatibility-checks the configured checkpoint store.
pub fn build_store(
    config: &ValidatedConfig,
) -> Result<Option<SharedCheckpointStore>, RuntimeError> {
    if !config.redis_enabled {
        return Ok(None);
    }
    let store = RedisCheckpointStore::connect_with_io_timeout(
        &config.deployment.redis.url,
        config.deployment.namespace(),
        config.deployment.redis.stream_max_len,
        config.deployment.redis.dedup_ttl_seconds,
        config.redis_io_timeout,
    )
    .map_err(|error| RuntimeError::Redis(error.to_string()))?;
    store.health().map_err(RuntimeError::Redis)?;
    if store.load_meta().map_err(RuntimeError::Redis)?.is_some()
        && !store
            .validate_meta(
                config.deployment.expected_runtime_code_hash,
                MATH_COMPATIBILITY_VERSION,
            )
            .map_err(RuntimeError::Redis)?
    {
        return Err(RuntimeError::Redis(
            "existing checkpoint metadata is incompatible".into(),
        ));
    }
    let store: Box<dyn CheckpointStore> = Box::new(store);
    Ok(Some(Arc::new(tokio::sync::Mutex::new(store))))
}

#[cfg(feature = "base")]
async fn connect_base(
    config: &ValidatedConfig,
    rpc: RpcHttpClient,
    provider: &RpcSnapshotProvider,
    connect: ClientConnectConfig,
    store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    use lunarbase_client_base::{make_base_source, BaseFlashblocksBackend, BaseFlashblocksConfig};
    let backend = Arc::new(BaseFlashblocksBackend::with_config(
        rpc,
        BaseFlashblocksConfig {
            ws_url: config.deployment.realtime_source.clone(),
            max_frame_bytes: config.transport.max_frame_bytes,
            reorder_capacity: config.transport.reorder_capacity,
        },
        config.deployment.chain_id,
    ));
    let source = Arc::new(make_base_source(backend));
    Ok(ConnectedQuoteClient::connect_with_store(provider, source, connect, store).await?)
}

#[cfg(not(feature = "base"))]
async fn connect_base(
    _config: &ValidatedConfig,
    _rpc: RpcHttpClient,
    _provider: &RpcSnapshotProvider,
    _connect: ClientConnectConfig,
    _store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::FeatureDisabled("base"))
}

#[cfg(feature = "monad")]
async fn connect_monad(
    config: &ValidatedConfig,
    provider: &RpcSnapshotProvider,
    connect: ClientConnectConfig,
    store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    use lunarbase_client_monad::{MonadParserConfig, MonadParserSource, MonadRpcCanonicalBackend};
    let canonical = Arc::new(MonadRpcCanonicalBackend::new(
        config.deployment.http_rpc_url.clone(),
        config.deployment.chain_id,
    ));
    let source = Arc::new(
        MonadParserSource::new(
            MonadParserConfig {
                ws_url: config.deployment.realtime_source.clone(),
                core: config.deployment.core,
                chain_id: config.deployment.chain_id,
                max_frame_bytes: config.transport.max_frame_bytes,
            },
            canonical,
        )
        .map_err(IndexerError::from)?,
    );
    Ok(ConnectedQuoteClient::connect_with_store(provider, source, connect, store).await?)
}

#[cfg(not(feature = "monad"))]
async fn connect_monad(
    _config: &ValidatedConfig,
    _provider: &RpcSnapshotProvider,
    _connect: ClientConnectConfig,
    _store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::FeatureDisabled("monad"))
}

#[cfg(feature = "arbitrum")]
async fn connect_arbitrum(
    config: &ValidatedConfig,
    rpc: RpcHttpClient,
    provider: &RpcSnapshotProvider,
    connect: ClientConnectConfig,
    store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    use lunarbase_client_arbitrum::{make_arbitrum_source, ArbitrumNitroBackend};
    use lunarbase_client_core::WsRpcConfig;
    let mut backend = ArbitrumNitroBackend::with_config(
        rpc,
        config.deployment.realtime_source.clone(),
        config.deployment.chain_id,
        WsRpcConfig {
            max_frame_bytes: config.transport.max_frame_bytes,
            reorder_capacity: config.transport.reorder_capacity,
        },
    );
    if !config.transport.require_evm_parent_context {
        backend = backend.allow_missing_evm_parent_context();
    }
    let source = Arc::new(make_arbitrum_source(Arc::new(backend)));
    Ok(ConnectedQuoteClient::connect_with_store(provider, source, connect, store).await?)
}

#[cfg(not(feature = "arbitrum"))]
async fn connect_arbitrum(
    _config: &ValidatedConfig,
    _rpc: RpcHttpClient,
    _provider: &RpcSnapshotProvider,
    _connect: ClientConnectConfig,
    _store: Option<SharedCheckpointStore>,
) -> Result<ConnectedQuoteClient, RuntimeError> {
    Err(RuntimeError::FeatureDisabled("arbitrum"))
}
