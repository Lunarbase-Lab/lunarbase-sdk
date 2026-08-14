//! Construction of a dedicated source connection for the event worker.

use crate::{
    config::Config,
    metrics::Metrics,
    redis_store::RedisEventStore,
    runtime::{self, RuntimeError},
};
use lunarbase_client::model::Network;
#[cfg(feature = "evm")]
use lunarbase_client::model::SourceError;
use std::sync::Arc;
use tokio::sync::watch;

pub(crate) async fn run_selected(
    config: Arc<Config>,
    store: RedisEventStore,
    metrics: Arc<Metrics>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    match config.network {
        Network::Evm => run_evm(config, store, metrics, shutdown).await,
        Network::Base => run_base(config, store, metrics, shutdown).await,
        Network::Monad => run_monad(config, store, metrics, shutdown).await,
        Network::Arbitrum => run_arbitrum(config, store, metrics, shutdown).await,
    }
}

#[cfg(feature = "evm")]
async fn run_evm(
    config: Arc<Config>,
    store: RedisEventStore,
    metrics: Arc<Metrics>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    let rpc = lunarbase_source_evm::rpc::client::RpcHttpClient::new(config.http_rpc_url.clone())
        .map_err(SourceError::from)?;
    let source = Arc::new(
        lunarbase_source_evm::ws::EvmRpcSource::with_delivery_mode(
            rpc,
            config.realtime_url.clone(),
            Network::Evm,
            config.chain_id,
            delivery_mode(config.minimum_commitment),
        )
        .with_backfill_page_blocks(config.backfill_page_blocks),
    );
    runtime::run(source, config, store, metrics, shutdown).await
}

#[cfg(not(feature = "evm"))]
async fn run_evm(
    _config: Arc<Config>,
    _store: RedisEventStore,
    _metrics: Arc<Metrics>,
    _shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    Err(RuntimeError::UnsupportedNetwork(Network::Evm))
}

#[cfg(feature = "base")]
async fn run_base(
    config: Arc<Config>,
    store: RedisEventStore,
    metrics: Arc<Metrics>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    let rpc = lunarbase_source_evm::rpc::client::RpcHttpClient::new(config.http_rpc_url.clone())
        .map_err(SourceError::from)?;
    let source = match config.minimum_commitment {
        lunarbase_client::model::Commitment::Realtime => {
            lunarbase_source_evm::ws::EvmRpcSource::base_flashblocks(
                rpc,
                config.realtime_url.clone(),
                config.chain_id,
            )
        }
        commitment => lunarbase_source_evm::ws::EvmRpcSource::with_delivery_mode(
            rpc,
            config.realtime_url.clone(),
            Network::Base,
            config.chain_id,
            delivery_mode(commitment),
        ),
    }
    .with_backfill_page_blocks(config.backfill_page_blocks);
    let source = Arc::new(source);
    runtime::run(source, config, store, metrics, shutdown).await
}

#[cfg(not(feature = "base"))]
async fn run_base(
    _config: Arc<Config>,
    _store: RedisEventStore,
    _metrics: Arc<Metrics>,
    _shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    Err(RuntimeError::UnsupportedNetwork(Network::Base))
}

#[cfg(feature = "monad")]
async fn run_monad(
    config: Arc<Config>,
    store: RedisEventStore,
    metrics: Arc<Metrics>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    #[cfg(all(feature = "monad-native", target_os = "linux"))]
    let source = lunarbase_source_monad_native::MonadEventRingSource::new(
        lunarbase_source_monad_native::MonadEventRingConfig {
            event_ring_path: config.realtime_url.clone().into(),
            core: config.core,
            chain_id: config.chain_id,
            queue_bound: config.source_queue_bound,
            poll_interval: config.native_poll_interval,
            delivery_mode: monad_delivery_mode(config.minimum_commitment),
            emit_removed_logs: true,
        },
        config.http_rpc_url.clone(),
    )?;
    #[cfg(not(all(feature = "monad-native", target_os = "linux")))]
    let source = lunarbase_source_monad::parser::MonadParserSource::new(
        lunarbase_source_monad::parser::MonadParserConfig {
            ws_url: config.realtime_url.clone(),
            core: config.core,
            chain_id: config.chain_id,
            delivery_mode: monad_delivery_mode(config.minimum_commitment),
            emit_removed_logs: true,
            ..lunarbase_source_monad::parser::MonadParserConfig::durable_v2()
        },
        config.http_rpc_url.clone(),
    )?;
    runtime::run(Arc::new(source), config, store, metrics, shutdown).await
}

#[cfg(not(feature = "monad"))]
async fn run_monad(
    _config: Arc<Config>,
    _store: RedisEventStore,
    _metrics: Arc<Metrics>,
    _shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    Err(RuntimeError::UnsupportedNetwork(Network::Monad))
}

#[cfg(feature = "monad")]
fn monad_delivery_mode(
    commitment: lunarbase_client::model::Commitment,
) -> lunarbase_source_monad::execution::MonadDeliveryMode {
    use lunarbase_client::model::Commitment;
    use lunarbase_source_monad::execution::MonadDeliveryMode;
    match commitment {
        Commitment::Realtime => MonadDeliveryMode::Realtime,
        Commitment::Canonical => MonadDeliveryMode::BlockOrdered,
        Commitment::Finalized => MonadDeliveryMode::Finalized,
    }
}

#[cfg(feature = "arbitrum")]
async fn run_arbitrum(
    config: Arc<Config>,
    store: RedisEventStore,
    metrics: Arc<Metrics>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    let source =
        lunarbase_source_arbitrum::source::ArbitrumNitroSource::from_urls_with_delivery_mode(
            config.http_rpc_url.clone(),
            config.realtime_url.clone(),
            config.chain_id,
            delivery_mode(config.minimum_commitment),
        )?
        .with_backfill_page_blocks(config.backfill_page_blocks);
    runtime::run(Arc::new(source), config, store, metrics, shutdown).await
}

#[cfg(any(feature = "evm", feature = "base", feature = "arbitrum"))]
fn delivery_mode(
    commitment: lunarbase_client::model::Commitment,
) -> lunarbase_source_evm::ws::EvmDeliveryMode {
    use lunarbase_client::model::Commitment;
    use lunarbase_source_evm::ws::EvmDeliveryMode;
    match commitment {
        Commitment::Realtime => EvmDeliveryMode::Realtime,
        Commitment::Canonical => EvmDeliveryMode::BlockOrdered,
        Commitment::Finalized => EvmDeliveryMode::Finalized,
    }
}

#[cfg(not(feature = "arbitrum"))]
async fn run_arbitrum(
    _config: Arc<Config>,
    _store: RedisEventStore,
    _metrics: Arc<Metrics>,
    _shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    Err(RuntimeError::UnsupportedNetwork(Network::Arbitrum))
}
