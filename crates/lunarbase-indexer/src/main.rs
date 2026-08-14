//! Runnable LunarBase realtime indexer and quote service.

mod api;
mod checkpoint;
mod config;
mod metrics;
mod runtime;

use alloy_primitives::keccak256;
use checkpoint::RedisCheckpointStore;
use clap::Parser;
use config::{Cli, Config};
use lunarbase_client::indexer::errors::ClientRuntimeEvent;
use lunarbase_client::model::{Commitment, ContractLog, Network};
use lunarbase_client::protocol::abi::describe_core_event;
use metrics::Metrics;
use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
    time::timeout,
};
use tracing_subscriber::EnvFilter;

const EVENT_DEDUP_CAPACITY_MULTIPLIER: usize = 4;

#[tokio::main]
async fn main() {
    init_tracing();
    if let Err(error) = run().await {
        tracing::error!(error = %error, "lunarbase-indexer stopped with an error");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let config = Config::load(&cli)?;
    tracing::info!(
        minimum_commitment = ?config.event_log_min_commitment,
        reducer_before_event_sink = true,
        "Core event logging policy configured"
    );
    if config.event_log_min_commitment > Commitment::Realtime
        && matches!(
            config.client.deployment.network,
            Network::Evm | Network::Base | Network::Arbitrum
        )
    {
        tracing::warn!(
            network = ?config.client.deployment.network,
            minimum_commitment = ?config.event_log_min_commitment,
            "live source emits realtime Core logs; higher-commitment event output is normally limited to canonical recovery/backfill"
        );
    }
    let metrics = Arc::new(Metrics::default());
    let signal = wait_for_signal();
    tokio::pin!(signal);
    let store = config
        .redis_url
        .as_ref()
        .map(|url| RedisCheckpointStore::new(url.clone(), &config.client.deployment));
    let checkpoint = tokio::select! {
        result = runtime::load_checkpoint(store.as_ref(), &metrics) => result,
        result = &mut signal => {
            result?;
            tracing::info!("shutdown received before checkpoint load completed");
            return Ok(());
        }
    };
    let event_capacity = config.client.buffer_capacity;
    let event_dedup_capacity = event_capacity.saturating_mul(EVENT_DEDUP_CAPACITY_MULTIPLIER);
    let (core_event_tx, core_event_rx) = mpsc::channel(event_capacity);
    let mut event_task = tokio::spawn(log_core_events(
        core_event_rx,
        metrics.clone(),
        event_dedup_capacity,
    ));
    let client_result = tokio::select! {
        result = runtime::connect_client(&config, checkpoint, core_event_tx) => result,
        result = &mut signal => {
            result?;
            event_task.abort();
            let _ = event_task.await;
            tracing::info!("shutdown received during client bootstrap");
            return Ok(());
        }
        result = &mut event_task => {
            return Err(std::io::Error::other(match result {
                Ok(()) => "Core event logger stopped during client bootstrap".into(),
                Err(error) => format!("Core event logger failed during client bootstrap: {error}"),
            }).into());
        }
    };
    let client = match client_result {
        Ok(client) => Arc::new(client),
        Err(error) => {
            event_task.abort();
            let _ = event_task.await;
            return Err(error.into());
        }
    };
    let mut runtime_events = client.subscribe_runtime_events();

    tracing::info!(
        network = ?config.client.deployment.network,
        chain_id = config.client.deployment.chain_id,
        core = %config.client.deployment.core,
        fee_class = ?config.client.deployment.fee_class,
        verified_router = ?config.client.deployment.verified_router,
        bind = %config.bind,
        redis = store.is_some(),
        event_log_min_commitment = ?config.event_log_min_commitment,
        "lunarbase-indexer is ready"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let checkpoint_task = runtime::spawn_checkpoint_loop(
        client.clone(),
        store.clone(),
        config.checkpoint_interval,
        metrics.clone(),
        shutdown_rx.clone(),
    );
    let mut api_task = tokio::spawn(api::serve(
        config.bind,
        client.clone(),
        metrics.clone(),
        wait_for_shutdown(shutdown_rx),
    ));
    let mut api_finished = false;
    let mut event_logger_finished = false;
    let mut failure = tokio::select! {
        result = &mut signal => {
            result?;
            None
        }
        result = &mut api_task => {
            api_finished = true;
            Some(match result {
                Ok(Ok(())) => "HTTP API stopped unexpectedly".into(),
                Ok(Err(error)) => format!("HTTP API failed: {error}"),
                Err(error) => format!("HTTP API task failed: {error}"),
            })
        }
        detail = wait_for_runtime_failure(&mut runtime_events) => Some(detail),
        result = &mut event_task => {
            event_logger_finished = true;
            Some(match result {
                Ok(()) => "Core event logger stopped unexpectedly".into(),
                Err(error) => format!("Core event logger task failed: {error}"),
            })
        }
    };
    tracing::info!(failure = failure.as_deref(), "graceful shutdown started");
    let _ = shutdown_tx.send(true);
    let deadline = Instant::now() + config.shutdown_timeout;

    if let Some(store) = &store
        && timeout(
            remaining(deadline),
            runtime::flush_checkpoint(&client, store, &metrics),
        )
        .await
        .is_err()
    {
        metrics.checkpoint_failure();
        tracing::warn!("final Redis checkpoint exceeded shutdown deadline");
    }
    if let Err(error) = client.shutdown_gracefully(remaining(deadline)).await {
        failure.get_or_insert_with(|| format!("client shutdown failed: {error}"));
    }
    if !event_logger_finished && let Err(detail) = join_unit_task(event_task, deadline).await {
        failure.get_or_insert_with(|| format!("Core event logger {detail}"));
    }
    if let Err(detail) = join_unit_task(checkpoint_task, deadline).await {
        failure.get_or_insert_with(|| format!("checkpoint task {detail}"));
    }
    if !api_finished && let Err(detail) = join_api_task(api_task, deadline).await {
        failure.get_or_insert_with(|| format!("HTTP API task {detail}"));
    }
    tracing::info!("graceful shutdown complete");
    if let Some(detail) = failure {
        Err(std::io::Error::other(detail).into())
    } else {
        Ok(())
    }
}

async fn wait_for_runtime_failure(
    receiver: &mut broadcast::Receiver<ClientRuntimeEvent>,
) -> String {
    loop {
        match receiver.recv().await {
            Ok(ClientRuntimeEvent::BackgroundTaskStopped { task }) => {
                return format!("required client task `{task}` stopped");
            }
            Ok(ClientRuntimeEvent::BackgroundTaskPanicked { task, detail }) => {
                return format!("required client task `{task}` panicked: {detail}");
            }
            Ok(event) => log_runtime_event(&event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(
                    skipped,
                    "runtime event consumer lagged behind its bounded channel"
                );
            }
            Err(broadcast::error::RecvError::Closed) => {
                return "client runtime event channel closed".into();
            }
        }
    }
}

fn log_runtime_event(event: &ClientRuntimeEvent) {
    let code = event.code();
    let detail = event.detail();
    match event {
        ClientRuntimeEvent::RecoveryCompleted => {
            tracing::info!(event = code, detail, "client runtime event");
        }
        ClientRuntimeEvent::RecoveryStarted => {
            tracing::warn!(event = code, detail, "client runtime event");
        }
        _ => tracing::warn!(event = code, detail, "client runtime event"),
    }
}

async fn log_core_events(
    mut receiver: mpsc::Receiver<ContractLog>,
    metrics: Arc<Metrics>,
    dedup_capacity: usize,
) {
    let mut dedup = EventDedup::new(dedup_capacity);
    while let Some(log) = receiver.recv().await {
        let event_id = core_event_id(&log);
        if !dedup.insert(event_id.clone()) {
            metrics.core_event_duplicate();
            continue;
        }
        log_core_event(&log, &event_id);
        metrics.core_event_logged();
    }
}

fn log_core_event(log: &ContractLog, event_id: &str) {
    let (event_name, arguments, decode_error) = match describe_core_event(log) {
        Ok(Some(description)) => (description.name, description.arguments, String::new()),
        Ok(None) => ("Unknown", String::new(), String::new()),
        Err(error) => ("Malformed", String::new(), error.to_string()),
    };
    let block_hash = log
        .cursor
        .block_hash
        .map(|hash| format!("{hash:#x}"))
        .unwrap_or_default();
    let transaction_hash = log
        .transaction_hash
        .map(|hash| format!("{hash:#x}"))
        .unwrap_or_default();
    let topic0 = log
        .topics
        .first()
        .map(|topic| format!("{topic:#x}"))
        .unwrap_or_default();
    let topics = serde_json::to_string(&log.topics).unwrap_or_else(|_| "[]".into());
    let data = format!("{:#x}", log.data);

    tracing::info!(
        target: "lunarbase_indexer::events",
        event = "protocol_event",
        schema_version = 1_u16,
        operation = "observed",
        event_id,
        event_name,
        arguments,
        decode_error,
        chain_id = log.cursor.chain_id,
        core = %log.address,
        block_number = log.cursor.block_number,
        execution_block_number = log.cursor.execution_block_number,
        block_hash,
        transaction_hash,
        transaction_index = ?log.cursor.transaction_index,
        log_index = ?log.cursor.log_index,
        commitment = ?log.cursor.commitment,
        removed = log.removed,
        topic0,
        topics,
        data,
        "Core protocol event"
    );
}

fn core_event_id(log: &ContractLog) -> String {
    let block_hash = option_hash(log.cursor.block_hash);
    let transaction_hash = option_hash(log.transaction_hash);
    let transaction_index = option_number(log.cursor.transaction_index);
    let log_index = option_number(log.cursor.log_index);
    let mut id = format!(
        "v1:{}:{}:bh={block_hash}:txh={transaction_hash}:txi={transaction_index}:li={log_index}:core={}",
        log.cursor.chain_id, log.cursor.block_number, log.address
    );
    let has_stable_position = log.cursor.log_index.is_some()
        && (log.transaction_hash.is_some() || log.cursor.transaction_index.is_some());
    if !has_stable_position {
        let source_sequence = option_number(log.cursor.source_sequence);
        let source_sub_index = option_number(log.cursor.source_sub_index);
        let payload_digest = core_event_payload_digest(log);
        id.push_str(&format!(
            ":seq={source_sequence}:sub={source_sub_index}:payload={payload_digest:#x}"
        ));
    }
    id
}

fn option_hash(value: Option<lunarbase_math::B256>) -> String {
    value.map_or_else(|| "none".into(), |value| format!("some:{value:#x}"))
}

fn option_number<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "none".into(), |value| format!("some:{value}"))
}

fn core_event_payload_digest(log: &ContractLog) -> lunarbase_math::B256 {
    let mut payload = Vec::with_capacity(16 + log.topics.len() * 32 + log.data.len());
    payload.extend_from_slice(&(log.topics.len() as u64).to_be_bytes());
    for topic in &log.topics {
        payload.extend_from_slice(topic.as_slice());
    }
    payload.extend_from_slice(&(log.data.len() as u64).to_be_bytes());
    payload.extend_from_slice(&log.data);
    keccak256(payload)
}

struct EventDedup {
    capacity: usize,
    order: VecDeque<String>,
    seen: HashSet<String>,
}

impl EventDedup {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::with_capacity(capacity),
            seen: HashSet::with_capacity(capacity),
        }
    }

    fn insert(&mut self, event_id: String) -> bool {
        if self.seen.contains(&event_id) {
            return false;
        }
        self.seen.insert(event_id.clone());
        self.order.push_back(event_id);
        if self.order.len() > self.capacity
            && let Some(expired) = self.order.pop_front()
        {
            self.seen.remove(&expired);
        }
        true
    }
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

async fn join_unit_task(mut task: JoinHandle<()>, deadline: Instant) -> Result<(), String> {
    match timeout(remaining(deadline), &mut task).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(format!("failed during shutdown: {error}")),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("exceeded shutdown deadline".into())
        }
    }
}

async fn join_api_task(
    mut task: JoinHandle<Result<(), std::io::Error>>,
    deadline: Instant,
) -> Result<(), String> {
    match timeout(remaining(deadline), &mut task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(format!("failed during shutdown: {error}")),
        Ok(Err(error)) => Err(format!("failed during shutdown: {error}")),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("exceeded shutdown deadline".into())
        }
    }
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(unix)]
async fn wait_for_signal() -> Result<(), std::io::Error> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("lunarbase_indexer=info"))
        .add_directive(
            "lunarbase_indexer::events=info"
                .parse()
                .expect("static event log directive is valid"),
        );
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}

#[cfg(test)]
mod tests {
    use super::{EventDedup, core_event_id, join_api_task, join_unit_task};
    use lunarbase_client::model::{ChainCursor, Commitment, ContractLog};
    use lunarbase_math::{Address, B256, Bytes};
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn shutdown_join_reports_task_panics() {
        let task = tokio::spawn(async {
            panic!("intentional task panic");
        });
        let error = join_unit_task(task, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();

        assert!(error.contains("failed during shutdown"));
    }

    #[tokio::test]
    async fn shutdown_join_reports_api_errors() {
        let task = tokio::spawn(async { Err(std::io::Error::other("intentional API failure")) });
        let error = join_api_task(task, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();

        assert!(error.contains("intentional API failure"));
    }

    #[test]
    fn fallback_event_id_includes_block_number() {
        let first = log(41, None);
        let second = log(42, None);

        assert_ne!(core_event_id(&first), core_event_id(&second));
    }

    #[test]
    fn event_id_distinguishes_absent_positions_from_zero() {
        let mut absent = log(41, None);
        absent.cursor.transaction_index = None;
        absent.cursor.log_index = None;
        let mut zero = absent.clone();
        zero.cursor.transaction_index = Some(0);
        zero.cursor.log_index = Some(0);

        assert_ne!(core_event_id(&absent), core_event_id(&zero));
    }

    #[test]
    fn incomplete_event_id_uses_source_position_and_payload_digest() {
        let mut first = log(41, None);
        first.cursor.transaction_index = None;
        first.cursor.log_index = None;
        first.cursor.source_sequence = Some(7);
        first.cursor.source_sub_index = Some(1);
        first.topics = vec![B256::new([2; 32])];
        first.data = Bytes::from_static(&[3]);

        let mut different_sequence = first.clone();
        different_sequence.cursor.source_sequence = Some(8);
        let mut different_payload = first.clone();
        different_payload.data = Bytes::from_static(&[4]);

        assert_ne!(core_event_id(&first), core_event_id(&different_sequence));
        assert_ne!(core_event_id(&first), core_event_id(&different_payload));
    }

    #[test]
    fn bounded_dedup_suppresses_live_replays_and_evicts_old_ids() {
        let mut dedup = EventDedup::new(2);

        assert!(dedup.insert("a".into()));
        assert!(!dedup.insert("a".into()));
        assert!(dedup.insert("b".into()));
        assert!(dedup.insert("c".into()));
        assert!(dedup.insert("a".into()), "oldest key was evicted");
    }

    fn log(block_number: u64, transaction_hash: Option<B256>) -> ContractLog {
        ContractLog {
            address: Address::new([1; 20]),
            transaction_hash,
            topics: Vec::new(),
            data: Bytes::new(),
            removed: false,
            cursor: ChainCursor {
                chain_id: 97,
                block_number,
                execution_block_number: block_number,
                block_hash: None,
                transaction_index: Some(2),
                log_index: Some(3),
                source_sequence: None,
                source_sub_index: None,
                commitment: Commitment::Realtime,
            },
        }
    }
}
