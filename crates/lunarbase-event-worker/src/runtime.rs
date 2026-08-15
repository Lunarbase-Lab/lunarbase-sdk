//! Canonical recovery followed by at-least-once live event persistence.

use crate::{
    config::Config,
    event::DurableEvent,
    metrics::Metrics,
    pump::{self, PumpRuntime, QueuedUpdate},
    redis_store::{RedisEventStore, StoreError},
};
use lunarbase_client::{
    model::{
        BackfillRequest, ChainCursor, ChainUpdate, ContractFilter, ContractLog, Network,
        SourceError,
    },
    source::ChainDataSource,
};
use std::{sync::Arc, time::Instant};
use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    time::sleep,
};

#[derive(Debug, Error)]
pub(crate) enum RuntimeError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("source network mismatch: configured {configured:?}, source {actual:?}")]
    NetworkMismatch {
        configured: Network,
        actual: Network,
    },
    #[error("source update belongs to another Core deployment")]
    LogIdentity,
    #[error("canonical recovery returned an invalid log: {0}")]
    RecoveryLog(String),
    #[error("source pump stopped unexpectedly")]
    PumpStopped,
    #[cfg(not(all(
        feature = "evm",
        feature = "base",
        feature = "monad",
        feature = "arbitrum"
    )))]
    #[error("network support is not compiled: {0:?}")]
    UnsupportedNetwork(Network),
}

impl RuntimeError {
    fn retryable_recovery(&self) -> bool {
        matches!(self, Self::Source(_) | Self::RecoveryLog(_))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Transition {
    Continue,
    Recover,
    Shutdown,
}

pub(crate) async fn run<S>(
    source: Arc<S>,
    config: Arc<Config>,
    store: RedisEventStore,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError>
where
    S: ChainDataSource + 'static,
{
    if source.network() != config.network {
        return Err(RuntimeError::NetworkMismatch {
            configured: config.network,
            actual: source.network(),
        });
    }
    initialize_store(&store, &config, &metrics, &mut shutdown).await?;
    if *shutdown.borrow() {
        return Ok(());
    }

    let filter = ContractFilter {
        address: config.core,
        topics: Vec::new(),
    };
    let (sender, mut receiver) = mpsc::channel(config.source_queue_bound);
    let source_byte_budget = Arc::new(tokio::sync::Semaphore::new(config.source_queue_byte_bound));
    let (active_sender, mut active) = watch::channel(false);
    let pump = pump::spawn(
        source.clone(),
        filter.clone(),
        sender,
        PumpRuntime {
            reconnect_delay: config.reconnect_delay,
            stall_timeout: config.source_stall_timeout,
            active: active_sender,
            shutdown: shutdown.clone(),
            metrics: metrics.clone(),
            byte_budget: source_byte_budget,
            byte_capacity: config.source_queue_byte_bound,
        },
    );

    let result = drive(
        source,
        &config,
        &filter,
        &store,
        &metrics,
        &mut receiver,
        &mut active,
        &mut shutdown,
    )
    .await;
    metrics.set_ready(false);
    if !pump.is_finished() {
        pump.abort();
    }
    let _ = pump.await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn drive<S: ChainDataSource>(
    source: Arc<S>,
    config: &Config,
    filter: &ContractFilter,
    store: &RedisEventStore,
    metrics: &Metrics,
    receiver: &mut mpsc::Receiver<QueuedUpdate>,
    active: &mut watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    loop {
        metrics.set_ready(false);
        if !wait_until_active(active, shutdown).await? {
            return Ok(());
        }
        match recover(
            source.as_ref(),
            config,
            filter,
            store,
            metrics,
            receiver,
            active,
            shutdown,
        )
        .await
        {
            Ok(Transition::Shutdown) => return Ok(()),
            Ok(Transition::Recover) => continue,
            Ok(Transition::Continue) => {}
            Err(error) => {
                metrics.recovery_failure();
                if !error.retryable_recovery() {
                    return Err(error);
                }
                tracing::warn!(error = %error, "event worker recovery failed");
                if !sleep_or_shutdown(config.reconnect_delay, shutdown).await {
                    return Ok(());
                }
                continue;
            }
        }
        if *active.borrow() && metrics.queues_empty() {
            metrics.set_ready(true);
        }
        match consume_live(config, store, metrics, receiver, active, shutdown).await? {
            Transition::Shutdown => return Ok(()),
            Transition::Recover => continue,
            Transition::Continue => unreachable!("live consumption has no finite success"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn recover<S: ChainDataSource>(
    source: &S,
    config: &Config,
    filter: &ContractFilter,
    store: &RedisEventStore,
    metrics: &Metrics,
    receiver: &mut mpsc::Receiver<QueuedUpdate>,
    active: &watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Transition, RuntimeError> {
    metrics.recovery();
    let cursor = load_cursor_with_retry(store, config, metrics, shutdown).await?;
    if *shutdown.borrow() {
        return Ok(Transition::Shutdown);
    }
    let head = source.canonical_head().await?;
    validate_cursor(&head, config.chain_id)?;
    metrics.observe_head(head.block_number);
    let from_block = cursor
        .as_ref()
        .map_or(config.deployment_block, |cursor| cursor.block_number);
    if from_block > head.block_number {
        return Err(RuntimeError::RecoveryLog(format!(
            "durable cursor block {from_block} is ahead of canonical head {}",
            head.block_number
        )));
    }
    let mut page_start = from_block;
    loop {
        let page_end = page_start
            .saturating_add(config.backfill_page_blocks.saturating_sub(1))
            .min(head.block_number);
        let mut logs = source
            .backfill(BackfillRequest {
                from_block: page_start,
                to_block: page_end,
                filter: filter.clone(),
            })
            .await?;
        logs.sort_by_key(|log| log.cursor.event_order());
        for log in logs {
            validate_recovery_log(&log, config, page_start, page_end)?;
            if persist_log(log, config, store, metrics, shutdown).await? == Transition::Shutdown {
                return Ok(Transition::Shutdown);
            }
        }
        if page_end == head.block_number {
            break;
        }
        page_start = page_end.saturating_add(1);
    }

    while let Ok(queued) = receiver.try_recv() {
        match handle_update(queued.into_inner(), config, store, metrics, shutdown).await? {
            Transition::Continue => {}
            transition => return Ok(transition),
        }
    }
    if !*active.borrow() {
        return Ok(Transition::Recover);
    }
    Ok(Transition::Continue)
}

async fn consume_live(
    config: &Config,
    store: &RedisEventStore,
    metrics: &Metrics,
    receiver: &mut mpsc::Receiver<QueuedUpdate>,
    active: &mut watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Transition, RuntimeError> {
    loop {
        tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return Ok(Transition::Shutdown),
            update = receiver.recv() => {
                let update = update.ok_or(RuntimeError::PumpStopped)?.into_inner();
                let transition = handle_update(update, config, store, metrics, shutdown).await?;
                if transition != Transition::Continue {
                    return Ok(transition);
                }
                if *active.borrow() && metrics.queues_empty() {
                    metrics.set_ready(true);
                }
            }
        }
    }
}

async fn handle_update(
    update: ChainUpdate,
    config: &Config,
    store: &RedisEventStore,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Transition, RuntimeError> {
    match update {
        ChainUpdate::Head(cursor) => {
            validate_cursor(&cursor, config.chain_id)?;
            metrics.observe_head(cursor.block_number);
            Ok(Transition::Continue)
        }
        ChainUpdate::Log(log) => persist_log(log, config, store, metrics, shutdown).await,
        ChainUpdate::Gap { cursor, reason } => {
            if let Some(cursor) = cursor {
                validate_cursor(&cursor, config.chain_id)?;
            }
            metrics.source_gap();
            tracing::warn!(reason, "event source requested canonical recovery");
            Ok(Transition::Recover)
        }
        ChainUpdate::Reorg { old_head, new_head } => {
            validate_cursor(&old_head, config.chain_id)?;
            validate_cursor(&new_head, config.chain_id)?;
            metrics.source_gap();
            tracing::warn!(
                old = old_head.block_number,
                new = new_head.block_number,
                "event source reorg"
            );
            Ok(Transition::Recover)
        }
    }
}

async fn persist_log(
    log: ContractLog,
    config: &Config,
    store: &RedisEventStore,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Transition, RuntimeError> {
    validate_log_identity(&log, config)?;
    metrics.observe_head(log.cursor.block_number);
    if log.cursor.commitment < config.minimum_commitment {
        return Ok(Transition::Continue);
    }
    let event = Arc::new(DurableEvent::from_log(&log).map_err(StoreError::from)?);
    loop {
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return Ok(Transition::Shutdown),
            result = store.append(event.clone()) => result,
        };
        match result {
            Ok(outcome) => {
                metrics.persisted(
                    log.cursor.block_number,
                    !outcome.appended,
                    started.elapsed(),
                );
                return Ok(Transition::Continue);
            }
            Err(error) if error.retryable() => {
                metrics.redis_failure();
                tracing::warn!(error = %error, event_id = %event.event_id, "Redis event append will retry");
                if !sleep_or_shutdown(config.reconnect_delay, shutdown).await {
                    return Ok(Transition::Shutdown);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn initialize_store(
    store: &RedisEventStore,
    config: &Config,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    loop {
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return Ok(()),
            result = store.initialize() => result,
        };
        match result {
            Ok(()) => return Ok(()),
            Err(error) if error.retryable() => {
                metrics.redis_failure();
                tracing::warn!(error = %error, "Redis initialization will retry");
                if !sleep_or_shutdown(config.reconnect_delay, shutdown).await {
                    return Ok(());
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn load_cursor_with_retry(
    store: &RedisEventStore,
    config: &Config,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<ChainCursor>, RuntimeError> {
    loop {
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return Ok(None),
            result = store.load_cursor(config.chain_id, config.core) => result,
        };
        match result {
            Ok(cursor) => return Ok(cursor),
            Err(error) if error.retryable() => {
                metrics.redis_failure();
                if !sleep_or_shutdown(config.reconnect_delay, shutdown).await {
                    return Ok(None);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn validate_cursor(cursor: &ChainCursor, chain_id: u64) -> Result<(), RuntimeError> {
    if cursor.chain_id != chain_id {
        return Err(RuntimeError::LogIdentity);
    }
    Ok(())
}

fn validate_log_identity(log: &ContractLog, config: &Config) -> Result<(), RuntimeError> {
    validate_cursor(&log.cursor, config.chain_id)?;
    if log.address != config.core {
        return Err(RuntimeError::LogIdentity);
    }
    Ok(())
}

fn validate_recovery_log(
    log: &ContractLog,
    config: &Config,
    from_block: u64,
    to_block: u64,
) -> Result<(), RuntimeError> {
    validate_log_identity(log, config)?;
    if log.removed || log.cursor.block_number < from_block || log.cursor.block_number > to_block {
        return Err(RuntimeError::RecoveryLog(format!(
            "block {} outside {from_block}..={to_block} or marked removed",
            log.cursor.block_number
        )));
    }
    Ok(())
}

async fn wait_until_active(
    active: &mut watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool, RuntimeError> {
    while !*active.borrow() {
        tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return Ok(false),
            changed = active.changed() => {
                if changed.is_err() {
                    return Err(RuntimeError::PumpStopped);
                }
            }
        }
    }
    Ok(true)
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
}

async fn sleep_or_shutdown(
    delay: std::time::Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        biased;
        () = wait_for_shutdown(shutdown) => false,
        () = sleep(delay) => true,
    }
}
