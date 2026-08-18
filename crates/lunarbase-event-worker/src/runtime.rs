//! Canonical recovery followed by at-least-once live event persistence.

#[path = "runtime/control.rs"]
mod control;
#[path = "runtime/forks.rs"]
mod forks;
#[path = "runtime/persist.rs"]
mod persist;
#[path = "runtime/recovery_coverage.rs"]
mod recovery_coverage;
#[path = "runtime/recovery_state.rs"]
mod recovery_state;
#[path = "runtime/upstream_correction.rs"]
mod upstream;
#[path = "runtime/validation.rs"]
mod validation;
use control::wait_until_active;
pub(super) use control::{sleep_or_shutdown, wait_for_shutdown};
use forks::ForkRuntime;
use recovery_coverage::recovery_log_is_covered;
pub(super) use recovery_state::Transition;
use recovery_state::{RecoveryAction, RecoveryState};
use validation::{validate_cursor, validate_log_identity, validate_recovery_log};

use crate::{
    config::Config,
    metrics::Metrics,
    pump::{self, PumpRuntime, QueuedUpdate},
    redis_store::{RedisEventStore, StoreError},
};
use lunarbase_client::{
    model::{
        BackfillRequest, BlockRef, ChainCursor, ChainUpdate, ContractFilter, Network, SourceError,
    },
    source::ChainDataSource,
};
use lunarbase_source_evm::fork::{ForkError, ForkResolver};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Error)]
pub(crate) enum RuntimeError {
    #[error(transparent)]
    Fork(#[from] ForkError),
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
    #[error("finalized source identity was retracted")]
    FinalizedConflict,
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
        matches!(self, Self::Source(_) | Self::RecoveryLog(_) | Self::Fork(_))
            || matches!(self, Self::Store(error) if error.retryable())
    }
}

pub(crate) async fn run<S>(
    source: Arc<S>,
    fork_resolver: Option<ForkResolver>,
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

    let mut forks = fork_resolver
        .map(|resolver| ForkRuntime::new(resolver, &config))
        .transpose()?;
    let result = drive(
        source,
        forks.as_mut(),
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
    mut forks: Option<&mut ForkRuntime>,
    config: &Config,
    filter: &ContractFilter,
    store: &RedisEventStore,
    metrics: &Metrics,
    receiver: &mut mpsc::Receiver<QueuedUpdate>,
    active: &mut watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    let mut recovery = RecoveryState::default();
    loop {
        metrics.set_ready(false);
        if !wait_until_active(active, shutdown).await? {
            return Ok(());
        }
        if let Some(detail) = recovery.conflict() {
            metrics.recovery_failure();
            tracing::warn!(
                detail,
                "event worker retained a conflicting recovery watermark"
            );
            if !sleep_or_shutdown(config.reconnect_delay, shutdown).await {
                return Ok(());
            }
            continue;
        }
        let Some(source_lease) = metrics.source_lease().filter(|_| *active.borrow()) else {
            continue;
        };
        let attempted_target = recovery.target().cloned();
        match recover(
            source.as_ref(),
            forks.as_deref_mut(),
            recovery.take_target(),
            recovery.required(),
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
            Ok(transition) => match recovery.apply(transition, forks.is_some()) {
                RecoveryAction::Shutdown => return Ok(()),
                RecoveryAction::Retry => continue,
                RecoveryAction::Live => {}
            },
            Err(error) => {
                recovery.restore_target(attempted_target);
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
        if *active.borrow() && metrics.queues_empty() && !metrics.publish_ready_if(source_lease) {
            continue;
        }
        match consume_live(
            forks.as_deref_mut(),
            config,
            store,
            metrics,
            receiver,
            active,
            source_lease,
            shutdown,
        )
        .await
        {
            Ok(transition) => match recovery.apply(transition, forks.is_some()) {
                RecoveryAction::Shutdown => return Ok(()),
                RecoveryAction::Retry => continue,
                RecoveryAction::Live => unreachable!("live consumption has no finite success"),
            },
            Err(error) => {
                metrics.recovery_failure();
                if !error.retryable_recovery() {
                    return Err(error);
                }
                tracing::warn!(error = %error, "event worker paused for recovery");
                if !sleep_or_shutdown(config.reconnect_delay, shutdown).await {
                    return Ok(());
                }
                recovery.clear_target();
                continue;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn recover<S: ChainDataSource>(
    source: &S,
    mut forks: Option<&mut ForkRuntime>,
    target: Option<BlockRef>,
    required: Option<&ChainCursor>,
    config: &Config,
    filter: &ContractFilter,
    store: &RedisEventStore,
    metrics: &Metrics,
    receiver: &mut mpsc::Receiver<QueuedUpdate>,
    active: &watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Transition, RuntimeError> {
    metrics.recovery();
    let target = forks.is_some().then_some(target).flatten();
    if let Some(forks) = forks.as_deref_mut()
        && forks
            .reconcile(source, target, config, filter, store, metrics, shutdown)
            .await?
            == Transition::Shutdown
    {
        return Ok(Transition::Shutdown);
    }
    if *shutdown.borrow() {
        return Ok(Transition::Shutdown);
    }
    let cursor = load_cursor_with_retry(store, config, metrics, shutdown).await?;
    if *shutdown.borrow() {
        return Ok(Transition::Shutdown);
    }
    let head = source.canonical_head().await?;
    validate_cursor(&head, config.chain_id)?;
    if let Some(required) = required
        && !recovery_coverage::head_covers(required, &head)?
    {
        return Err(RuntimeError::RecoveryLog(format!(
            "canonical head {} does not cover required source cursor {}",
            head.block_number, required.block_number
        )));
    }
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
        validation::normalize_recovery_page(&mut logs, config, page_start, page_end)?;
        logs.sort_by_key(|log| log.cursor.event_order());
        for log in logs {
            if cursor.as_ref().is_some_and(|durable| {
                recovery_log_is_covered(forks.is_some(), &log.cursor, durable)
            }) {
                continue;
            }
            match persist::log(log, config, store, metrics, shutdown).await? {
                Transition::Continue => {}
                Transition::Shutdown => return Ok(Transition::Shutdown),
                Transition::Recover(target) => return Ok(Transition::Recover(target)),
                Transition::RecoverRequired(required) => {
                    return Ok(Transition::RecoverRequired(required));
                }
            }
        }
        if page_end == head.block_number {
            break;
        }
        page_start = page_end.saturating_add(1);
    }

    while let Ok(queued) = receiver.try_recv() {
        match handle_update(
            queued.into_inner(),
            forks.as_deref_mut(),
            config,
            store,
            metrics,
            shutdown,
        )
        .await?
        {
            Transition::Continue => {}
            transition => return Ok(transition),
        }
    }
    if !*active.borrow() {
        return Ok(Transition::Recover(None));
    }
    Ok(Transition::Continue)
}

async fn consume_live(
    mut forks: Option<&mut ForkRuntime>,
    config: &Config,
    store: &RedisEventStore,
    metrics: &Metrics,
    receiver: &mut mpsc::Receiver<QueuedUpdate>,
    active: &mut watch::Receiver<bool>,
    source_lease: u64,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Transition, RuntimeError> {
    loop {
        tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return Ok(Transition::Shutdown),
            update = receiver.recv() => {
                let update = update.ok_or(RuntimeError::PumpStopped)?.into_inner();
                let transition = handle_update(update, forks.as_deref_mut(), config, store, metrics, shutdown).await?;
                if transition != Transition::Continue {
                    return Ok(transition);
                }
                if *active.borrow()
                    && metrics.queues_empty()
                    && !metrics.publish_ready_if(source_lease)
                {
                    return Ok(Transition::Recover(None));
                }
            }
        }
    }
}

async fn handle_update(
    update: ChainUpdate,
    forks: Option<&mut ForkRuntime>,
    config: &Config,
    store: &RedisEventStore,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Transition, RuntimeError> {
    match update {
        ChainUpdate::Head(head) => match forks {
            Some(forks) => {
                forks
                    .observe_head(head, config, store, metrics, shutdown)
                    .await
            }
            None => persist::head(head, config, store, metrics, shutdown).await,
        },
        ChainUpdate::Log(log) => {
            if log.removed {
                validate_log_identity(&log, config)?;
                if config.minimum_commitment == lunarbase_client::model::Commitment::Finalized {
                    return if log.cursor.commitment < lunarbase_client::model::Commitment::Finalized
                    {
                        Ok(Transition::Continue)
                    } else {
                        Err(RuntimeError::FinalizedConflict)
                    };
                }
                metrics.source_gap();
                let target = forks
                    .is_some()
                    .then(|| BlockRef::new(log.cursor.clone(), None));
                tracing::warn!(
                    block = log.cursor.block_number,
                    "provider removal triggered exact fork recovery"
                );
                return Ok(Transition::Recover(target));
            }
            persist::log(log, config, store, metrics, shutdown).await
        }
        ChainUpdate::Gap { cursor, reason } => {
            if let Some(cursor) = cursor.as_ref() {
                validate_cursor(cursor, config.chain_id)?;
            }
            metrics.source_gap();
            tracing::warn!(reason, "event source requested canonical recovery");
            Ok(cursor.map_or(Transition::Recover(None), Transition::RecoverRequired))
        }
        ChainUpdate::Reorg { old_head, new_head } => {
            validate_cursor(&old_head.cursor, config.chain_id)?;
            validate_cursor(&new_head.cursor, config.chain_id)?;
            if config.minimum_commitment == lunarbase_client::model::Commitment::Finalized {
                return if old_head.cursor.commitment
                    == lunarbase_client::model::Commitment::Finalized
                    || new_head.cursor.commitment == lunarbase_client::model::Commitment::Finalized
                {
                    Err(RuntimeError::FinalizedConflict)
                } else {
                    Ok(Transition::Continue)
                };
            }
            metrics.source_gap();
            tracing::warn!(
                old = old_head.cursor.block_number,
                new = new_head.cursor.block_number,
                "durable fork correction scheduled"
            );
            Ok(Transition::Recover(Some(new_head)))
        }
        ChainUpdate::Correction(correction) => {
            upstream::apply(correction, forks, config, store, metrics, shutdown).await
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

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
