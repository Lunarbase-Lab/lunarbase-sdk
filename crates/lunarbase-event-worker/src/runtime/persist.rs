//! Durable persistence retries kept off the ingestion control-flow module.

use super::{
    RuntimeError, Transition, sleep_or_shutdown, validate_cursor, validate_log_identity,
    wait_for_shutdown,
};
use crate::{
    config::Config,
    event::{DurableEvent, DurableHead},
    metrics::Metrics,
    redis_store::{RedisEventStore, StoreError},
};
use lunarbase_client::model::{BlockRef, ContractLog};
use std::{sync::Arc, time::Instant};
use tokio::sync::watch;

pub(super) async fn log(
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
    let block_number = log.cursor.block_number;
    let event = Arc::new(DurableEvent::from_log(&log).map_err(StoreError::from)?);
    loop {
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return Ok(Transition::Shutdown),
            result = store.append_event(event.clone()) => result,
        };
        match result {
            Ok(outcome) => {
                metrics.persisted(block_number, !outcome.appended, started.elapsed());
                return Ok(Transition::Continue);
            }
            Err(error) if error.retryable() => {
                metrics.redis_failure();
                tracing::warn!(
                    error = %error,
                    record_id = %event.record_id,
                    "Redis event append will retry"
                );
                if !sleep_or_shutdown(config.reconnect_delay, shutdown).await {
                    return Ok(Transition::Shutdown);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) async fn head(
    head: BlockRef,
    config: &Config,
    store: &RedisEventStore,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Transition, RuntimeError> {
    validate_cursor(&head.cursor, config.chain_id)?;
    metrics.observe_head(head.cursor.block_number);
    if head.cursor.commitment < config.minimum_commitment {
        return Ok(Transition::Continue);
    }
    let block_number = head.cursor.block_number;
    let durable = Arc::new(DurableHead::from_block(&head, config.core).map_err(StoreError::from)?);
    loop {
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return Ok(Transition::Shutdown),
            result = store.append_head(durable.clone()) => result,
        };
        match result {
            Ok(outcome) => {
                metrics.header_journaled(block_number, !outcome.appended);
                return Ok(Transition::Continue);
            }
            Err(error) if error.retryable() => {
                metrics.redis_failure();
                tracing::warn!(
                    error = %error,
                    block_hash = %durable.block_hash,
                    "Redis block journal append will retry"
                );
                if !sleep_or_shutdown(config.reconnect_delay, shutdown).await {
                    return Ok(Transition::Shutdown);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}
