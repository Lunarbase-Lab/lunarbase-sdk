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
    let event = match DurableEvent::from_log(&log) {
        Ok(event) => Arc::new(event),
        Err(error) => return Ok(log_recovery(metrics, &log, error)),
    };
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
            Err(error) if recoverable_log_error(&error) => {
                return Ok(log_recovery(metrics, &log, error));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn recoverable_log_error(error: &StoreError) -> bool {
    match error {
        StoreError::QueueByteLimit | StoreError::Event(_) => true,
        StoreError::Journal(detail) => [
            "LUNARBASE_LOG_ALREADY_ACTIVE",
            "LUNARBASE_LOG_ON_NONCANONICAL_BLOCK",
            "LUNARBASE_REORG_IN_PROGRESS",
            "LUNARBASE_FORK_REQUIRES_CORRECTION",
        ]
        .iter()
        .any(|marker| detail.contains(marker)),
        StoreError::Redis(_)
        | StoreError::Durability(_)
        | StoreError::CorrectionBudget(_)
        | StoreError::Json(_)
        | StoreError::ChannelClosed
        | StoreError::WorkerPanicked => false,
    }
}

fn log_recovery(
    metrics: &Metrics,
    log: &ContractLog,
    reason: impl std::fmt::Display,
) -> Transition {
    metrics.source_gap();
    tracing::warn!(
        error = %reason,
        block = log.cursor.block_number,
        "durable log continuity requires canonical recovery"
    );
    let target = log
        .cursor
        .block_hash
        .filter(|hash| !hash.is_zero())
        .map(|_| {
            let mut cursor = log.cursor.clone();
            cursor.transaction_index = None;
            cursor.log_index = None;
            cursor.source_sub_index = None;
            BlockRef::new(cursor, None)
        });
    Transition::Recover(target)
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
    let durable = match DurableHead::from_block(&head, config.core) {
        Ok(durable) => Arc::new(durable),
        Err(error) => return Err(head_recovery(metrics, &head, error)),
    };
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
            Err(error) if recoverable_head_error(&error) => {
                return Err(head_recovery(metrics, &head, error));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn recoverable_head_error(error: &StoreError) -> bool {
    match error {
        StoreError::QueueByteLimit | StoreError::Event(_) => true,
        StoreError::Journal(detail) => [
            "LUNARBASE_REORG_IN_PROGRESS",
            "LUNARBASE_HEADER_IDENTITY_MISMATCH",
            "LUNARBASE_FORK_REQUIRES_CORRECTION",
            "LUNARBASE_PARENT_LINK_MISMATCH",
            "LUNARBASE_CANONICAL_HEAD_MISSING",
            "LUNARBASE_HEAD_DISCONTINUITY",
            "LUNARBASE_FINALIZED_HEAD_MISSING",
        ]
        .iter()
        .any(|marker| detail.contains(marker)),
        StoreError::Redis(_)
        | StoreError::Durability(_)
        | StoreError::CorrectionBudget(_)
        | StoreError::Json(_)
        | StoreError::ChannelClosed
        | StoreError::WorkerPanicked => false,
    }
}

fn head_recovery(
    metrics: &Metrics,
    head: &BlockRef,
    reason: impl std::fmt::Display,
) -> RuntimeError {
    metrics.source_gap();
    tracing::warn!(
        error = %reason,
        block = head.cursor.block_number,
        "durable head continuity requires canonical recovery"
    );
    RuntimeError::RecoveryLog(format!("durable head continuity: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::{log_recovery, recoverable_head_error, recoverable_log_error};
    use crate::{metrics::Metrics, redis_store::StoreError, runtime::Transition};
    use alloy_primitives::{Address, B256, Bytes};
    use lunarbase_client::model::{ChainCursor, Commitment, ContractLog};

    #[test]
    fn altered_active_log_requests_recovery_without_stopping_the_worker() {
        let error = StoreError::Journal("LUNARBASE_LOG_ALREADY_ACTIVE".into());
        assert!(recoverable_log_error(&error));
        let metrics = Metrics::new(8, 1 << 20, 8, 1 << 20);
        metrics.set_ready(true);
        let log = ContractLog {
            address: Address::new([3; 20]),
            transaction_hash: Some(B256::new([7; 32])),
            topics: vec![B256::new([8; 32])],
            data: Bytes::from_static(&[9; 64]),
            removed: false,
            cursor: ChainCursor {
                transaction_index: Some(2),
                log_index: Some(3),
                source_sub_index: Some(4),
                ..ChainCursor::block(8453, 41, Some(B256::new([6; 32])), Commitment::Canonical)
            },
        };

        let Transition::Recover(Some(target)) = log_recovery(&metrics, &log, error) else {
            panic!("continuity conflict must recover the affected block");
        };
        assert_eq!(target.cursor.block_number, 41);
        assert!(target.cursor.transaction_index.is_none());
        assert!(target.cursor.log_index.is_none());
        assert!(target.cursor.source_sub_index.is_none());
        assert!(!metrics.is_ready());
        assert!(
            metrics
                .render()
                .contains("lunarbase_event_worker_source_gaps_total 1\n")
        );
    }

    #[test]
    fn head_continuity_is_recoverable_but_metadata_mismatch_is_fatal() {
        assert!(recoverable_head_error(&StoreError::Journal(
            "LUNARBASE_HEAD_DISCONTINUITY".into()
        )));
        assert!(!recoverable_head_error(&StoreError::Journal(
            "LUNARBASE_METADATA_MISMATCH".into()
        )));
    }
}
