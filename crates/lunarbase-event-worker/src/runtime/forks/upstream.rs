//! Validation and atomic persistence of already-resolved source corrections.

use super::{ForkRuntime, same_block};
use crate::{
    config::Config,
    event::ReorgCorrection,
    metrics::Metrics,
    redis_store::{CorrectionLimits, RedisEventStore, StoreError},
    runtime::{
        RuntimeError, Transition, sleep_or_shutdown, validate_log_identity, wait_for_shutdown,
    },
};
use lunarbase_client::model::{BlockRef, ChainCorrection, Commitment};
use lunarbase_source_evm::fork::{CanonicalWindow, ForkResolution};
use std::sync::Arc;
use tokio::sync::watch;

impl ForkRuntime {
    pub(in crate::runtime) async fn apply_upstream_correction(
        &mut self,
        mut correction: ChainCorrection,
        config: &Config,
        store: &RedisEventStore,
        metrics: &Metrics,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Transition, RuntimeError> {
        validate_deployment_identity(&correction, config)?;
        let recovery_target = correction.new_tip.clone();
        if let Err(error) = validate_resolution_envelope(&correction, config) {
            return Ok(recovery_transition(metrics, &recovery_target, error));
        }
        if !self.loaded {
            return Ok(recovery_transition(
                metrics,
                &recovery_target,
                "durable fork history was not loaded before live correction",
            ));
        }
        let duplicate = match validate_durable_branch(&self.window, &correction) {
            Ok(duplicate) => duplicate,
            Err(error) => return Ok(recovery_transition(metrics, &recovery_target, error)),
        };
        let finalized = match validate_finality(self, &correction, duplicate) {
            Ok(finalized) => finalized,
            Err(error) => return Ok(recovery_transition(metrics, &recovery_target, error)),
        };
        if correction.old_branch.is_empty() && correction.new_branch.is_empty() {
            return Ok(Transition::Continue);
        }
        correction.normalize_for_retention();

        let durable_resolution = resolution_from(&correction);
        let next_window = if duplicate {
            None
        } else {
            match corrected_window(&self.window, &correction) {
                Ok(window) => Some(window),
                Err(error) => return Ok(recovery_transition(metrics, &recovery_target, error)),
            }
        };
        let durable = match ReorgCorrection::new(
            &durable_resolution,
            finalized,
            correction.replacement_logs,
            config.core,
        ) {
            Ok(durable) => Arc::new(durable),
            Err(error) => return Ok(recovery_transition(metrics, &recovery_target, error)),
        };
        if durable.retained_bytes() > config.correction_byte_bound {
            return Ok(recovery_transition(
                metrics,
                &recovery_target,
                "materialized correction exceeds its byte budget",
            ));
        }
        let reorg_id = durable.reorg_id.clone();
        let outcome = loop {
            let result = tokio::select! {
                biased;
                () = wait_for_shutdown(shutdown) => return Ok(Transition::Shutdown),
                result = store.correct(
                    durable.clone(),
                    CorrectionLimits {
                        max_events: config.correction_event_bound,
                        max_bytes: config.correction_byte_bound,
                    },
                ) => result,
            };
            match result {
                Ok(outcome) => break outcome,
                Err(error) if error.retryable() => {
                    metrics.redis_failure();
                    tracing::warn!(error = %error, reorg_id, "Redis correction will retry");
                    if !sleep_or_shutdown(config.reconnect_delay, shutdown).await {
                        return Ok(Transition::Shutdown);
                    }
                }
                Err(error) if recoverable_store_error(&error) => {
                    return Ok(recovery_transition(metrics, &recovery_target, error));
                }
                Err(error) => return Err(store_invariant(error)),
            }
        };
        debug_assert!(
            outcome.reverted.saturating_add(outcome.applied)
                <= config.correction_event_bound.saturating_sub(2),
            "Redis store validates correction event count before its atomic commit"
        );
        metrics.reorg_corrected(outcome.reverted, outcome.applied, !outcome.appended);
        metrics.observe_head(durable.new_tip.cursor.block_number);
        if let Some(window) = next_window {
            self.window = window;
        }
        tracing::info!(
            reorg_id,
            reverted = outcome.reverted,
            applied = outcome.applied,
            duplicate = !outcome.appended,
            stream_id = outcome.stream_id,
            "resolved source correction committed without recovery"
        );
        Ok(Transition::Continue)
    }
}

fn validate_deployment_identity(
    correction: &ChainCorrection,
    config: &Config,
) -> Result<(), RuntimeError> {
    let blocks = std::iter::once(&correction.common_ancestor)
        .chain(std::iter::once(&correction.old_tip))
        .chain(std::iter::once(&correction.new_tip))
        .chain(correction.old_branch.iter())
        .chain(correction.new_branch.iter());
    if blocks
        .into_iter()
        .any(|block| block.cursor.chain_id != config.chain_id)
        || correction
            .replacement_logs
            .iter()
            .any(|log| log.cursor.chain_id != config.chain_id || log.address != config.core)
    {
        return Err(RuntimeError::LogIdentity);
    }
    Ok(())
}

fn validate_resolution_envelope(
    correction: &ChainCorrection,
    config: &Config,
) -> Result<(), RuntimeError> {
    correction.validate().map_err(invariant)?;
    if config.minimum_commitment == Commitment::Finalized {
        return Err(invariant(
            "finalized delivery cannot accept an optimistic correction",
        ));
    }
    if correction.old_branch.len() > config.fork_max_depth
        || correction.new_branch.len() > config.fork_max_depth
        || correction.retained_bytes() > config.correction_byte_bound
        || correction.replacement_logs.len().saturating_add(2) > config.correction_event_bound
    {
        return Err(invariant("correction exceeds worker resource bounds"));
    }
    let blocks = std::iter::once(&correction.common_ancestor)
        .chain(std::iter::once(&correction.old_tip))
        .chain(std::iter::once(&correction.new_tip))
        .chain(correction.old_branch.iter())
        .chain(correction.new_branch.iter());
    if blocks.into_iter().any(|block| {
        block.cursor.commitment < config.minimum_commitment
            || block.cursor.transaction_index.is_some()
            || block.cursor.log_index.is_some()
    }) {
        return Err(invariant(
            "correction block identity or commitment violates worker policy",
        ));
    }
    for log in &correction.replacement_logs {
        validate_log_identity(log, config).map_err(invariant)?;
        if log.cursor.commitment < config.minimum_commitment {
            return Err(invariant(
                "replacement log commitment violates worker policy",
            ));
        }
    }
    Ok(())
}

fn validate_finality(
    runtime: &ForkRuntime,
    correction: &ChainCorrection,
    duplicate_candidate: bool,
) -> Result<BlockRef, RuntimeError> {
    let finalized = runtime
        .finalized
        .as_ref()
        .ok_or_else(|| invariant("durable finalized boundary is absent"))?;
    let window_finalized = runtime
        .window
        .finalized()
        .ok_or_else(|| invariant("fork window finalized boundary is absent"))?;
    let crosses_finalized = correction.common_ancestor.cursor.block_number
        < finalized.cursor.block_number
        || (correction.common_ancestor.cursor.block_number == finalized.cursor.block_number
            && !same_block(&correction.common_ancestor, finalized));
    if !same_block(finalized, window_finalized) || (!duplicate_candidate && crosses_finalized) {
        return Err(invariant("correction conflicts with finalized history"));
    }
    Ok(finalized.clone())
}

fn validate_durable_branch(
    window: &CanonicalWindow,
    correction: &ChainCorrection,
) -> Result<bool, RuntimeError> {
    let retained = window.blocks().collect::<Vec<_>>();
    let tip = window
        .tip()
        .ok_or_else(|| invariant("durable fork window is empty"))?;
    if same_block(tip, &correction.new_tip) {
        // Redis remains authoritative for exact semantic idempotency. An exact
        // retry can outlive its ancestor in the finalized local window, while
        // an altered envelope has a different reorg ID and fails store checks.
        return Ok(true);
    }
    let ancestor = retained
        .iter()
        .position(|block| same_block(block, &correction.common_ancestor))
        .ok_or_else(|| invariant("correction ancestor is outside durable history"))?;
    if !same_block(tip, &correction.old_tip) {
        return Err(invariant("correction old tip is not the durable tip"));
    }
    if retained.len().saturating_sub(ancestor + 1) != correction.old_branch.len()
        || retained
            .iter()
            .skip(ancestor + 1)
            .zip(&correction.old_branch)
            .any(|(stored, supplied)| !same_block(stored, supplied))
    {
        return Err(invariant(
            "correction branch disagrees with durable history",
        ));
    }
    Ok(false)
}

fn corrected_window(
    window: &CanonicalWindow,
    correction: &ChainCorrection,
) -> Result<CanonicalWindow, RuntimeError> {
    let retained = window.blocks().collect::<Vec<_>>();
    let ancestor = retained
        .iter()
        .position(|block| same_block(block, &correction.common_ancestor))
        .ok_or_else(|| invariant("correction ancestor is outside durable history"))?;
    let resolution = ForkResolution {
        common_ancestor: retained[ancestor].clone(),
        old_tip: window
            .tip()
            .cloned()
            .ok_or_else(|| invariant("empty window"))?,
        new_tip: correction.new_tip.clone(),
        old_branch: retained
            .iter()
            .skip(ancestor + 1)
            .map(|block| (*block).clone())
            .collect(),
        new_branch: correction.new_branch.clone(),
    };
    let mut candidate = window.clone();
    candidate.apply_resolution(&resolution).map_err(invariant)?;
    Ok(candidate)
}

fn resolution_from(correction: &ChainCorrection) -> ForkResolution {
    ForkResolution {
        common_ancestor: correction.common_ancestor.clone(),
        old_tip: correction.old_tip.clone(),
        new_tip: correction.new_tip.clone(),
        old_branch: correction.old_branch.clone(),
        new_branch: correction.new_branch.clone(),
    }
}

fn store_invariant(error: StoreError) -> RuntimeError {
    invariant(error)
}

pub(super) fn recoverable_store_error(error: &StoreError) -> bool {
    match error {
        StoreError::CorrectionBudget(_) | StoreError::QueueByteLimit | StoreError::Event(_) => true,
        StoreError::Journal(detail) => [
            "LUNARBASE_REORG_STALE_HEAD",
            "LUNARBASE_REORG_FINALIZED_MISMATCH",
            "LUNARBASE_REORG_OLD_BRANCH_MISMATCH",
            "LUNARBASE_LOG_ALREADY_ACTIVE",
        ]
        .iter()
        .any(|marker| detail.contains(marker)),
        StoreError::Redis(_)
        | StoreError::Durability(_)
        | StoreError::Json(_)
        | StoreError::ChannelClosed
        | StoreError::WorkerPanicked => false,
    }
}

fn recovery_transition(
    metrics: &Metrics,
    target: &BlockRef,
    reason: impl std::fmt::Display,
) -> Transition {
    metrics.source_gap();
    tracing::warn!(error = %reason, block = target.cursor.block_number, "resolved correction requires canonical recovery");
    Transition::Recover(Some(target.clone()))
}

fn invariant(error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Store(StoreError::Journal(format!(
        "upstream correction invariant: {error}"
    )))
}

#[cfg(test)]
#[path = "upstream_tests.rs"]
mod tests;
