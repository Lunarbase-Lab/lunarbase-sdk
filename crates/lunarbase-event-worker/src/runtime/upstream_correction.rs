//! Direct bridge from resolved source corrections to durable fork lifecycle.

use super::{RuntimeError, Transition, forks::ForkRuntime};
use crate::{config::Config, metrics::Metrics, redis_store::RedisEventStore};
use alloy_primitives::Address;
use lunarbase_client::model::ChainCorrection;
use tokio::sync::watch;

pub(super) async fn apply(
    correction: Box<ChainCorrection>,
    forks: Option<&mut ForkRuntime>,
    config: &Config,
    store: &RedisEventStore,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Transition, RuntimeError> {
    validate_deployment_identity(&correction, config.chain_id, config.core)?;
    if config.minimum_commitment == lunarbase_client::model::Commitment::Finalized {
        return finalized_policy(&correction, metrics);
    }
    let Some(forks) = forks else {
        return recover_without_fork_runtime(&correction, config.chain_id, config.core, metrics);
    };
    forks
        .apply_upstream_correction(*correction, config, store, metrics, shutdown)
        .await
}

fn recover_without_fork_runtime(
    correction: &ChainCorrection,
    chain_id: u64,
    core: Address,
    metrics: &Metrics,
) -> Result<Transition, RuntimeError> {
    validate_deployment_identity(correction, chain_id, core)?;
    let validation = correction.validate();
    let target = validation.is_ok().then(|| correction.new_tip.clone());
    metrics.source_gap();
    tracing::warn!(
        block = correction.new_tip.cursor.block_number,
        error = ?validation.err(),
        "resolved correction requires an unavailable durable fork runtime"
    );
    Ok(Transition::Recover(target))
}

fn validate_deployment_identity(
    correction: &ChainCorrection,
    chain_id: u64,
    core: Address,
) -> Result<(), RuntimeError> {
    let blocks = std::iter::once(&correction.common_ancestor)
        .chain(std::iter::once(&correction.old_tip))
        .chain(std::iter::once(&correction.new_tip))
        .chain(correction.old_branch.iter())
        .chain(correction.new_branch.iter());
    if blocks
        .into_iter()
        .any(|block| block.cursor.chain_id != chain_id)
        || correction
            .replacement_logs
            .iter()
            .any(|log| log.cursor.chain_id != chain_id || log.address != core)
    {
        return Err(RuntimeError::LogIdentity);
    }
    Ok(())
}

fn finalized_policy(
    correction: &ChainCorrection,
    metrics: &Metrics,
) -> Result<Transition, RuntimeError> {
    if let Err(error) = correction.validate() {
        metrics.source_gap();
        tracing::warn!(error = %error, "malformed correction ignored by finalized delivery");
        return Ok(Transition::Recover(None));
    }
    if correction.new_tip.cursor.commitment < lunarbase_client::model::Commitment::Finalized {
        return Ok(Transition::Continue);
    }
    Err(RuntimeError::FinalizedConflict)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use lunarbase_client::model::{BlockRef, ChainCursor, Commitment};

    #[test]
    fn missing_fork_runtime_recovers_without_stopping_process() {
        let core = Address::new([0x13; 20]);
        let ancestor = block(40, 1, 0);
        let old_tip = block(41, 2, 1);
        let new_tip = block(41, 3, 1);
        let correction = ChainCorrection {
            common_ancestor: ancestor,
            old_tip: old_tip.clone(),
            new_tip: new_tip.clone(),
            old_branch: vec![old_tip],
            new_branch: vec![new_tip.clone()],
            replacement_logs: Vec::new(),
        };
        let metrics = Metrics::new(8, 1 << 20, 8, 1 << 20);
        metrics.set_ready(true);

        assert_eq!(
            recover_without_fork_runtime(&correction, 8453, core, &metrics).unwrap(),
            Transition::Recover(Some(new_tip))
        );
        assert!(!metrics.is_ready());
        assert!(
            metrics
                .render()
                .contains("lunarbase_event_worker_source_gaps_total 1\n")
        );
    }

    #[test]
    fn malformed_correction_recovers_without_trusting_its_target() {
        let core = Address::new([0x13; 20]);
        let ancestor = block(40, 1, 0);
        let old_tip = block(41, 2, 1);
        let mut new_tip = block(41, 3, 1);
        new_tip.cursor.block_hash = None;
        let correction = ChainCorrection {
            common_ancestor: ancestor,
            old_tip: old_tip.clone(),
            new_tip: new_tip.clone(),
            old_branch: vec![old_tip],
            new_branch: vec![new_tip],
            replacement_logs: Vec::new(),
        };
        let metrics = Metrics::new(8, 1 << 20, 8, 1 << 20);
        metrics.set_ready(true);

        assert_eq!(
            recover_without_fork_runtime(&correction, 8453, core, &metrics).unwrap(),
            Transition::Recover(None)
        );
        assert!(!metrics.is_ready());
    }

    #[test]
    fn finalized_delivery_ignores_provisional_corrections_without_recovery_loop() {
        let ancestor = block(40, 1, 0);
        let old_tip = block(41, 2, 1);
        let new_tip = block(41, 3, 1);
        let mut correction = ChainCorrection {
            common_ancestor: ancestor,
            old_tip: old_tip.clone(),
            new_tip: new_tip.clone(),
            old_branch: vec![old_tip],
            new_branch: vec![new_tip],
            replacement_logs: Vec::new(),
        };
        let metrics = Metrics::new(8, 1 << 20, 8, 1 << 20);
        metrics.set_ready(true);

        assert_eq!(
            finalized_policy(&correction, &metrics).unwrap(),
            Transition::Continue
        );
        assert!(metrics.is_ready());

        correction.new_tip.cursor.commitment = Commitment::Finalized;
        correction.new_branch[0].cursor.commitment = Commitment::Finalized;
        assert!(matches!(
            finalized_policy(&correction, &metrics),
            Err(RuntimeError::FinalizedConflict)
        ));
    }

    fn block(number: u64, hash: u8, parent: u8) -> BlockRef {
        BlockRef::new(
            ChainCursor::block(
                8453,
                number,
                Some(B256::new([hash; 32])),
                Commitment::Canonical,
            ),
            Some(B256::new([parent; 32])),
        )
    }
}
