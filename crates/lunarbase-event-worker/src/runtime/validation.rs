//! Deployment identity and bounded canonical-backfill validation.

use super::RuntimeError;
use crate::config::Config;
use lunarbase_client::model::{ChainCursor, ContractLog};

// These guards exceed the bundled RPC adapter's 16,384-log/32-MiB limits while
// protecting the generic ChainDataSource boundary used by tests and embedders.
const RECOVERY_PAGE_LOG_LIMIT: usize = 65_536;
const RECOVERY_PAGE_BYTE_LIMIT: usize = 64 * 1024 * 1024;

pub(super) fn validate_cursor(cursor: &ChainCursor, chain_id: u64) -> Result<(), RuntimeError> {
    if cursor.chain_id != chain_id {
        return Err(RuntimeError::LogIdentity);
    }
    Ok(())
}

pub(super) fn validate_log_identity(
    log: &ContractLog,
    config: &Config,
) -> Result<(), RuntimeError> {
    validate_cursor(&log.cursor, config.chain_id)?;
    if log.address != config.core {
        return Err(RuntimeError::LogIdentity);
    }
    Ok(())
}

pub(super) fn validate_recovery_log(
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

pub(super) fn normalize_recovery_page(
    logs: &mut [ContractLog],
    config: &Config,
    from_block: u64,
    to_block: u64,
) -> Result<(), RuntimeError> {
    normalize_recovery_page_with_limits(
        logs,
        config,
        from_block,
        to_block,
        RECOVERY_PAGE_LOG_LIMIT,
        RECOVERY_PAGE_BYTE_LIMIT,
    )
}

fn normalize_recovery_page_with_limits(
    logs: &mut [ContractLog],
    config: &Config,
    from_block: u64,
    to_block: u64,
    max_logs: usize,
    max_bytes: usize,
) -> Result<(), RuntimeError> {
    for log in logs.iter() {
        validate_recovery_log(log, config, from_block, to_block)?;
    }
    let retained_bytes = logs.iter().try_fold(0_usize, |bytes, log| {
        log.retained_bytes()
            .checked_add(bytes)
            .filter(|total| *total <= max_bytes)
            .ok_or_else(recovery_page_budget_error)
    })?;
    if logs.len() > max_logs || retained_bytes > max_bytes {
        return Err(recovery_page_budget_error());
    }
    for log in logs {
        log.normalize_for_retention();
    }
    Ok(())
}

fn recovery_page_budget_error() -> RuntimeError {
    RuntimeError::RecoveryLog("page exceeded its count or byte budget".into())
}

#[cfg(test)]
mod tests {
    use super::{RuntimeError, normalize_recovery_page_with_limits};
    use crate::config::Config;
    use alloy_primitives::{Address, B256, Bytes};
    use lunarbase_client::model::{ChainCursor, Commitment, ContractLog, Network};
    use std::{net::SocketAddr, time::Duration};

    const CORE: Address = Address::new([0x35; 20]);
    const BACKING_BYTES: usize = 1 << 20;

    #[test]
    fn count_overflow_rejects_before_compacting_tail_slices() {
        let backings = vec![large_backing(), large_backing()];
        let mut logs = vec![tail_log(0, &backings[0]), tail_log(1, &backings[1])];
        let pointers = logs.iter().map(|log| log.data.as_ptr()).collect::<Vec<_>>();

        let error = normalize_recovery_page_with_limits(&mut logs, &config(), 7, 7, 1, usize::MAX)
            .unwrap_err();

        assert_retryable_budget_error(error);
        assert_uncompacted(logs, pointers, &backings);
    }

    #[test]
    fn byte_overflow_rejects_before_compacting_tail_slice() {
        let backings = vec![large_backing()];
        let mut logs = vec![tail_log(0, &backings[0])];
        let pointers = vec![logs[0].data.as_ptr()];
        let max_bytes = logs[0].retained_bytes().saturating_sub(1);

        let error = normalize_recovery_page_with_limits(&mut logs, &config(), 7, 7, 1, max_bytes)
            .unwrap_err();

        assert_retryable_budget_error(error);
        assert_uncompacted(logs, pointers, &backings);
    }

    fn assert_retryable_budget_error(error: RuntimeError) {
        assert!(matches!(&error, RuntimeError::RecoveryLog(_)));
        assert!(error.retryable_recovery());
    }

    fn assert_uncompacted(logs: Vec<ContractLog>, pointers: Vec<*const u8>, backings: &[Bytes]) {
        for ((log, pointer), backing) in logs.into_iter().zip(pointers).zip(backings) {
            assert_eq!(log.data.as_ptr(), pointer);
            assert_eq!(log.data.len(), 1);
            assert_eq!(backing.len(), BACKING_BYTES);
            assert_eq!(pointer, backing.as_ptr().wrapping_add(BACKING_BYTES - 1));
        }
    }

    fn large_backing() -> Bytes {
        Bytes::from(vec![0x5a; BACKING_BYTES])
    }

    fn tail_log(log_index: u32, backing: &Bytes) -> ContractLog {
        let data = backing.slice(backing.len().saturating_sub(1)..);
        let mut cursor =
            ChainCursor::block(8453, 7, Some(B256::new([0x45; 32])), Commitment::Canonical);
        cursor.transaction_index = Some(0);
        cursor.log_index = Some(log_index);
        ContractLog {
            address: CORE,
            transaction_hash: None,
            topics: Vec::new(),
            data,
            removed: false,
            cursor,
        }
    }

    fn config() -> Config {
        Config {
            network: Network::Base,
            chain_id: 8453,
            core: CORE,
            deployment_block: 1,
            http_rpc_url: "http://127.0.0.1:1".into(),
            realtime_url: "ws://127.0.0.1:1".into(),
            redis_url: "redis://127.0.0.1:1".into(),
            redis_namespace: "validation".into(),
            consumer_group: "validation".into(),
            minimum_commitment: Commitment::Canonical,
            bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            source_queue_bound: 8,
            source_queue_byte_bound: 1 << 20,
            backfill_page_blocks: 100,
            fork_window_blocks: 128,
            fork_window_bytes: 1 << 20,
            fork_max_depth: 128,
            correction_event_bound: 128,
            correction_byte_bound: 1 << 20,
            redis_queue_bound: 8,
            redis_queue_byte_bound: 1 << 20,
            reconnect_delay: Duration::from_millis(10),
            source_stall_timeout: Duration::from_secs(1),
            redis_timeout: Duration::from_secs(2),
            #[cfg(all(feature = "monad-native", target_os = "linux"))]
            native_poll_interval: Duration::from_micros(100),
            shutdown_timeout: Duration::from_secs(2),
        }
    }
}
