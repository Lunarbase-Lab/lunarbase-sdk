//! Deadline-bounded canonical catch-up for compatible durable checkpoints.

use crate::indexer::client_types::{ClientRuntimeStats, CoreEventSink};
use crate::indexer::engine::{QuoteIndexer, validate_core_recovery_log};
use crate::indexer::errors::IndexerError;
use crate::indexer::event_delivery::try_observe_core_event;
use crate::indexer::runtime_helpers::source_operation;
use crate::indexer::tasks::{RECOVERY_LIMITS, RecoveryLimits, RecoveryUsage, recovery_page_end};
use crate::model::{BackfillRequest, ChainUpdate, ContractFilter, ContractLog};
use crate::source::ChainDataSource;
use crate::state::reducer::ReducerError;
use std::time::Duration;

pub(super) async fn recover_checkpoint<S: ChainDataSource>(
    indexer: &mut QuoteIndexer,
    source: &S,
    filter: &ContractFilter,
    core_event_sink: Option<&CoreEventSink>,
    stats: &ClientRuntimeStats,
    operation_timeout: Duration,
) -> Result<(), IndexerError> {
    let checkpoint_cursor = indexer
        .reducer
        .cursor()
        .cloned()
        .ok_or(IndexerError::NoCursor)?;
    indexer.reducer.mark_not_ready();
    let head = source_operation(
        "checkpoint canonical head",
        operation_timeout,
        source.canonical_head(),
    )
    .await?;
    if head.chain_id != checkpoint_cursor.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if head.block_number < checkpoint_cursor.block_number {
        return Err(IndexerError::Gap(
            "canonical head regressed below checkpoint".into(),
        ));
    }
    let from_block =
        if checkpoint_cursor.transaction_index.is_none() && checkpoint_cursor.log_index.is_none() {
            checkpoint_cursor.block_number.saturating_add(1)
        } else {
            checkpoint_cursor.block_number
        };
    if from_block <= head.block_number {
        let mut usage = RecoveryUsage::default();
        let mut page_start = from_block;
        loop {
            let page_end = recovery_page_end(page_start, head.block_number);
            let mut logs = load_page(
                source,
                filter,
                indexer.deployment().core,
                indexer.deployment().chain_id,
                page_start,
                page_end,
                &mut usage,
                RECOVERY_LIMITS,
                operation_timeout,
            )
            .await?;
            logs.sort_by_key(|log| log.cursor.event_order());
            for log in logs {
                if log.cursor.event_order() <= checkpoint_cursor.event_order() {
                    continue;
                }
                if core_event_sink.is_some_and(|sink| sink.accepts(log.cursor.commitment)) {
                    if let Some(log) = indexer.apply_core_log_for_delivery(log)? {
                        try_observe_core_event(core_event_sink, log, stats);
                    }
                } else {
                    indexer.apply_core_update(ChainUpdate::Log(log))?;
                }
            }
            if page_end == head.block_number {
                break;
            }
            page_start = page_end.saturating_add(1);
        }
    }
    indexer.apply_core_update(ChainUpdate::Head(crate::model::BlockRef::new(
        head.clone(),
        None,
    )))?;
    indexer.set_canonical_floor(head)?;
    indexer.reducer.publish_ready();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn load_page<S: ChainDataSource>(
    source: &S,
    filter: &ContractFilter,
    core: lunarbase_math::Address,
    chain_id: u64,
    from_block: u64,
    to_block: u64,
    usage: &mut RecoveryUsage,
    limits: RecoveryLimits,
    operation_timeout: Duration,
) -> Result<Vec<ContractLog>, IndexerError> {
    let mut logs = source_operation(
        "checkpoint backfill",
        operation_timeout,
        source.backfill(BackfillRequest {
            from_block,
            to_block,
            filter: filter.clone(),
        }),
    )
    .await?;
    prepare_page(
        &mut logs, core, chain_id, from_block, to_block, usage, limits,
    )?;
    Ok(logs)
}

#[allow(clippy::too_many_arguments)]
fn prepare_page(
    logs: &mut [ContractLog],
    core: lunarbase_math::Address,
    chain_id: u64,
    from_block: u64,
    to_block: u64,
    usage: &mut RecoveryUsage,
    limits: RecoveryLimits,
) -> Result<(), IndexerError> {
    for log in logs.iter() {
        validate_core_recovery_log(log, core, chain_id, from_block..=to_block)?;
    }
    usage.admit(logs, limits)?;
    for log in logs {
        log.normalize_for_retention();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{RecoveryLimits, RecoveryUsage, prepare_page};
    use crate::bootstrap::BootstrapSnapshot;
    use crate::indexer::errors::IndexerError;
    use crate::model::{
        BackfillRequest, ChainCursor, Checkpoint, Commitment, ContractFilter, ContractLog,
        DeploymentConfig, Network, SourceError,
    };
    use crate::source::{ChainDataSource, SourceStream};
    use lunarbase_math::{Address, B256, Bytes};

    const CORE: Address = Address::new([0x33; 20]);
    const CHAIN_ID: u64 = 8453;

    #[tokio::test]
    async fn custom_source_count_rejection_precedes_compaction() {
        let source = PageSource {
            logs: vec![sliced_log(0), sliced_log(1)],
        };
        let mut page = source.backfill(request()).await.unwrap();
        let pointers = page.iter().map(|log| log.data.as_ptr()).collect::<Vec<_>>();

        let result = prepare_page(
            &mut page,
            CORE,
            CHAIN_ID,
            7,
            7,
            &mut RecoveryUsage::default(),
            RecoveryLimits::new(1, usize::MAX),
        );

        assert!(matches!(result, Err(IndexerError::Gap(_))));
        assert_eq!(
            page.iter().map(|log| log.data.as_ptr()).collect::<Vec<_>>(),
            pointers,
            "rejected source page must not compact-copy its sliced buffers"
        );
    }

    #[tokio::test]
    async fn custom_source_byte_rejection_precedes_compaction() {
        let source = PageSource {
            logs: vec![sliced_log(0)],
        };
        let mut page = source.backfill(request()).await.unwrap();
        let pointer = page[0].data.as_ptr();
        let byte_limit = page[0].retained_bytes().saturating_sub(1);

        let result = prepare_page(
            &mut page,
            CORE,
            CHAIN_ID,
            7,
            7,
            &mut RecoveryUsage::default(),
            RecoveryLimits::new(1, byte_limit),
        );

        assert!(matches!(result, Err(IndexerError::Gap(_))));
        assert_eq!(page[0].data.as_ptr(), pointer);
    }

    struct PageSource {
        logs: Vec<ContractLog>,
    }

    impl ChainDataSource for PageSource {
        fn network(&self) -> Network {
            Network::Base
        }

        async fn snapshot(
            &self,
            _deployment: &DeploymentConfig,
        ) -> Result<BootstrapSnapshot, SourceError> {
            unreachable!("page source only supports backfill")
        }

        async fn backfill(
            &self,
            _request: BackfillRequest,
        ) -> Result<Vec<ContractLog>, SourceError> {
            Ok(self.logs.clone())
        }

        async fn subscribe(&self, _filter: ContractFilter) -> Result<SourceStream, SourceError> {
            unreachable!("page source only supports backfill")
        }

        async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
            unreachable!("page source only supports backfill")
        }

        async fn validate_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<bool, SourceError> {
            unreachable!("page source only supports backfill")
        }
    }

    fn request() -> BackfillRequest {
        BackfillRequest {
            from_block: 7,
            to_block: 7,
            filter: ContractFilter {
                address: CORE,
                topics: Vec::new(),
            },
        }
    }

    fn sliced_log(log_index: u32) -> ContractLog {
        let backing = Bytes::from(vec![0x5a; 1 << 20]);
        let data = backing.slice(backing.len().saturating_sub(1)..);
        drop(backing);
        let mut cursor = ChainCursor::block(
            CHAIN_ID,
            7,
            Some(B256::new([0x44; 32])),
            Commitment::Canonical,
        );
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
}
