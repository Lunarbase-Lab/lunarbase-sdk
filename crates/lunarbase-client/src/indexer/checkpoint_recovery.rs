//! Deadline-bounded canonical catch-up for compatible durable checkpoints.

use crate::indexer::client_types::{ClientRuntimeStats, CoreEventSink};
use crate::indexer::engine::{QuoteIndexer, validate_core_recovery_log};
use crate::indexer::errors::IndexerError;
use crate::indexer::event_delivery::try_observe_core_event;
use crate::indexer::runtime_helpers::source_operation;
use crate::indexer::tasks::recovery_page_end;
use crate::model::{BackfillRequest, ChainUpdate, ContractFilter};
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
        let mut page_start = from_block;
        loop {
            let page_end = recovery_page_end(page_start, head.block_number);
            let mut logs = source_operation(
                "checkpoint backfill",
                operation_timeout,
                source.backfill(BackfillRequest {
                    from_block: page_start,
                    to_block: page_end,
                    filter: filter.clone(),
                }),
            )
            .await?;
            logs.sort_by_key(|log| log.cursor.event_order());
            for log in logs {
                validate_core_recovery_log(
                    &log,
                    indexer.deployment().core,
                    indexer.deployment().chain_id,
                    page_start..=page_end,
                )?;
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
    indexer.apply_core_update(ChainUpdate::Head(head.clone()))?;
    indexer.set_canonical_floor(head);
    indexer.reducer.publish_ready();
    Ok(())
}
