//! Ownership-preserving delivery helpers for the optional Core event sink.

use crate::indexer::client_types::CoreEventSink;
use crate::indexer::engine::{QuoteIndexer, validate_core_log_identity};
use crate::indexer::errors::IndexerError;
use crate::model::{ChainUpdate, ContractLog};

pub(super) async fn emit_handoff_events(
    indexer: &mut QuoteIndexer,
    buffered: &[ChainUpdate],
    core_event_sink: Option<&CoreEventSink>,
    skip_canonical_covered: bool,
) -> Result<(), IndexerError> {
    let Some(core_event_sink) = core_event_sink else {
        return Ok(());
    };
    for update in buffered {
        if let ChainUpdate::Log(log) = update {
            validate_core_log_identity(
                log,
                indexer.deployment().core,
                indexer.deployment().chain_id,
            )?;
        }
    }
    let mut logs = buffered
        .iter()
        .filter_map(|update| match update {
            ChainUpdate::Log(log) if core_event_sink.accepts(log.cursor.commitment) => {
                Some(log.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    logs.sort_by_key(|log| log.cursor.event_order());
    logs.dedup_by(|right, left| same_core_event_identity(left, right));
    for log in logs {
        if skip_canonical_covered && indexer.canonical_floor_covers_core_log(&log)? {
            continue;
        }
        send_required_core_event(Some(core_event_sink), log).await?;
    }
    Ok(())
}

pub(super) fn same_core_event_identity(left: &ContractLog, right: &ContractLog) -> bool {
    left.address == right.address
        && left.transaction_hash == right.transaction_hash
        && left.topics == right.topics
        && left.data == right.data
        && left.removed == right.removed
        && left.cursor.chain_id == right.cursor.chain_id
        && left.cursor.block_hash == right.cursor.block_hash
        && left.cursor.event_order() == right.cursor.event_order()
}

pub(super) async fn send_required_core_event(
    sink: Option<&CoreEventSink>,
    log: ContractLog,
) -> Result<(), IndexerError> {
    if let Some(sink) = sink
        && sink.accepts(log.cursor.commitment)
    {
        sink.sender
            .send(log)
            .await
            .map_err(|_| IndexerError::EventSinkClosed)?;
    }
    Ok(())
}
