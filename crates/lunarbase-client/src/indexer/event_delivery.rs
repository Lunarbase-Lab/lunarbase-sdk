//! Ownership-preserving delivery helpers for the optional Core event observer.

use crate::indexer::client_types::{ClientRuntimeStats, CoreEventSink};
use crate::model::ContractLog;
use std::sync::atomic::Ordering;

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

pub(super) fn try_observe_core_event(
    sink: Option<&CoreEventSink>,
    log: ContractLog,
    stats: &ClientRuntimeStats,
) {
    if let Some(sink) = sink
        && sink.accepts(log.cursor.commitment)
        && sink.sender.try_send(log).is_err()
    {
        stats.event_observer_drops.fetch_add(1, Ordering::Relaxed);
    }
}
