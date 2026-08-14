//! Materialization helpers for applied, removed, and head lifecycle outputs.

use crate::execution::{ExecutionHead, ExecutionLog};
use lunarbase_client::model::Commitment;
use lunarbase_math::B256;

pub(super) fn materialize_log(
    mut log: ExecutionLog,
    sequence: Option<u64>,
    block_hash: B256,
    commitment: Commitment,
    removed: bool,
) -> ExecutionLog {
    if let Some(sequence) = sequence {
        log.sequence = sequence;
        log.source_sub_index = log.log_index;
    }
    log.block_hash = Some(block_hash);
    log.commitment = commitment;
    log.removed = removed;
    log
}

pub(super) fn head(
    sequence: u64,
    block_number: u64,
    block_hash: Option<B256>,
    commitment: Commitment,
) -> ExecutionHead {
    ExecutionHead {
        sequence,
        block_number,
        block_hash,
        commitment,
    }
}
