//! Hash and proposal identity guards for state-mutating cursor transitions.

use super::ReducerError;
use crate::model::{ChainCursor, Commitment};

pub(super) fn validate_head_against_event(
    event: Option<&ChainCursor>,
    current: Option<&ChainCursor>,
    head: &ChainCursor,
) -> Result<(), ReducerError> {
    let Some(event) = event else {
        return Ok(());
    };
    if event.block_number != head.block_number
        || current.is_some_and(|current| head.block_number < current.block_number)
    {
        return Ok(());
    }
    let conflict = event.execution_block_number != head.execution_block_number
        || match (event.block_hash, head.block_hash) {
            (Some(left), Some(right)) => left != right,
            (None, Some(_)) => true,
            (None, None) => event.source_sequence.is_none() || head.source_sequence.is_none(),
            (Some(_), None) => true,
        };
    if conflict {
        return Err(ReducerError::BlockHashMismatch);
    }
    Ok(())
}

pub(super) fn validate_event_against_head(
    head: &ChainCursor,
    event: &ChainCursor,
) -> Result<(), ReducerError> {
    if head.block_number != event.block_number {
        return Ok(());
    }
    let conflict = head.execution_block_number != event.execution_block_number
        || match (head.block_hash, event.block_hash) {
            (Some(_), None) => true,
            (Some(left), Some(right)) => left != right,
            (None, None) => head.source_sequence.is_none() || event.source_sequence.is_none(),
            (None, Some(_)) => false,
        };
    if conflict {
        return Err(ReducerError::BlockHashMismatch);
    }
    Ok(())
}

pub(super) fn validate_event_successor(
    previous: &ChainCursor,
    next: &ChainCursor,
) -> Result<(), ReducerError> {
    if previous.block_number != next.block_number {
        return Ok(());
    }
    let conflict = previous.execution_block_number != next.execution_block_number
        || match (previous.block_hash, next.block_hash) {
            (Some(left), Some(right)) => left != right,
            (Some(_), None) | (None, Some(_)) => true,
            (None, None) => previous.source_sequence.is_none() || next.source_sequence.is_none(),
        };
    if conflict {
        return Err(ReducerError::BlockHashMismatch);
    }
    Ok(())
}

pub(super) fn is_realtime_progression(previous: &ChainCursor, next: &ChainCursor) -> bool {
    previous.commitment == Commitment::Realtime
        && next.commitment == Commitment::Realtime
        && next.source_sequence > previous.source_sequence
}
