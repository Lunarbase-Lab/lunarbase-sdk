//! Identity-aware source coverage retained across recovery retries.

use super::RuntimeError;
use lunarbase_client::model::{ChainCursor, Commitment};

pub(super) fn recovery_log_is_covered(
    has_fork_runtime: bool,
    update: &ChainCursor,
    durable: &ChainCursor,
) -> bool {
    update.block_number < durable.block_number
        || (has_fork_runtime && update.event_order() <= durable.event_order())
}

pub(super) fn merge(
    required: &mut Option<ChainCursor>,
    mut candidate: ChainCursor,
) -> Result<(), RuntimeError> {
    let Some(current) = required.as_ref().cloned() else {
        *required = Some(candidate);
        return Ok(());
    };
    if current.chain_id != candidate.chain_id {
        return Err(conflict("required cursor changed chain identity"));
    }
    if candidate.block_number < current.block_number {
        return Ok(());
    }
    if candidate.block_number == current.block_number {
        let same = immutable_identity(&current, &candidate)?;
        if same {
            if candidate.event_order() < current.event_order() {
                if candidate.commitment > current.commitment {
                    let mut promoted = current;
                    promoted.commitment = candidate.commitment;
                    *required = Some(promoted);
                }
                return Ok(());
            }
            candidate.commitment = candidate.commitment.max(current.commitment);
        } else if current.commitment == Commitment::Finalized
            || candidate.commitment < current.commitment
        {
            return Err(conflict(
                "required cursor conflicts with a stronger same-height identity",
            ));
        }
    }
    *required = Some(candidate);
    Ok(())
}

pub(super) fn head_covers(
    required: &ChainCursor,
    head: &ChainCursor,
) -> Result<bool, RuntimeError> {
    if required.chain_id != head.chain_id {
        return Err(conflict("canonical head changed chain identity"));
    }
    if head.block_number > required.block_number {
        return Ok(true);
    }
    if head.block_number < required.block_number {
        return Ok(false);
    }
    if immutable_identity(required, head)? {
        return Ok(head.commitment >= required.commitment);
    }
    if required.commitment == Commitment::Finalized {
        return Err(conflict(
            "canonical head conflicts with a finalized requirement",
        ));
    }
    Ok(head.commitment >= Commitment::Canonical && head.commitment >= required.commitment)
}

fn immutable_identity(left: &ChainCursor, right: &ChainCursor) -> Result<bool, RuntimeError> {
    match (left.block_hash, right.block_hash) {
        (Some(left_hash), Some(right_hash)) if left_hash == right_hash => {
            if left.execution_block_number != right.execution_block_number {
                return Err(conflict(
                    "same block hash has conflicting execution context",
                ));
            }
            Ok(true)
        }
        (Some(_), Some(_)) => Ok(false),
        _ => Err(conflict("recovery cursor has no immutable block hash")),
    }
}

fn conflict(detail: &str) -> RuntimeError {
    RuntimeError::RecoveryLog(detail.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    fn cursor(block: u64, hash: u8, commitment: Commitment) -> ChainCursor {
        ChainCursor::block(8453, block, Some(B256::new([hash; 32])), commitment)
    }

    #[test]
    fn lagging_head_does_not_cover_an_oversized_update_watermark() {
        let required = cursor(105, 1, Commitment::Realtime);
        assert!(!head_covers(&required, &cursor(104, 2, Commitment::Canonical)).unwrap());
        assert!(head_covers(&required, &cursor(105, 1, Commitment::Canonical)).unwrap());
    }

    #[test]
    fn requirement_never_downgrades_commitment() {
        let mut required = Some(cursor(105, 1, Commitment::Finalized));
        assert!(merge(&mut required, cursor(105, 1, Commitment::Realtime)).is_ok());
        assert_eq!(required.unwrap().commitment, Commitment::Finalized);
    }
}
