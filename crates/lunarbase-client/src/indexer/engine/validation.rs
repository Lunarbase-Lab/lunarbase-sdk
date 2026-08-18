//! Identity, recovery-bound, and handoff-order validation.

use crate::bootstrap::BootstrapSnapshot;
use crate::indexer::errors::IndexerError;
use crate::model::{ChainCursor, ChainUpdate, ContractLog, DeploymentConfig, SourceError};
use crate::state::reducer::ReducerError;
use lunarbase_math::arithmetic::BPS;
use lunarbase_math::{Address, U256};
use std::ops::RangeInclusive;

pub(super) fn validate_verified_router_snapshot(
    snapshot: &BootstrapSnapshot,
    deployment: &DeploymentConfig,
) -> Result<(), IndexerError> {
    match (
        deployment.verified_router,
        snapshot.verified_router.as_ref(),
    ) {
        (None, None) => Ok(()),
        (Some(expected), Some(actual))
            if actual.router == expected
                && actual.partner_fee_bps.len() <= snapshot.state.lanes.len().saturating_add(1)
                && actual.partner_fee_bps.iter().all(|(asset, fee)| {
                    (*asset == snapshot.state.cash || snapshot.state.lanes.contains_key(asset))
                        && U256::from(*fee) <= BPS
                }) =>
        {
            Ok(())
        }
        _ => Err(SourceError::Unavailable(
            "snapshot verified-router policy does not match deployment".into(),
        )
        .into()),
    }
}

/// Rejects source/filter violations before ABI decoding or event publication.
pub(crate) fn validate_core_log_identity(
    log: &ContractLog,
    expected_core: Address,
    expected_chain_id: u64,
) -> Result<(), IndexerError> {
    if log.address != expected_core {
        return Err(ReducerError::ContractAddressMismatch.into());
    }
    if log.cursor.chain_id != expected_chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    Ok(())
}

/// Validates canonical backfill identity and bounds before cursor filtering.
pub(crate) fn validate_core_recovery_log(
    log: &ContractLog,
    expected_core: Address,
    expected_chain_id: u64,
    block_range: RangeInclusive<u64>,
) -> Result<(), IndexerError> {
    validate_core_log_identity(log, expected_core, expected_chain_id)?;
    if log.removed
        || log.cursor.block_hash.is_none()
        || !block_range.contains(&log.cursor.block_number)
    {
        return Err(IndexerError::Gap(
            "canonical recovery backfill returned an invalid log".into(),
        ));
    }
    Ok(())
}

pub(crate) fn snapshot_covers(
    update: &ChainCursor,
    snapshot: &ChainCursor,
) -> Result<bool, IndexerError> {
    if update.chain_id != snapshot.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if update.block_number < snapshot.block_number {
        return Ok(true);
    }
    if update.block_number > snapshot.block_number {
        return Ok(false);
    }
    match (update.block_hash, snapshot.block_hash) {
        (Some(update_hash), Some(snapshot_hash)) if update_hash == snapshot_hash => {
            if update.execution_block_number == snapshot.execution_block_number {
                Ok(snapshot.commitment >= update.commitment)
            } else {
                Err(ReducerError::BlockHashMismatch.into())
            }
        }
        (Some(_), Some(_)) => Ok(false),
        _ => Err(IndexerError::Gap(
            "same-block handoff has no hash identity; canonical recovery required".into(),
        )),
    }
}

pub(super) fn canonical_floor_covers_log(
    update: &ChainCursor,
    floor: &ChainCursor,
) -> Result<bool, IndexerError> {
    if update.chain_id != floor.chain_id {
        return Err(ReducerError::ChainIdMismatch.into());
    }
    if update.block_number < floor.block_number {
        return Ok(true);
    }
    if update.block_number > floor.block_number {
        return Ok(false);
    }
    match (update.block_hash, floor.block_hash) {
        (Some(update_hash), Some(floor_hash)) if update_hash == floor_hash => {
            if update.execution_block_number != floor.execution_block_number {
                return Err(ReducerError::BlockHashMismatch.into());
            }
            let floor_is_block_complete =
                floor.transaction_index.is_none() && floor.log_index.is_none();
            Ok(floor_is_block_complete || update.event_order() <= floor.event_order())
        }
        (Some(_), Some(_)) => Err(ReducerError::BlockHashMismatch.into()),
        _ => Err(IndexerError::Gap(
            "same-block realtime log has no canonical hash identity".into(),
        )),
    }
}

pub(super) fn update_cursor(update: &ChainUpdate) -> Option<&ChainCursor> {
    match update {
        ChainUpdate::Head(head) => Some(&head.cursor),
        ChainUpdate::Log(log) => Some(&log.cursor),
        ChainUpdate::Reorg { new_head, .. } => Some(&new_head.cursor),
        ChainUpdate::Correction(correction) => Some(&correction.new_tip.cursor),
        ChainUpdate::Gap { cursor, .. } => cursor.as_ref(),
    }
}

fn update_order(update: &ChainUpdate) -> (u64, u32, u32, u64, u32, u8) {
    if let ChainUpdate::Correction(correction) = update {
        return branch_end_order(&correction.new_tip.cursor, 2);
    }
    if let ChainUpdate::Reorg { new_head, .. } = update {
        return branch_end_order(&new_head.cursor, 3);
    }
    let cursor = update_cursor(update);
    let order = cursor.map_or((u64::MAX, 0, 0, 0, 0), ChainCursor::event_order);
    let rank = match update {
        ChainUpdate::Head(_) => 0,
        ChainUpdate::Log(_) => 1,
        ChainUpdate::Correction(_) => 2,
        ChainUpdate::Reorg { .. } => 3,
        ChainUpdate::Gap { .. } => 4,
    };
    (order.0, order.1, order.2, order.3, order.4, rank)
}

fn branch_end_order(cursor: &ChainCursor, rank: u8) -> (u64, u32, u32, u64, u32, u8) {
    (
        cursor.block_number,
        u32::MAX,
        u32::MAX,
        u64::MAX,
        u32::MAX,
        rank,
    )
}

pub(crate) fn sort_chain_updates(updates: &mut [ChainUpdate]) {
    let mut segment_start = 0;
    for index in 0..updates.len() {
        if matches!(
            updates[index],
            ChainUpdate::Correction(_) | ChainUpdate::Reorg { .. } | ChainUpdate::Gap { .. }
        ) {
            updates[segment_start..index].sort_by_key(update_order);
            segment_start = index.saturating_add(1);
        }
    }
    updates[segment_start..].sort_by_key(update_order);
}

/// Orders a recovery attempt without cloning the staged update payloads.
pub(crate) fn sort_chain_update_refs(updates: &mut [&ChainUpdate]) {
    let mut segment_start = 0;
    for index in 0..updates.len() {
        if matches!(
            updates[index],
            ChainUpdate::Correction(_) | ChainUpdate::Reorg { .. } | ChainUpdate::Gap { .. }
        ) {
            updates[segment_start..index].sort_by_key(|update| update_order(update));
            segment_start = index.saturating_add(1);
        }
    }
    updates[segment_start..].sort_by_key(|update| update_order(update));
}

/// Orders indexed borrowed updates with the same control-message barriers as
/// owned handoff and recovery batches.
pub(crate) fn sort_chain_update_refs_with_indices(updates: &mut [(usize, &ChainUpdate)]) {
    let mut segment_start = 0;
    for index in 0..updates.len() {
        if matches!(
            updates[index].1,
            ChainUpdate::Correction(_) | ChainUpdate::Reorg { .. } | ChainUpdate::Gap { .. }
        ) {
            updates[segment_start..index].sort_by_key(|(_, update)| update_order(update));
            segment_start = index.saturating_add(1);
        }
    }
    updates[segment_start..].sort_by_key(|(_, update)| update_order(update));
}
