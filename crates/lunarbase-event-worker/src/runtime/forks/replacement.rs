//! Incrementally bounded replacement-log recovery for local fork resolution.

use super::RuntimeError;
use crate::{config::Config, redis_store::StoreError, runtime::validate_recovery_log};
use lunarbase_client::{
    model::{BackfillRequest, ContractFilter, ContractLog},
    source::ChainDataSource,
};
use lunarbase_source_evm::fork::{ForkError, ForkResolution};
use std::{collections::BTreeMap, mem::size_of};

pub(super) async fn replacement_logs<S: ChainDataSource>(
    source: &S,
    resolution: &ForkResolution,
    config: &Config,
    filter: &ContractFilter,
    base_retained_bytes: usize,
) -> Result<Vec<ContractLog>, RuntimeError> {
    let allowed = resolution
        .new_branch
        .iter()
        .map(|block| {
            block
                .cursor
                .block_hash
                .map(|hash| (block.cursor.block_number, hash))
                .ok_or_else(|| {
                    ForkError::InvalidIdentity("replacement block hash is absent".into())
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let from_block = resolution
        .common_ancestor
        .cursor
        .block_number
        .saturating_add(1);
    let to_block = resolution.new_tip.cursor.block_number;
    let mut logs = Vec::new();
    let max_logs = config.correction_event_bound.saturating_sub(2);
    let mut dynamic_bytes = 0_usize;
    let mut page_start = from_block;
    while page_start <= to_block {
        let page_end = page_start
            .saturating_add(config.backfill_page_blocks.saturating_sub(1))
            .min(to_block);
        let page = source
            .backfill(BackfillRequest {
                from_block: page_start,
                to_block: page_end,
                filter: filter.clone(),
            })
            .await?;
        validate_page(&page, &allowed, config, page_start, page_end)?;
        admit_page(
            &mut logs,
            page,
            max_logs,
            base_retained_bytes,
            &mut dynamic_bytes,
            config.correction_byte_bound,
        )?;
        if page_end == to_block {
            break;
        }
        page_start = page_end.saturating_add(1);
    }
    logs.sort_by_key(|log| log.cursor.event_order());
    if logs
        .windows(2)
        .any(|pair| pair[0].cursor.event_order() >= pair[1].cursor.event_order())
    {
        return Err(
            ForkError::InvalidIdentity("replacement logs are not strictly ordered".into()).into(),
        );
    }
    Ok(logs)
}

fn validate_page(
    page: &[ContractLog],
    allowed: &BTreeMap<u64, alloy_primitives::B256>,
    config: &Config,
    page_start: u64,
    page_end: u64,
) -> Result<(), RuntimeError> {
    for log in page {
        validate_recovery_log(log, config, page_start, page_end)?;
        if allowed.get(&log.cursor.block_number) != log.cursor.block_hash.as_ref() {
            return Err(ForkError::InvalidIdentity(
                "replacement backfill disagrees with resolved branch".into(),
            )
            .into());
        }
    }
    Ok(())
}

fn admit_page(
    logs: &mut Vec<ContractLog>,
    mut page: Vec<ContractLog>,
    max_logs: usize,
    base_retained_bytes: usize,
    dynamic_bytes: &mut usize,
    max_bytes: usize,
) -> Result<(), RuntimeError> {
    let next_len = logs.len().saturating_add(page.len());
    let page_dynamic = page.iter().fold(0_usize, |total, log| {
        total.saturating_add(
            log.retained_bytes()
                .saturating_sub(size_of::<ContractLog>()),
        )
    });
    if next_len > max_logs
        || charge(base_retained_bytes, next_len, *dynamic_bytes, page_dynamic) > max_bytes
    {
        return Err(budget(
            "replacement backfill exceeds correction count or byte budget",
        ));
    }
    logs.try_reserve_exact(page.len())
        .map_err(|_| budget("replacement backfill allocation failed"))?;
    if charge(
        base_retained_bytes,
        logs.capacity(),
        *dynamic_bytes,
        page_dynamic,
    ) > max_bytes
    {
        return Err(budget(
            "replacement backfill allocation exceeds correction byte budget",
        ));
    }
    for log in &mut page {
        log.normalize_for_retention();
    }
    debug_assert_eq!(
        page_dynamic,
        page.iter().fold(0_usize, |total, log| {
            total.saturating_add(
                log.retained_bytes()
                    .saturating_sub(size_of::<ContractLog>()),
            )
        })
    );
    *dynamic_bytes = (*dynamic_bytes).saturating_add(page_dynamic);
    logs.extend(page);
    Ok(())
}

fn charge(base: usize, slots: usize, dynamic: usize, added: usize) -> usize {
    base.saturating_add(size_of::<Vec<ContractLog>>())
        .saturating_add(slots.saturating_mul(size_of::<ContractLog>()))
        .saturating_add(dynamic)
        .saturating_add(added)
}

fn budget(reason: &str) -> RuntimeError {
    StoreError::CorrectionBudget(reason.into()).into()
}
