use super::protocol::WsHead;
use crate::rpc::backend::RpcHttpBackend;
use lunarbase_client::{
    model::{ChainCursor, ChainUpdate, Commitment, Network, SourceError},
    state::ordering::CursorReorderBuffer,
};
use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};

const STANDARD_LOG_GRACE: Duration = Duration::from_secs(2);

pub(super) fn is_at_or_before_watermark(
    cursor: &ChainCursor,
    watermark: Option<&ChainCursor>,
) -> bool {
    watermark.is_some_and(|watermark| cursor.block_number <= watermark.block_number)
}

pub(super) fn observe_standard_head(
    open_heads: &mut VecDeque<(Instant, WsHead)>,
    head: WsHead,
    observed_at: Instant,
) {
    open_heads.push_back((observed_at, head));
}

pub(super) fn standard_head_deadline(open_heads: &VecDeque<(Instant, WsHead)>) -> Option<Instant> {
    open_heads
        .get(1)
        .map(|(successor_observed_at, _)| *successor_observed_at + STANDARD_LOG_GRACE)
}

pub(super) fn take_ready_standard_head(
    open_heads: &mut VecDeque<(Instant, WsHead)>,
    observed_at: Instant,
) -> Option<WsHead> {
    if standard_head_deadline(open_heads).is_none_or(|deadline| observed_at < deadline) {
        return None;
    }
    open_heads.pop_front().map(|(_, head)| head)
}

pub(super) fn drain_completed_block(
    reorder: &mut CursorReorderBuffer,
    head: &WsHead,
    allow_preceding_startup_logs: bool,
) -> Result<Vec<ChainUpdate>, SourceError> {
    let Some(block_hash) = head.cursor.block_hash else {
        return Err(SourceError::Gap(
            "completed RPC head has no block hash".into(),
        ));
    };
    let mut logs = Vec::new();
    let mut completed_head = None;
    for update in reorder.drain_through(&head.cursor) {
        match update {
            ChainUpdate::Head(cursor)
                if cursor.block_number == head.cursor.block_number
                    && cursor.block_hash == Some(block_hash) =>
            {
                completed_head = Some(ChainUpdate::Head(cursor));
            }
            ChainUpdate::Log(log)
                if log.cursor.block_number == head.cursor.block_number
                    && log.cursor.block_hash == Some(block_hash) =>
            {
                logs.push(with_execution_context(ChainUpdate::Log(log), &head.cursor));
            }
            ChainUpdate::Log(log)
                if allow_preceding_startup_logs
                    && log.cursor.block_number < head.cursor.block_number
                    && log.cursor.block_hash.is_some() =>
            {
                logs.push(ChainUpdate::Log(log));
            }
            other => {
                let (kind, cursor) = match &other {
                    ChainUpdate::Head(cursor) => ("head", cursor),
                    ChainUpdate::Log(log) => ("log", &log.cursor),
                    ChainUpdate::Reorg { new_head, .. } => ("reorg", new_head),
                    ChainUpdate::Gap { cursor, .. } => {
                        let block = cursor.as_ref().map_or(0, |cursor| cursor.block_number);
                        return Err(SourceError::Gap(format!(
                            "buffered gap at block {block} cannot complete RPC block {}",
                            head.cursor.block_number
                        )));
                    }
                };
                return Err(SourceError::Gap(format!(
                    "buffered RPC {kind} at block {} hash {:?} does not match completed block {} hash {block_hash:?}",
                    cursor.block_number, cursor.block_hash, head.cursor.block_number
                )));
            }
        }
    }
    let Some(completed_head) = completed_head else {
        return Err(SourceError::Gap(
            "completed RPC block has no matching buffered head".into(),
        ));
    };
    logs.push(completed_head);
    Ok(logs)
}

pub(super) async fn validate_preceding_startup_logs(
    updates: &mut [ChainUpdate],
    completed_head: &WsHead,
    http: &RpcHttpBackend,
) -> Result<(), SourceError> {
    let mut canonical_blocks = BTreeMap::<u64, ChainCursor>::new();
    for update in updates {
        let ChainUpdate::Log(log) = update else {
            continue;
        };
        if log.cursor.block_number >= completed_head.cursor.block_number {
            continue;
        }
        let block_number = log.cursor.block_number;
        if let std::collections::btree_map::Entry::Vacant(entry) =
            canonical_blocks.entry(block_number)
        {
            let tag = format!("0x{block_number:x}");
            let cursor = if http.network() == Network::Arbitrum {
                http.rpc()
                    .block_cursor_with_execution_context(
                        &tag,
                        http.chain_id(),
                        Commitment::Canonical,
                    )
                    .await
            } else {
                http.rpc()
                    .block_cursor(&tag, http.chain_id(), Commitment::Canonical)
                    .await
            }
            .map_err(SourceError::from)?;
            entry.insert(cursor);
        }
        let canonical = canonical_blocks
            .get(&block_number)
            .expect("canonical block was inserted above");
        if canonical.block_hash != log.cursor.block_hash {
            return Err(SourceError::Gap(format!(
                "startup RPC log at block {block_number} does not match its canonical block hash"
            )));
        }
        log.cursor.execution_block_number = canonical.execution_block_number;
    }
    Ok(())
}

pub(super) fn with_execution_context(mut update: ChainUpdate, head: &ChainCursor) -> ChainUpdate {
    if let ChainUpdate::Log(log) = &mut update
        && log.cursor.block_number == head.block_number
    {
        log.cursor.execution_block_number = head.execution_block_number;
    }
    update
}
