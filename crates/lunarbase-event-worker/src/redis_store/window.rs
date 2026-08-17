//! Bounded startup reconstruction of the durable canonical header window.

use super::{JournalWindow, RedisKeys, StoreError, redis_error};
use crate::event::decode_header;
use lunarbase_client::model::{BlockRef, Commitment};
use redis::Connection;

pub(super) fn load(
    connection: &mut Connection,
    keys: &RedisKeys,
    chain_id: u64,
    max_blocks: usize,
    max_bytes: usize,
) -> Result<JournalWindow, StoreError> {
    if max_blocks == 0 || max_bytes == 0 {
        return Err(StoreError::Journal(
            "invalid durable fork-window limits".into(),
        ));
    }
    let canonical_hash = redis::cmd("GET")
        .arg(&keys.canonical_head)
        .query::<Option<String>>(connection)
        .map_err(redis_error)?;
    let finalized_hash = redis::cmd("GET")
        .arg(&keys.finalized_head)
        .query::<Option<String>>(connection)
        .map_err(redis_error)?;
    let Some(canonical_hash) = canonical_hash else {
        if finalized_hash.is_some() {
            return Err(StoreError::Journal(
                "finalized head exists without canonical head".into(),
            ));
        }
        return Ok(JournalWindow::default());
    };
    let canonical_payload = load_header(connection, keys, &canonical_hash)?;
    let canonical = decode_header(&canonical_payload, chain_id)?;
    if canonical.cursor.block_hash.map(|hash| format!("{hash:#x}")) != Some(canonical_hash.clone())
    {
        return Err(StoreError::Journal(
            "canonical head key disagrees with header identity".into(),
        ));
    }

    let first_height = canonical
        .cursor
        .block_number
        .saturating_add(1)
        .saturating_sub(max_blocks as u64);
    let mut height_pipeline = redis::pipe();
    for height in first_height..=canonical.cursor.block_number {
        height_pipeline
            .cmd("HGET")
            .arg(&keys.canonical_height)
            .arg(height);
    }
    let hashes = height_pipeline
        .query::<Vec<Option<String>>>(connection)
        .map_err(redis_error)?;
    if hashes.last().and_then(Option::as_deref) != Some(canonical_hash.as_str()) {
        return Err(StoreError::Journal(
            "canonical head is absent from the height index".into(),
        ));
    }
    let suffix_start = hashes
        .iter()
        .rposition(Option::is_none)
        .map_or(0, |index| index.saturating_add(1));
    let retained = hashes
        .into_iter()
        .enumerate()
        .skip(suffix_start)
        .map(|(offset, hash)| {
            hash.map(|hash| (first_height.saturating_add(offset as u64), hash))
                .ok_or_else(|| {
                    StoreError::Journal("canonical suffix contains an internal gap".into())
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut header_pipeline = redis::pipe();
    for (_, hash) in &retained {
        header_pipeline.cmd("HGET").arg(&keys.headers).arg(hash);
    }
    let payloads = header_pipeline
        .query::<Vec<Option<String>>>(connection)
        .map_err(redis_error)?;
    let mut blocks: Vec<BlockRef> = Vec::with_capacity(payloads.len());
    let mut charged = 0_usize;
    for ((height, hash), payload) in retained.into_iter().zip(payloads) {
        let payload = payload
            .ok_or_else(|| StoreError::Journal(format!("canonical header {hash} is missing")))?;
        charged = charged
            .saturating_add(payload.len())
            .saturating_add(std::mem::size_of::<BlockRef>());
        if charged > max_bytes {
            return Err(StoreError::CorrectionBudget(
                "durable fork window byte limit exceeded".into(),
            ));
        }
        let block = decode_header(&payload, chain_id)?;
        if block.cursor.block_number != height
            || block.cursor.block_hash.map(|value| format!("{value:#x}")) != Some(hash)
        {
            return Err(StoreError::Journal(
                "canonical height/header identity mismatch".into(),
            ));
        }
        if let Some(previous) = blocks.last()
            && (block.cursor.block_number != previous.cursor.block_number.saturating_add(1)
                || block.parent_hash != previous.cursor.block_hash)
        {
            return Err(StoreError::Journal(
                "durable canonical headers are disconnected".into(),
            ));
        }
        blocks.push(block);
    }

    let finalized = finalized_hash
        .map(|hash| {
            let payload = load_header(connection, keys, &hash)?;
            let mut block = decode_header(&payload, chain_id)?;
            if block.cursor.block_hash.map(|value| format!("{value:#x}")) != Some(hash) {
                return Err(StoreError::Journal(
                    "finalized head key disagrees with header identity".into(),
                ));
            }
            block.cursor.commitment = Commitment::Finalized;
            Ok(block)
        })
        .transpose()?;
    Ok(JournalWindow { blocks, finalized })
}

fn load_header(
    connection: &mut Connection,
    keys: &RedisKeys,
    hash: &str,
) -> Result<String, StoreError> {
    redis::cmd("HGET")
        .arg(&keys.headers)
        .arg(hash)
        .query::<Option<String>>(connection)
        .map_err(redis_error)?
        .ok_or_else(|| StoreError::Journal(format!("durable header {hash} is missing")))
}
