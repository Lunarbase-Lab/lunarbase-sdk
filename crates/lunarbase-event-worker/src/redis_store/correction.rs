//! Rare-path bounded and atomic Redis fork correction.

use super::{CorrectionOutcome, RedisKeys, StoreError, commands::DeploymentMetadata, redis_error};
use crate::event::ReorgCorrection;
use redis::{Connection, Script, streams::StreamRangeReply};
use serde::Serialize;
use std::collections::BTreeMap;

const CORRECTION_LUA: &str = include_str!("correction.lua");

#[derive(Clone, Copy, Debug)]
pub(crate) struct CorrectionLimits {
    pub max_events: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Plan<'a> {
    fingerprint: &'a str,
    reorg_id: &'a str,
    begin_record_id: &'a str,
    commit_record_id: &'a str,
    old_tip_hash: String,
    new_tip_hash: String,
    finalized_hash: String,
    cursor_json: &'a str,
    cursor_order: &'a str,
    control_fields: Vec<String>,
    old_blocks: Vec<OldBlock<'a>>,
    old_logs: Vec<OldLog>,
    new_heads: Vec<NewHead<'a>>,
    new_events: Vec<NewEvent<'a>>,
    removed_reference_count: usize,
    removed_reference_bytes: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OldBlock<'a> {
    block_hash: &'a str,
    block_number: &'a str,
    key_index: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OldLog {
    logical_log_id: String,
    source_record_id: String,
    source_stream_id: String,
    record_id: String,
    block_hash: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NewHead<'a> {
    header_json: &'a str,
    block_hash: &'a str,
    block_number: &'a str,
    header_bytes: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NewEvent<'a> {
    record_id: &'a str,
    logical_log_id: &'a str,
    block_hash: &'a str,
    cursor_order: &'a str,
    journal_bytes: usize,
    key_index: usize,
    fields: Vec<String>,
}

pub(super) fn script() -> Script {
    Script::new(CORRECTION_LUA)
}

pub(super) fn correct(
    connection: &mut Connection,
    keys: &RedisKeys,
    metadata: &DeploymentMetadata,
    script: &Script,
    correction: &ReorgCorrection,
    limits: CorrectionLimits,
) -> Result<CorrectionOutcome, StoreError> {
    if correction.retained_bytes() > limits.max_bytes {
        return Err(StoreError::CorrectionBudget(
            "correction byte limit exceeded".into(),
        ));
    }
    let base_event_count = correction.new_events.len().saturating_add(2);
    if base_event_count > limits.max_events {
        return Err(StoreError::CorrectionBudget(
            "correction event limit exceeded".into(),
        ));
    }
    let reference_lists = load_references(
        connection,
        keys,
        correction,
        limits.max_events - base_event_count,
    )?;
    let (old_logs, reference_bytes) = materialize_old_logs(correction, &reference_lists)?;
    let event_count = old_logs
        .len()
        .saturating_add(correction.new_events.len())
        .saturating_add(2);
    if event_count > limits.max_events {
        return Err(StoreError::CorrectionBudget(
            "correction event limit exceeded".into(),
        ));
    }
    let source_bytes = load_source_bytes(connection, keys, &old_logs)?;

    let old_key_start = 14;
    let new_key_start = old_key_start + correction.old_blocks.len();
    let new_key_indices = correction
        .new_heads
        .iter()
        .enumerate()
        .map(|(index, head)| (head.block_hash.as_str(), new_key_start + index))
        .collect::<BTreeMap<_, _>>();
    let control_fields = correction
        .control_fields(old_logs.len())?
        .into_iter()
        .flat_map(|(name, value)| [name.to_owned(), value])
        .collect();
    let plan = Plan {
        fingerprint: metadata.fingerprint(),
        reorg_id: &correction.reorg_id,
        begin_record_id: &correction.begin_record_id,
        commit_record_id: &correction.commit_record_id,
        old_tip_hash: required_hash(&correction.old_tip)?,
        new_tip_hash: required_hash(&correction.new_tip)?,
        finalized_hash: required_hash(&correction.finalized)?,
        cursor_json: &correction.cursor_json,
        cursor_order: &correction.cursor_order,
        control_fields,
        old_blocks: correction
            .old_blocks
            .iter()
            .enumerate()
            .map(|(index, block)| OldBlock {
                block_hash: &block.block_hash,
                block_number: &block.block_number,
                key_index: old_key_start + index,
            })
            .collect(),
        old_logs,
        new_heads: correction
            .new_heads
            .iter()
            .map(|head| NewHead {
                header_json: &head.header_json,
                block_hash: &head.block_hash,
                block_number: &head.block_number,
                header_bytes: head.header_json.len(),
            })
            .collect(),
        new_events: correction
            .new_events
            .iter()
            .map(|event| {
                let key_index = new_key_indices
                    .get(event.block_hash.as_str())
                    .copied()
                    .ok_or_else(|| {
                        StoreError::Journal("replacement log has no replacement header".into())
                    })?;
                Ok(NewEvent {
                    record_id: &event.record_id,
                    logical_log_id: &event.logical_log_id,
                    block_hash: &event.block_hash,
                    cursor_order: &event.cursor_order,
                    journal_bytes: event.journal_reference_bytes(),
                    key_index,
                    fields: event
                        .fields
                        .iter()
                        .flat_map(|(name, value)| [(*name).to_owned(), value.clone()])
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?,
        removed_reference_count: reference_lists.iter().map(Vec::len).sum(),
        removed_reference_bytes: reference_bytes,
    };
    let payload = serde_json::to_vec(&plan)?;
    let retained = correction
        .retained_bytes()
        .saturating_add(source_bytes)
        .saturating_add(payload.len());
    if retained > limits.max_bytes {
        return Err(StoreError::CorrectionBudget(
            "correction byte limit exceeded".into(),
        ));
    }

    let mut invocation = script.prepare_invoke();
    add_base_keys(&mut invocation, keys);
    for block in &correction.old_blocks {
        invocation.key(keys.block_logs(&block.block_hash));
    }
    for head in &correction.new_heads {
        invocation.key(keys.block_logs(&head.block_hash));
    }
    invocation.arg(payload);
    let (stream_id, appended, reverted, applied) = invocation
        .invoke::<(String, i64, usize, usize)>(connection)
        .map_err(redis_error)?;
    Ok(CorrectionOutcome {
        stream_id,
        appended: appended == 1,
        reverted,
        applied,
    })
}

fn load_references(
    connection: &mut Connection,
    keys: &RedisKeys,
    correction: &ReorgCorrection,
    max_references: usize,
) -> Result<Vec<Vec<String>>, StoreError> {
    if correction.old_blocks.is_empty() {
        return Ok(Vec::new());
    }
    let mut size_pipeline = redis::pipe();
    for block in &correction.old_blocks {
        size_pipeline
            .cmd("LLEN")
            .arg(keys.block_logs(&block.block_hash));
    }
    let counts = size_pipeline
        .query::<Vec<usize>>(connection)
        .map_err(redis_error)?;
    if counts.len() != correction.old_blocks.len() {
        return Err(StoreError::Journal(
            "old branch reference count reply mismatch".into(),
        ));
    }
    if counts
        .iter()
        .fold(0_usize, |total, count| total.saturating_add(*count))
        > max_references
    {
        return Err(StoreError::CorrectionBudget(
            "correction event limit exceeded".into(),
        ));
    }
    let mut pipeline = redis::pipe();
    for block in &correction.old_blocks {
        pipeline
            .cmd("LRANGE")
            .arg(keys.block_logs(&block.block_hash))
            .arg(0)
            .arg(-1);
    }
    pipeline.query(connection).map_err(redis_error)
}

fn materialize_old_logs(
    correction: &ReorgCorrection,
    reference_lists: &[Vec<String>],
) -> Result<(Vec<OldLog>, usize), StoreError> {
    if reference_lists.len() != correction.old_blocks.len() {
        return Err(StoreError::Journal(
            "old branch reference reply count mismatch".into(),
        ));
    }
    let mut logs = Vec::new();
    let mut bytes = 0_usize;
    for (block, references) in correction.old_blocks.iter().zip(reference_lists).rev() {
        for reference in references.iter().rev() {
            let mut parts = reference.splitn(4, '|');
            let order = parts.next();
            let logical = parts.next();
            let source_record = parts.next();
            let source_stream = parts.next();
            let (Some(order), Some(logical), Some(source_record), Some(source_stream)) =
                (order, logical, source_record, source_stream)
            else {
                return Err(StoreError::Journal(
                    "malformed old branch log reference".into(),
                ));
            };
            bytes = bytes
                .saturating_add(order.len())
                .saturating_add(logical.len())
                .saturating_add(source_record.len())
                .saturating_add(32)
                .saturating_add(3);
            logs.push(OldLog {
                logical_log_id: logical.to_owned(),
                source_record_id: source_record.to_owned(),
                source_stream_id: source_stream.to_owned(),
                record_id: correction.lifecycle_record_id(logical, "reverted"),
                block_hash: block.block_hash.clone(),
            });
        }
    }
    Ok((logs, bytes))
}

fn load_source_bytes(
    connection: &mut Connection,
    keys: &RedisKeys,
    logs: &[OldLog],
) -> Result<usize, StoreError> {
    let mut bytes = 0_usize;
    for chunk in logs.chunks(64) {
        let mut pipeline = redis::pipe();
        for log in chunk {
            pipeline
                .cmd("XRANGE")
                .arg(&keys.stream)
                .arg(&log.source_stream_id)
                .arg(&log.source_stream_id);
        }
        let replies = pipeline
            .query::<Vec<StreamRangeReply>>(connection)
            .map_err(redis_error)?;
        if replies.len() != chunk.len() {
            return Err(StoreError::Journal(
                "old branch stream reply count mismatch".into(),
            ));
        }
        for (reply, expected) in replies.iter().zip(chunk) {
            let Some(entry) = reply.ids.as_slice().first() else {
                return Err(StoreError::Journal(
                    "old branch stream entry is missing".into(),
                ));
            };
            if entry.id != expected.source_stream_id || reply.ids.len() != 1 {
                return Err(StoreError::Journal(
                    "old branch stream entry identity mismatch".into(),
                ));
            }
            bytes = bytes.saturating_add(entry.id.len());
            for (name, value) in &entry.map {
                let value = redis::from_redis_value::<String>(value).map_err(redis_error)?;
                bytes = bytes.saturating_add(name.len()).saturating_add(value.len());
            }
        }
    }
    Ok(bytes)
}

fn add_base_keys<'a>(invocation: &mut redis::ScriptInvocation<'a>, keys: &RedisKeys) {
    invocation
        .key(&keys.stream)
        .key(&keys.cursor)
        .key(&keys.cursor_order)
        .key(&keys.record_ids)
        .key(&keys.log_state)
        .key(&keys.headers)
        .key(&keys.canonical_height)
        .key(&keys.canonical_head)
        .key(&keys.finalized_head)
        .key(&keys.reorg_manifest)
        .key(&keys.metadata)
        .key(&keys.journal_usage)
        .key(&keys.resume);
}

fn required_hash(block: &lunarbase_client::model::BlockRef) -> Result<String, StoreError> {
    block
        .cursor
        .block_hash
        .map(|hash| format!("{hash:#x}"))
        .ok_or_else(|| StoreError::Journal("correction block hash is absent".into()))
}
