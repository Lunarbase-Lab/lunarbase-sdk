//! Redis initialization and preloaded atomic journal commands.

use super::{AppendOutcome, RedisKeys, StoreError, redis_error};
use crate::event::{DurableEvent, DurableHead, ID_DOMAIN_VERSION, STREAM_SCHEMA_VERSION};
use redis::{Connection, Script};

const INITIALIZE_METADATA_LUA: &str = r#"
local fingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
if fingerprint then
  if fingerprint ~= ARGV[1]
      or redis.call('HGET', KEYS[1], 'schemaVersion') ~= ARGV[2]
      or redis.call('HGET', KEYS[1], 'chainId') ~= ARGV[3]
      or redis.call('HGET', KEYS[1], 'core') ~= ARGV[4]
      or redis.call('HGET', KEYS[1], 'deliveryMode') ~= ARGV[5]
      or redis.call('HGET', KEYS[1], 'idDomain') ~= ARGV[6] then
    return redis.error_reply('LUNARBASE_METADATA_MISMATCH')
  end
  return 0
end
redis.call(
  'HSET', KEYS[1],
  'fingerprint', ARGV[1],
  'schemaVersion', ARGV[2],
  'chainId', ARGV[3],
  'core', ARGV[4],
  'deliveryMode', ARGV[5],
  'idDomain', ARGV[6]
)
return 1
"#;

pub(super) const JOURNAL_LUA: &str = r#"
local fingerprint = redis.call('HGET', KEYS[11], 'fingerprint')
if fingerprint ~= ARGV[1] then
  return redis.error_reply('LUNARBASE_METADATA_MISMATCH')
end
if redis.call('EXISTS', KEYS[10]) ~= 0 then
  return redis.error_reply('LUNARBASE_REORG_IN_PROGRESS')
end

local function advance_cursor(cursor_json, cursor_order)
  local current_order = redis.call('GET', KEYS[3])
  if (not current_order) or cursor_order > current_order then
    redis.call('SET', KEYS[2], cursor_json)
    redis.call('SET', KEYS[3], cursor_order)
  end
end

if ARGV[2] == 'head' then
  local header_json = ARGV[3]
  local block_hash = ARGV[4]
  local parent_hash = ARGV[5]
  local commitment = ARGV[6]
  local block_number = ARGV[10]
  local existing_header = redis.call('HGET', KEYS[6], block_hash)
  if existing_header and existing_header ~= header_json then
    return redis.error_reply('LUNARBASE_HEADER_IDENTITY_MISMATCH')
  end
  local mapped_hash = redis.call('HGET', KEYS[7], block_number)
  if mapped_hash and mapped_hash ~= block_hash then
    return redis.error_reply('LUNARBASE_FORK_REQUIRES_CORRECTION')
  end
  if tonumber(block_number) > 0 then
    local mapped_parent = redis.call('HGET', KEYS[7], tostring(tonumber(block_number) - 1))
    if mapped_parent and mapped_parent ~= parent_hash then
      return redis.error_reply('LUNARBASE_PARENT_LINK_MISMATCH')
    end
  end

  local canonical_head = redis.call('GET', KEYS[8])
  local promote_head = false
  if not canonical_head then
    promote_head = true
  elseif canonical_head == block_hash then
    promote_head = true
  else
    local canonical_json = redis.call('HGET', KEYS[6], canonical_head)
    if not canonical_json then
      return redis.error_reply('LUNARBASE_CANONICAL_HEAD_MISSING')
    end
    local canonical_number = tonumber(cjson.decode(canonical_json)['blockNumber'])
    local next_number = tonumber(block_number)
    if next_number > canonical_number then
      if commitment ~= 'finalized'
          and (next_number ~= canonical_number + 1 or parent_hash ~= canonical_head) then
        return redis.error_reply('LUNARBASE_HEAD_DISCONTINUITY')
      end
      promote_head = true
    elseif next_number == canonical_number then
      return redis.error_reply('LUNARBASE_FORK_REQUIRES_CORRECTION')
    end
  end

  if redis.call('HSETNX', KEYS[6], block_hash, header_json) == 1 then
    redis.call('HINCRBY', KEYS[12], 'headers', 1)
    redis.call('HINCRBY', KEYS[12], 'headerBytes', ARGV[9])
  end
  redis.call('HSET', KEYS[7], block_number, block_hash)
  if promote_head then
    redis.call('SET', KEYS[8], block_hash)
  end
  if commitment == 'finalized' then
    local finalized_head = redis.call('GET', KEYS[9])
    local promote_finalized = not finalized_head
    if finalized_head and finalized_head ~= block_hash then
      local finalized_json = redis.call('HGET', KEYS[6], finalized_head)
      if not finalized_json then
        return redis.error_reply('LUNARBASE_FINALIZED_HEAD_MISSING')
      end
      promote_finalized =
        tonumber(block_number) > tonumber(cjson.decode(finalized_json)['blockNumber'])
    elseif finalized_head == block_hash then
      promote_finalized = true
    end
    if promote_finalized then
      redis.call('SET', KEYS[9], block_hash)
    end
  end
  return {'', existing_header and 0 or 1}
end

if ARGV[2] ~= 'log' then
  return redis.error_reply('LUNARBASE_UNKNOWN_JOURNAL_COMMAND')
end
local record_id = ARGV[3]
local logical_log_id = ARGV[4]
local block_hash = ARGV[5]
local block_number = ARGV[9]
local existing_stream_id = redis.call('HGET', KEYS[4], record_id)
if existing_stream_id then
  return {existing_stream_id, 0}
end
local mapped_hash = redis.call('HGET', KEYS[7], block_number)
if mapped_hash and mapped_hash ~= block_hash then
  return redis.error_reply('LUNARBASE_LOG_ON_NONCANONICAL_BLOCK')
end
local state = redis.call('HGET', KEYS[5], logical_log_id)
local revision = 0
if state then
  local active, stored_revision = string.match(state, '^([^|]+)|([^|]+)|')
  if active == '1' then
    return redis.error_reply('LUNARBASE_LOG_ALREADY_ACTIVE')
  end
  revision = tonumber(stored_revision)
end
revision = revision + 1
local stream_id = redis.call(
  'XADD', KEYS[1], '*',
  'schemaVersion', '2',
  'recordType', 'log',
  'operation', 'applied',
  'recordId', record_id,
  'logicalLogId', logical_log_id,
  'blockHash', block_hash,
  'lifecycleRevision', tostring(revision),
  unpack(ARGV, 10)
)
redis.call('HSET', KEYS[4], record_id, stream_id)
redis.call(
  'HSET', KEYS[5], logical_log_id,
  '1|' .. tostring(revision) .. '|' .. record_id .. '|' .. stream_id .. '|' .. block_hash
)
redis.call(
  'RPUSH', KEYS[13],
  ARGV[7] .. '|' .. logical_log_id .. '|' .. record_id .. '|' .. stream_id
)
redis.call('HINCRBY', KEYS[12], 'logReferences', 1)
redis.call('HINCRBY', KEYS[12], 'logReferenceBytes', ARGV[8])
advance_cursor(ARGV[6], ARGV[7])
return {stream_id, 1}
"#;

#[derive(Clone, Debug)]
pub(super) struct DeploymentMetadata {
    fingerprint: String,
    schema_version: String,
    chain_id: String,
    core: String,
    delivery_mode: &'static str,
}

impl DeploymentMetadata {
    pub(super) fn new(
        chain_id: u64,
        core: alloy_primitives::Address,
        delivery_mode: &'static str,
    ) -> Self {
        let core = format!("{core:#x}");
        Self {
            fingerprint: format!(
                "v{STREAM_SCHEMA_VERSION}|{chain_id}|{core}|{delivery_mode}|{ID_DOMAIN_VERSION}"
            ),
            schema_version: STREAM_SCHEMA_VERSION.to_string(),
            chain_id: chain_id.to_string(),
            core,
            delivery_mode,
        }
    }

    pub(super) fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

pub(super) fn script() -> Script {
    Script::new(JOURNAL_LUA)
}

pub(super) fn initialize(
    connection: &mut Connection,
    keys: &RedisKeys,
    group: &str,
    metadata: &DeploymentMetadata,
    script: &Script,
    correction_script: &Script,
) -> Result<(), StoreError> {
    redis::cmd("PING")
        .query::<String>(connection)
        .map_err(redis_error)?;
    verify_durability(connection)?;
    let metadata_script = Script::new(INITIALIZE_METADATA_LUA);
    let mut invocation = metadata_script.prepare_invoke();
    invocation
        .key(&keys.metadata)
        .arg(&metadata.fingerprint)
        .arg(&metadata.schema_version)
        .arg(&metadata.chain_id)
        .arg(&metadata.core)
        .arg(metadata.delivery_mode)
        .arg(ID_DOMAIN_VERSION);
    invocation.invoke::<i64>(connection).map_err(redis_error)?;
    match redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&keys.stream)
        .arg(group)
        .arg("0-0")
        .arg("MKSTREAM")
        .query::<String>(connection)
    {
        Ok(_) => {}
        Err(error) if error.to_string().contains("BUSYGROUP") => {}
        Err(error) => return Err(redis_error(error)),
    }
    script
        .prepare_invoke()
        .load(connection)
        .map_err(redis_error)?;
    correction_script
        .prepare_invoke()
        .load(connection)
        .map_err(redis_error)?;
    Ok(())
}

pub(super) fn append_event(
    connection: &mut Connection,
    keys: &RedisKeys,
    metadata: &DeploymentMetadata,
    script: &Script,
    event: &DurableEvent,
) -> Result<AppendOutcome, StoreError> {
    let mut invocation = invocation(script, keys, &event.block_hash);
    invocation
        .arg(&metadata.fingerprint)
        .arg("log")
        .arg(&event.record_id)
        .arg(&event.logical_log_id)
        .arg(&event.block_hash)
        .arg(&event.cursor_json)
        .arg(&event.cursor_order)
        .arg(event.journal_reference_bytes())
        .arg(&event.cursor_order[..20]);
    for (name, value) in &event.fields {
        invocation.arg(name).arg(value);
    }
    let (stream_id, appended) = invocation
        .invoke::<(String, i64)>(connection)
        .map_err(redis_error)?;
    Ok(AppendOutcome {
        stream_id,
        appended: appended == 1,
    })
}

pub(super) fn append_head(
    connection: &mut Connection,
    keys: &RedisKeys,
    metadata: &DeploymentMetadata,
    script: &Script,
    head: &DurableHead,
) -> Result<AppendOutcome, StoreError> {
    let mut invocation = invocation(script, keys, &head.block_hash);
    invocation
        .arg(&metadata.fingerprint)
        .arg("head")
        .arg(&head.header_json)
        .arg(&head.block_hash)
        .arg(&head.parent_hash)
        .arg(head.commitment)
        .arg(&head.cursor_json)
        .arg(&head.cursor_order)
        .arg(head.header_json.len())
        .arg(&head.block_number);
    let (stream_id, appended) = invocation
        .invoke::<(String, i64)>(connection)
        .map_err(redis_error)?;
    Ok(AppendOutcome {
        stream_id,
        appended: appended == 1,
    })
}

fn invocation<'a>(
    script: &'a Script,
    keys: &RedisKeys,
    block_hash: &str,
) -> redis::ScriptInvocation<'a> {
    let mut invocation = script.prepare_invoke();
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
        .key(keys.block_logs(block_hash))
        .key(&keys.resume);
    invocation
}

fn verify_durability(connection: &mut Connection) -> Result<(), StoreError> {
    let appendonly = config_value(connection, "appendonly")?;
    let appendfsync = config_value(connection, "appendfsync")?;
    let eviction = config_value(connection, "maxmemory-policy")?;
    if appendonly != "yes" || appendfsync != "always" || eviction != "noeviction" {
        return Err(StoreError::Durability(
            "require appendonly=yes, appendfsync=always, and maxmemory-policy=noeviction".into(),
        ));
    }
    Ok(())
}

fn config_value(connection: &mut Connection, name: &str) -> Result<String, StoreError> {
    let values = redis::cmd("CONFIG")
        .arg("GET")
        .arg(name)
        .query::<Vec<String>>(connection)
        .map_err(redis_error)?;
    values
        .get(1)
        .map(|value| value.to_ascii_lowercase())
        .ok_or_else(|| StoreError::Durability(format!("Redis CONFIG GET {name} returned no value")))
}
