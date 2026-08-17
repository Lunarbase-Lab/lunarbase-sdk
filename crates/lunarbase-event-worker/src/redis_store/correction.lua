local plan = cjson.decode(ARGV[1])

local function fail(message)
  return redis.error_reply(message)
end

local function field(fields, name)
  for index = 1, #fields, 2 do
    if fields[index] == name then
      return fields[index + 1]
    end
  end
  return nil
end

local function xadd(record_type, operation, record_id, fields)
  local command = {
    'XADD', KEYS[1], '*',
    'schemaVersion', '2',
    'recordType', record_type,
    'operation', operation,
    'recordId', record_id
  }
  for index = 1, #fields do
    command[#command + 1] = fields[index]
  end
  return redis.call(unpack(command))
end

if redis.call('HGET', KEYS[11], 'fingerprint') ~= plan.fingerprint then
  return fail('LUNARBASE_METADATA_MISMATCH')
end
local committed = redis.call('HGET', KEYS[4], plan.commitRecordId)
if committed then
  return {committed, 0, #plan.oldLogs, #plan.newEvents}
end
if redis.call('EXISTS', KEYS[10]) ~= 0 then
  return fail('LUNARBASE_REORG_IN_PROGRESS')
end
if redis.call('GET', KEYS[8]) ~= plan.oldTipHash then
  return fail('LUNARBASE_REORG_STALE_HEAD')
end
if redis.call('GET', KEYS[9]) ~= plan.finalizedHash then
  return fail('LUNARBASE_REORG_FINALIZED_MISMATCH')
end
if redis.call('HGET', KEYS[4], plan.beginRecordId) then
  return fail('LUNARBASE_PARTIAL_REORG_STATE')
end

for _, block in ipairs(plan.oldBlocks) do
  if redis.call('HGET', KEYS[7], block.blockNumber) ~= block.blockHash then
    return fail('LUNARBASE_REORG_OLD_BRANCH_MISMATCH')
  end
end

for _, item in ipairs(plan.oldLogs) do
  if redis.call('HGET', KEYS[4], item.recordId) then
    return fail('LUNARBASE_REORG_RECORD_EXISTS')
  end
  local source = redis.call('XRANGE', KEYS[1], item.sourceStreamId, item.sourceStreamId)
  if #source ~= 1 then
    return fail('LUNARBASE_REORG_SOURCE_EVENT_MISSING')
  end
  local source_fields = source[1][2]
  if field(source_fields, 'recordId') ~= item.sourceRecordId
      or field(source_fields, 'logicalLogId') ~= item.logicalLogId
      or field(source_fields, 'blockHash') ~= item.blockHash
      or field(source_fields, 'operation') ~= 'applied' then
    return fail('LUNARBASE_REORG_SOURCE_EVENT_MISMATCH')
  end
  local state = redis.call('HGET', KEYS[5], item.logicalLogId)
  local active, _, record_id, stream_id, block_hash =
    string.match(state or '', '^([^|]+)|([^|]+)|([^|]+)|([^|]+)|([^|]+)$')
  if active ~= '1'
      or record_id ~= item.sourceRecordId
      or stream_id ~= item.sourceStreamId
      or block_hash ~= item.blockHash then
    return fail('LUNARBASE_REORG_LOG_STATE_MISMATCH')
  end
end

for _, head in ipairs(plan.newHeads) do
  local existing = redis.call('HGET', KEYS[6], head.blockHash)
  if existing and existing ~= head.headerJson then
    return fail('LUNARBASE_HEADER_IDENTITY_MISMATCH')
  end
end

for _, item in ipairs(plan.newEvents) do
  if redis.call('HGET', KEYS[4], item.recordId) then
    return fail('LUNARBASE_REORG_RECORD_EXISTS')
  end
  local state = redis.call('HGET', KEYS[5], item.logicalLogId)
  if state and string.sub(state, 1, 2) == '1|' then
    return fail('LUNARBASE_LOG_ALREADY_ACTIVE')
  end
end

redis.call('SET', KEYS[10], ARGV[1])
local begin_id = xadd('reorg', 'begin', plan.beginRecordId, plan.controlFields)
redis.call('HSET', KEYS[4], plan.beginRecordId, begin_id)

for _, item in ipairs(plan.oldLogs) do
  local source = redis.call('XRANGE', KEYS[1], item.sourceStreamId, item.sourceStreamId)
  local source_fields = source[1][2]
  local state = redis.call('HGET', KEYS[5], item.logicalLogId)
  local _, stored_revision = string.match(state, '^([^|]+)|([^|]+)|')
  local revision = tonumber(stored_revision) + 1
  local fields = {
    'logicalLogId', item.logicalLogId,
    'blockHash', item.blockHash,
    'lifecycleRevision', tostring(revision),
    'reorgId', plan.reorgId
  }
  for index = 1, #source_fields, 2 do
    local name = source_fields[index]
    if name ~= 'schemaVersion'
        and name ~= 'recordType'
        and name ~= 'operation'
        and name ~= 'recordId'
        and name ~= 'logicalLogId'
        and name ~= 'blockHash'
        and name ~= 'lifecycleRevision'
        and name ~= 'reorgId' then
      fields[#fields + 1] = name
      fields[#fields + 1] = source_fields[index + 1]
    end
  end
  local stream_id = xadd('log', 'reverted', item.recordId, fields)
  redis.call('HSET', KEYS[4], item.recordId, stream_id)
  redis.call(
    'HSET', KEYS[5], item.logicalLogId,
    '0|' .. tostring(revision) .. '|' .. item.recordId .. '|' ..
      stream_id .. '|' .. item.blockHash
  )
end

for _, block in ipairs(plan.oldBlocks) do
  redis.call('HDEL', KEYS[7], block.blockNumber)
  redis.call('DEL', KEYS[block.keyIndex])
end
if plan.removedReferenceCount > 0 then
  redis.call('HINCRBY', KEYS[12], 'logReferences', -plan.removedReferenceCount)
  redis.call('HINCRBY', KEYS[12], 'logReferenceBytes', -plan.removedReferenceBytes)
end

for _, head in ipairs(plan.newHeads) do
  if redis.call('HSETNX', KEYS[6], head.blockHash, head.headerJson) == 1 then
    redis.call('HINCRBY', KEYS[12], 'headers', 1)
    redis.call('HINCRBY', KEYS[12], 'headerBytes', head.headerBytes)
  end
  redis.call('HSET', KEYS[7], head.blockNumber, head.blockHash)
end

for _, item in ipairs(plan.newEvents) do
  local state = redis.call('HGET', KEYS[5], item.logicalLogId)
  local revision = 1
  if state then
    local _, stored_revision = string.match(state, '^([^|]+)|([^|]+)|')
    revision = tonumber(stored_revision) + 1
  end
  local fields = {
    'logicalLogId', item.logicalLogId,
    'blockHash', item.blockHash,
    'lifecycleRevision', tostring(revision),
    'reorgId', plan.reorgId
  }
  for index = 1, #item.fields do
    fields[#fields + 1] = item.fields[index]
  end
  local stream_id = xadd('log', 'applied', item.recordId, fields)
  redis.call('HSET', KEYS[4], item.recordId, stream_id)
  redis.call(
    'HSET', KEYS[5], item.logicalLogId,
    '1|' .. tostring(revision) .. '|' .. item.recordId .. '|' ..
      stream_id .. '|' .. item.blockHash
  )
  redis.call(
    'RPUSH', KEYS[item.keyIndex],
    item.cursorOrder .. '|' .. item.logicalLogId .. '|' ..
      item.recordId .. '|' .. stream_id
  )
  redis.call('HINCRBY', KEYS[12], 'logReferences', 1)
  redis.call('HINCRBY', KEYS[12], 'logReferenceBytes', item.journalBytes)
end

redis.call('SET', KEYS[8], plan.newTipHash)
redis.call('SET', KEYS[2], plan.cursorJson)
redis.call('SET', KEYS[3], plan.cursorOrder)
local commit_id = xadd('reorg', 'commit', plan.commitRecordId, plan.controlFields)
redis.call('HSET', KEYS[4], plan.commitRecordId, commit_id)
redis.call('DEL', KEYS[10])
return {commit_id, 1, #plan.oldLogs, #plan.newEvents}
