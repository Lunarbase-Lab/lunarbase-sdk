# Durable event delivery v2

This document is the normative delivery contract for fork-aware LunarBase Core
events. The words MUST, MUST NOT, SHOULD, and MAY describe requirements for a
production implementation.

The event worker currently emits Redis schema v1. Schema v2 becomes active only
after the worker is configured to open the separate v2 namespace. A v1
namespace MUST NOT be opened or mutated as v2.

## Scope and isolation

The worker provides an append-only, at-least-once lifecycle feed. It owns its
RPC connections, source queues, fork journal, Redis connection, readiness, and
resource budgets. None of these objects may be shared with a quote indexer.
Redis commands, event encoding, fork resolution, and consumer backpressure MUST
remain absent from the quote request path.

The v2 feed describes changes to canonical membership:

- `applied` makes one immutable log active;
- `reverted` makes the same immutable log inactive;
- `begin` opens one resolved fork correction;
- `commit` closes that correction.
- `halt` terminates a stream whose continuity cannot be proved.

Provider `removed` notifications are discontinuity signals. They are not a
complete correction protocol and MUST NOT be treated as proof that every log
from an abandoned branch was delivered.

## Block and log identity

A fork-aware source MUST preserve:

```text
BlockRef {
  chainId,
  blockNumber,
  executionBlockNumber,
  blockHash,
  parentHash
}
```

Every production log MUST have a block hash, transaction hash, transaction
index, and log index. Missing stable coordinates are a continuity failure; the
worker MUST stop ingestion and become unready instead of inventing an identity.
Parent linkage is carried once by the block head and persisted in the header
journal. A realtime log may arrive before its head, so `parentHash` is optional
on a log record and MUST NOT be populated through an HTTP request per log.

All hexadecimal values use lowercase canonical `0x` encoding. Decimal values
have no leading zeroes, except the value zero itself.

### logicalLogId

`logicalLogId` identifies the immutable on-chain log and is identical in its
`applied` and `reverted` records. Its preimage is:

```text
"lunarbase-durable-log-v2"
|| chainId_be_u64
|| core_20_bytes
|| blockHash_32_bytes
|| transactionHash_32_bytes
|| transactionIndex_be_u32
|| logIndex_be_u32
```

The encoded value is `v2:0x` followed by the lowercase Keccak-256 digest.
Commitment, receive order, operation, and reorg identity MUST NOT enter this
preimage.

### reorgId

`reorgId` is stable across retries of one correction:

```text
"lunarbase-durable-reorg-v2"
|| chainId_be_u64
|| core_20_bytes
|| ancestorBlockHash
|| oldTipBlockHash
|| newTipBlockHash
```

### recordId

`recordId` identifies one lifecycle transition, not one immutable log:

```text
normal apply: Keccak("lunarbase-durable-record-v2" || logicalLogId || "origin")
fork log:    Keccak("lunarbase-durable-record-v2" || logicalLogId || reorgId || operation)
control:     Keccak("lunarbase-durable-record-v2" || reorgId || operation)
```

`reorgId` and `recordId` use the same `v2:0x<lowercase digest>` encoding
as `logicalLogId`.

The same branch may become active again after another fork. Its new
`applied` transition then has a new `reorgId` and a new `recordId`, while
its `logicalLogId` remains unchanged.

`lifecycleRevision` is a positive decimal counter assigned atomically by
Redis for each `logicalLogId`. A transition increments it once; an ambiguous
retry returns the already committed record without incrementing it.

## Redis Stream record

The machine-readable contract is
[`schemas/durable-events/v2.schema.json`](../schemas/durable-events/v2.schema.json).
Redis stores every property as a UTF-8 Stream field value. `topics` is a
compact JSON array of lowercase hashes. Empty optional transport positions are
omitted rather than encoded with sentinel values.

The v2 record intentionally has no `rawLog`, `topic0`, `eventName`,
`arguments`, or `decodeError` duplication. `topics` and `data` are the
authoritative raw payload; `topic0` is `topics[0]`. ABI presentation belongs
in a downstream enrichment consumer, outside ingestion.

Stream order is authoritative. A normal record has no `reorgId`. Every log
inside a correction carries the active `reorgId`.

## Redis keyspace

All keys for one deployment MUST share one Redis Cluster hash tag:

```text
<namespace>:event:v2:{<chainId>:<core>}:stream
<namespace>:event:v2:{<chainId>:<core>}:metadata
<namespace>:event:v2:{<chainId>:<core>}:cursor
<namespace>:event:v2:{<chainId>:<core>}:cursor-order
<namespace>:event:v2:{<chainId>:<core>}:resume
<namespace>:event:v2:{<chainId>:<core>}:record-ids
<namespace>:event:v2:{<chainId>:<core>}:log-state
<namespace>:event:v2:{<chainId>:<core>}:headers
<namespace>:event:v2:{<chainId>:<core>}:canonical-height
<namespace>:event:v2:{<chainId>:<core>}:canonical-head
<namespace>:event:v2:{<chainId>:<core>}:finalized-head
<namespace>:event:v2:{<chainId>:<core>}:reorg-manifest
<namespace>:event:v2:{<chainId>:<core>}:journal-usage
<namespace>:event:v2:{<chainId>:<core>}:block:<blockHash>:logs
```

`headers` stores compact `BlockRef` values. `canonical-height` maps a block
number to its active hash. Each block log list contains only event order,
`logicalLogId`, applied `recordId`, and Stream ID. It MUST NOT copy topics or
data. A rare fork reads abandoned payloads from the Stream by the referenced
IDs in bounded chunks. Stream retention therefore MUST keep every entry still
referenced by the non-finalized journal.

The metadata binds schema version, chain ID, Core address, delivery mode, and
ID domains. A mismatch fails startup before any write.

## Fast-path transaction

The common path is an immediately available batch of heads and applied logs.
It MUST use one preloaded Lua script through `EVALSHA` per batch. A
`NOSCRIPT` response reloads the script and retries the same batch.

One successful transaction:

1. verifies deployment metadata and that no reorg manifest is open;
2. ignores already committed `recordId` values;
3. appends new Stream records;
4. updates record deduplication and per-log lifecycle state;
5. stores header linkage, including blocks with zero Core logs;
6. appends lightweight block-to-Stream references;
7. advances the canonical head, recovery cursor, and optional source resume
   token together.

The writer drains records already present in its bounded queue up to configured
item and byte limits. It MUST NOT add a batching timer or wait to fill a batch.
The ingestion task performs no Redis read before a normal append, no full
Stream scan, and no work proportional to retained fork depth.

A source resume token or parser acknowledgement becomes externally visible
only after the Redis transaction that contains the corresponding records has
committed.

## Fork resolution and correction

The worker detects a fork from parent linkage. Provider retractions MAY trigger
resolution earlier, but never define the complete abandoned branch.

On discontinuity the worker:

1. revokes readiness and pauses normal persistence;
2. keeps newly received frames only within count and byte budgets;
3. walks headers by hash through bounded RPC requests until it finds a common
   ancestor present in the journal;
4. rejects an ancestor below the finalized watermark;
5. resolves and validates both branch log sets before opening a correction;
6. atomically creates a durable reorg manifest with phase and progress.

The public correction order is exactly:

```text
reorg/begin
log/reverted: abandoned blocks newest to oldest
log/reverted: logs within each block in reverse event order
log/applied:  replacement blocks oldest to newest
log/applied:  logs within each block in event order
reorg/commit
```

`reorg/begin` and `reorg/commit` contain the same ancestor, old tip, new tip,
finalized watermark, and expected applied/reverted counts. The commit counts
MUST equal the number of lifecycle records between the barriers.

Corrections are written in bounded atomic chunks. The manifest records phase,
next block, next event, counts, and last committed Stream ID in the same
transaction as every chunk. A process crash resumes after the last committed
position. Stable `recordId` values make replay of an ambiguous chunk
idempotent.

Readiness remains false from discontinuity through `reorg/commit`. The worker
updates the canonical-height index and source resume position before committing
the barrier, then closes the manifest atomically with `ReorgCommit`.

## Delivery modes

- `realtime` publishes applied logs immediately, journals their block, and
  later publishes exact corrections when necessary.
- `block-ordered` publishes a complete executed block in event order and uses
  the same fork correction contract.
- `finalized` publishes only finalized blocks. A fork at or below the
  finalized watermark is a fatal provider invariant violation; no correction
  is guessed.

Commitment selects delivery latency. It does not weaken identity, durability,
ordering, or fork detection.

## Consumer contract

Redis consumer groups provide at-least-once transport. Consumers MUST:

1. make side effects idempotent by `recordId`;
2. fold active membership by `logicalLogId`;
3. persist their side effect and processed `recordId` together;
4. acknowledge a Stream ID only after that transaction commits;
5. reclaim abandoned pending entries with `XAUTOCLAIM`.

A consumer that cannot expose partial fork state MUST stage all records from
`reorg/begin` through `reorg/commit` under `reorgId` and publish them as one
local transaction. It MUST verify the barrier identity and counts. A second
`begin`, a mismatched `commit`, or an ordinary record while a correction is
open is a protocol error.

Lifecycle consumers MAY apply each record in Stream order, but MUST surface
that a correction is in progress until the matching commit is processed.

### Terminal gap

When exact correction is impossible, Redis availability permitting, the worker
atomically appends one `gap/halt` record and marks the namespace terminal. It
does not advance the recovery cursor or source resume token. `gapReason` is a
bounded machine code; optional `gapDetails` is diagnostic only.

The worker never writes a terminal gap for a temporary dependency failure that
can still be retried without losing continuity. A consumer commits the terminal
status idempotently by `recordId`, acknowledges the record, and rejects later
entries in that namespace. Recovery requires an operator-chosen canonical
bootstrap boundary and a new namespace.

## Bounds, retention, and failure policy

The source queue, Redis queue, pending frames, header journal, block-log
references, correction plan, and RPC response bodies MUST each have item and
byte limits. Limits charge shared buffers to every retained owner.

Journal entries may be pruned only when all conditions hold:

- the block is at or below the finalized watermark;
- no active reorg manifest references it;
- no retained Stream entry references data that would be removed;
- the configured archive and required consumer low-watermarks passed it.

Stream entries, `record-ids`, and finalized block references are trimmed in
the same bounded maintenance transaction. Trimming MUST use a safe Stream ID
watermark and MUST NOT use approximate length-based eviction.

Redis MUST use AOF with `appendfsync always` and
`maxmemory-policy noeviction`. Capacity alerts fire before the configured
journal or Redis memory high-watermark. If finality stalls, a consumer blocks
retention, a common ancestor is outside the retained window, or any byte budget
is exhausted, the worker stops ingestion and remains unhealthy. It never
silently drops an event, deletes an unfinalized branch, or substitutes a normal
backfill for an unknown correction.

## Schema transition

Schema v2 uses a new key prefix, consumer group, and bootstrap boundary. It
does not mutate v1 keys and consumers MUST NOT merge both streams.

For cutover, stop the v1 writer, choose a verified finalized block, complete or
archive v1 processing through that boundary, bootstrap downstream state at the
same boundary, and start the v2 worker from the next block. Canonical backfill
then covers downtime. A v1 `eventId` is not a v2 `recordId` or
`logicalLogId`.

Until the v2 runtime and its crash/reorg tests are enabled, fork-sensitive
production consumers SHOULD use finalized delivery.
