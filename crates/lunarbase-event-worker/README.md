# LunarBase durable event worker

`lunarbase-event-worker` is a standalone service that persists raw LunarBase
Core logs to a Redis Stream. It owns separate HTTP and realtime source
connections, queues, health state, and metrics; no worker code runs in the
quote request path.

The worker provides at-least-once delivery. It never drops an accepted event
when Redis or a downstream consumer is slow: ingestion is backpressured and
`/readyz` returns 503 until continuity is restored.

## Durability contract

Startup is fail-closed unless Redis reports all three settings:

```text
appendonly yes
appendfsync always
maxmemory-policy noeviction
```

The current wire format and Redis key layout are schema v2; deterministic IDs
use domain v3. Deployment metadata rejects older ID domains. Upgrade with a
fresh namespace after draining/migrating the old one, and never run mixed
v2/v3 ID writers against one namespace. Each command uses a preloaded Lua
script through `EVALSHA`; `NOSCRIPT` transparently reloads the same script.
A normal
log append atomically validates deployment metadata and canonical membership,
deduplicates `recordId`, appends the Stream record, updates
`logicalLogId` lifecycle state, writes a lightweight block-journal reference,
and advances the recovery cursor. A block-head command atomically persists
parent linkage and canonical/finalized indexes. A head alone never advances
the recovery cursor, so a crash between a head and its logs cannot skip the
block. No Redis read is required before a normal append.

An ambiguous connection failure retries the same `recordId`. Redis therefore
cannot expose a log without its deduplication, lifecycle, block reference, and
cursor updates. Provider `removed` notifications are never persisted as a
complete correction: they trigger bounded exact-hash resolution. The worker
stays live but unready while one atomic `begin -> reverted -> applied -> commit`
correction is retried; normal persistence remains backpressured.

Use a dedicated Redis resource with persistent storage, capacity alerts,
authentication, network isolation, and backups. The worker intentionally does
not apply lossy `MAXLEN` trimming. Automated safe archival/pruning is not yet
implemented: the Stream and its record, lifecycle, header, height, and block
reference indexes grow monotonically. Queue and in-process fork-window limits
do not bound total Redis memory. Do not trim one key independently; provision
and alert Redis for the full retention horizon until the watermark-safe
maintenance worker is available.

## Run

Build one network profile and supply a deployment identity:

```sh
cargo run -p lunarbase-event-worker --no-default-features --features base -- \
  --network base \
  --chain-id 8453 \
  --core 0x1111111111111111111111111111111111111111 \
  --deployment-block 12345678 \
  --http-rpc-url https://rpc.example \
  --realtime-url wss://realtime.example \
  --redis-url redis://redis.internal:6379 \
  --minimum-commitment realtime
```

All flags have matching `LUNARBASE_EVENT_*` environment variables. See
[`examples/indexer/event-worker.env.example`](../../examples/indexer/event-worker.env.example).

Operational endpoints are independent of the quote indexer:

- `GET /livez` — process liveness;
- `GET /readyz` — source/recovery/Redis continuity;
- `GET /metrics` — Prometheus counters and gauges.

For EVM, Base, and Arbitrum, `realtime`, `block-ordered`, and `finalized`
select the source delivery mode rather than filtering one realtime stream.
Realtime preserves provider receive order, block-ordered closes and sorts an
executed block before publishing it, and finalized follows the HTTP finalized
watermark with bounded backfill pages. `LUNARBASE_EVENT_BACKFILL_PAGE_BLOCKS`
bounds both recovery and finalized catch-up requests (default `1000`).

Source and Redis handoffs are bounded by both item count and retained bytes.
The default byte budgets are 64 MiB and 16 MiB respectively; configure them
with `LUNARBASE_EVENT_SOURCE_QUEUE_BYTE_BOUND` and
`LUNARBASE_EVENT_REDIS_QUEUE_BYTE_BOUND`. Saturation backpressures ingestion
and revokes readiness. It never silently removes a required event.

The in-process fork-resolution window defaults to 4096 headers / 2 MiB and
exact resolution is capped at 4096 blocks. This is not a durable Redis
retention bound. One correction admits at most 8192 total lifecycle records,
including `reorg-begin`, `reorg-commit`, reverted logs, and replacement logs,
and at most 16 MiB. A protocol-valid larger delta becomes an observable
recovery gap; the worker remains alive rather than inflating one blocking Lua
transaction. Override these with `LUNARBASE_EVENT_FORK_WINDOW_BLOCKS`,
`LUNARBASE_EVENT_FORK_WINDOW_BYTES`, `LUNARBASE_EVENT_FORK_MAX_DEPTH`,
`LUNARBASE_EVENT_CORRECTION_EVENT_BOUND`, and
`LUNARBASE_EVENT_CORRECTION_BYTE_BOUND`.

The complete fork-aware contract is specified in
[`docs/EVENT_DELIVERY.md`](../../docs/EVENT_DELIVERY.md).

## Redis schema v2

All keys share one Redis Cluster hash tag. The deployment has a Stream,
metadata, cursor/resume state, record and lifecycle indexes, header and
canonical-height journals, canonical/finalized heads, reorg state, usage
accounting, and one lightweight log-reference list per observed block. Exact
key names and the target retention invariants are documented in
[`docs/EVENT_DELIVERY.md`](../../docs/EVENT_DELIVERY.md).

Every normal Stream entry contains:

- schema and identity: `schemaVersion`, `recordType`, `recordId`,
  `logicalLogId`, `chainId`, and `core`;
- lifecycle: `operation`, `lifecycleRevision`, and `commitment`;
- stable EVM position: block/execution block, block/transaction hashes,
  transaction/log indices, and optional source sequence/sub-index;
- authoritative payload: `topics` and `data`.

Schema v2 intentionally does not duplicate `rawLog`, `topic0`, decoded
arguments, or ABI errors on the ingestion hot path. ABI enrichment belongs in
an independent downstream consumer.

## Consumer groups

The configured group is created from `0-0` with `MKSTREAM`. Consumers use
the normal Redis at-least-once flow:

```text
XREADGROUP GROUP lunarbase-processors consumer-1 BLOCK 5000 COUNT 100 \
  STREAMS <stream-key> >
XACK <stream-key> lunarbase-processors <stream-id>
```

After a consumer crash, reclaim its pending entries with `XAUTOCLAIM`. A
consumer makes side effects idempotent by `recordId`, folds active membership
by `logicalLogId`, and acknowledges only after its own transaction commits.
