# LunarBase durable event worker

`lunarbase-event-worker` is a standalone service that persists raw LunarBase
Core logs to a Redis Stream. It owns separate HTTP and realtime source
connections, queues, health state, and metrics; no worker code runs in the
quote request path.

The worker provides at-least-once delivery. It never drops an accepted event
when Redis or a downstream consumer is slow: ingestion is backpressured and
`/readyz` returns 503 until continuity is restored.

## Durability contract

Startup is fail-closed unless Redis reports both:

```text
appendonly yes
appendfsync always
```

Each Lua invocation atomically:

1. checks the stable event ID;
2. appends a new Stream record when the ID has not been seen;
3. records the ID for retry deduplication; and
4. advances the deployment-bound recovery cursor monotonically.

An ambiguous connection failure retries the same stable ID. Redis therefore
cannot expose an event without its dedup entry and cursor update. Applied and
removed forms have different IDs and remain independently observable.

Use a dedicated Redis resource with `maxmemory-policy noeviction`, persistent
storage, capacity alerts, authentication, network isolation, and backups. The
worker intentionally does not apply lossy `MAXLEN` trimming. Archive events
and trim only behind the oldest required consumer-group position.

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

Provider retractions are persisted as `operation=removed` before the worker
enters canonical recovery, so consumers can observe both lifecycle edges.

## Redis schema v1

All keys share one Redis Cluster hash tag:

```text
<namespace>:event:v1:{<chainId>:<core>}:stream
<namespace>:event:v1:{<chainId>:<core>}:cursor
<namespace>:event:v1:{<chainId>:<core>}:cursor-order
<namespace>:event:v1:{<chainId>:<core>}:event-ids
<namespace>:event:v1:{<chainId>:<core>}:metadata
```

Every Stream entry contains:

- schema and identity: `schemaVersion`, `eventId`, `chainId`, `core`;
- lifecycle: `operation`, `commitment`, `removed`, `eventName`;
- position: block/execution block, hashes, transaction/log indices, source
  sequence and sub-index;
- payload: `topic0`, `topics`, `data`, `rawLog`;
- best-effort ABI description: `arguments` and `decodeError`.

The raw log is authoritative. ABI formatting errors are stored, not used to
discard the event.

## Consumer groups

The configured group is created from `0-0` with `MKSTREAM`. Consumers use the
normal Redis at-least-once flow:

```text
XREADGROUP GROUP lunarbase-processors consumer-1 BLOCK 5000 COUNT 100 \
  STREAMS <stream-key> >
XACK <stream-key> lunarbase-processors <stream-id>
```

After a consumer crash, reclaim its pending entries with `XAUTOCLAIM`. A
consumer must make its side effect idempotent by `eventId`, then acknowledge
only after that side effect commits.
