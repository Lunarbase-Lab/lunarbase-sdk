# LunarBase indexer production runbook

## Deployment

Run at least two independently indexing replicas for each chain, Core, and fee
class. Route traffic only to replicas whose /readyz endpoint returns 200.

Supply deployment identity, implementation address and code hash, RPC
endpoints, and source settings through command-line arguments, LUNARBASE_*
environment variables, or an operator-owned TOML file. Checked-in files under
examples/indexer are templates only.

Redis is optional restart acceleration. When enabled, keep it on an
access-controlled network, require authentication, encrypt transport where
available, and include it in backup and credential-rotation policy.

Durable event delivery uses the separate `lunarbase-event-worker`. Give it
independent HTTP/WebSocket connections, queue limits, and preferably a
dedicated Redis resource so event I/O cannot contend with quote replicas.
Unlike optional quote checkpoints, event Redis is required and startup fails
unless `appendonly=yes` and `appendfsync=always`. Use persistent storage,
`maxmemory-policy=noeviction`, capacity alerts, and tested restore procedures.
The durable stream and fork journal contract is defined in
[durable event delivery](EVENT_DELIVERY.md).

## Start

Before rollout:

1. Verify chain ID, Core, fee class, deployment block, implementation address,
   runtime-code hash, and any optional verified router.
2. Verify RPC archive range and realtime subscription limits.
3. Size queues and timeouts for the provider and expected event rate.
4. Start replicas independently and wait for readiness.
5. If event delivery is enabled, verify worker Redis durability with
   `CONFIG GET appendonly appendfsync` and wait for the worker `/readyz`.

Local production-shaped topology:

```sh
cp examples/indexer/.env.example examples/indexer/.env
```

Fill the required deployment values in `examples/indexer/.env`, then run:

```sh
docker compose --env-file examples/indexer/.env -f examples/indexer/docker-compose.yml config
docker compose --env-file examples/indexer/.env -f examples/indexer/docker-compose.yml up --build -d
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
curl -fsS http://127.0.0.1:8080/metrics
curl -fsS http://127.0.0.1:9091/livez
curl -fsS http://127.0.0.1:9091/readyz
curl -fsS http://127.0.0.1:9091/metrics
```

## Monitoring

- /healthz is a process liveness signal.
- /readyz is the traffic-routing signal. It fails closed when reducer-published
  state exceeds `source_stall_timeout_milliseconds`, even if the process and
  transport are still alive.
- /metrics exposes Prometheus metrics.

Alert on sustained non-readiness, repeated recovery, queue saturation, source
disconnects, quote errors, and checkpoint failures. A checkpoint failure
affects restart speed; it does not invalidate a running ready replica.

For the event worker, also alert on Redis write failures, source gaps,
duplicate retries, consumer-group pending growth, Redis memory/disk headroom,
and lag between source head and last persisted block. Worker non-readiness
does not stop quote serving because the two processes do not share resources.

## Recovery

A gap, reorganization, disconnect, queue overflow, or identity mismatch
revokes readiness. If a replica does not recover:

1. Remove it from traffic.
2. Verify RPC availability and canonical block/log access.
3. Recheck deployment identity and runtime-code hash.
4. Inspect source, queue, recovery, and checkpoint metrics.
5. Restart the replica after the dependency is healthy.

An invalid checkpoint is ignored automatically in favor of a canonical
snapshot. Delete checkpoint data only after preserving it for diagnosis.

The event worker resumes from its deployment-bound Redis cursor and backfills
the cursor block inclusively. Stable event IDs make an ambiguous write or
inclusive replay idempotent inside the worker. Downstream consumers must also
deduplicate by `eventId`, reclaim abandoned pending entries with `XAUTOCLAIM`,
and `XACK` only after their own side effect commits.

For fork-aware delivery, consumers switch to the v2 contract in
[EVENT_DELIVERY.md](EVENT_DELIVERY.md). A reorg is complete only after the
matching `recordType=reorg, operation=commit` entry. A terminal gap means live
materialization for that deployment must stop until the consumer is rebuilt
from a chosen canonical block.

## Graceful shutdown

Send SIGTERM and set the orchestrator grace period above
`shutdown_timeout_seconds`. The service immediately revokes quote admission,
stops the periodic checkpoint writer, closes HTTP gracefully, stops ingestion,
drains updates already accepted by the reducer, and only then writes the final
checkpoint. Redis rejects checkpoint cursor regressions atomically.

`max_in_flight_quotes` is a fail-fast concurrency bound: requests above it get
HTTP 429 instead of waiting in an unbounded service queue. Every source
handshake, snapshot, and recovery RPC is bounded by
`source_operation_timeout_milliseconds`.

## Capacity validation

Run the load tool against staging with deployment-specific vectors:

```sh
cargo run -p lunarbase-tools --bin lunarbase-load -- \
  --indexer-url http://127.0.0.1:8080 \
  --vectors /absolute/path/to/vectors.json \
  --requests 20000 --concurrency 128
```

Record throughput, p95 and p99 latency, memory, indexed block progress, and
recovery behavior. Repeat with the same provider limits and replica resources
used in production.

Before and after quote-path changes, run the deterministic in-process matrix:

```sh
make performance-baseline
```

The timing phase exercises one real `ConnectedQuoteClient` from 128 concurrent
readers. Allocation counting runs separately with one reader because its
instrumented allocator would otherwise distort latency and RSS. Compare JSON
reports only on the same pinned host, release profile, and CPU policy.

For a reviewable baseline/current comparison, capture each revision in a clean
directory on the same idle machine:

```sh
make performance-capture PERF_OUTPUT=/var/tmp/lunarbase-perf-baseline
make performance-capture PERF_OUTPUT=/var/tmp/lunarbase-perf-current
make performance-gate \
  PERF_BASELINE=/var/tmp/lunarbase-perf-baseline \
  PERF_CURRENT=/var/tmp/lunarbase-perf-current
```

Capture covers 15/64 lanes, 100 pairs, batches 1/16/256, 128 quote readers,
and both quote-only and mixed quote/event pressure. It runs timing and mixed
scenarios 10 times in fresh processes and measures allocations separately. The
gate compares medians and rejects more than 3% quote-throughput, 5% p99, or 5%
peak-RSS regression. Allocation count and bytes may not grow. These margins are
noise tolerances, not performance budgets; repeat any borderline failure on the
same idle host before changing the accepted baseline.

## Release verification

Before publishing or deploying:

```sh
make pre-push
docker build --build-arg NETWORK_FEATURES=base .
```

Publishing a vX.Y.Z GitHub Release reruns the release gate before registry
publication. Publication also waits for the `lunarbase-performance` dedicated
runner. Keep that runner idle, pin its CPU policy and toolchain, and do not run
unrelated services on it. Set the repository variable
`LUNARBASE_PERFORMANCE_BASELINE_REF` to a reviewed immutable commit that contains
the same performance schema and capture tooling. Moving the baseline requires a
separate reviewed decision; the release job never updates it automatically.

The process E2E gate starts dedicated quote and event-worker connections, kills
the event worker during ingestion, verifies inclusive replay without duplicate
stream entries, reclaims an abandoned consumer-group entry, then kills Redis.
After Redis restarts, AOF data must still exist and the worker must persist the
event received during the outage. Queue saturation tests additionally require
backpressure or a terminal recovery gap; silent event loss is forbidden.
