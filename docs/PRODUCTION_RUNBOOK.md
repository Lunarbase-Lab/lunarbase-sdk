# LunarBase indexer production runbook

## Deployment

Run at least two independently indexing replicas for each chain, Core, and
router profile. Route traffic only to replicas whose /readyz endpoint returns 200.

Supply deployment identity, implementation address and code hash, RPC
endpoints, and source settings through command-line arguments, LUNARBASE_*
environment variables, or an operator-owned TOML file. Checked-in files under
examples/indexer are templates only.

Redis is optional restart acceleration. When enabled, keep it on an
access-controlled network, require authentication, encrypt transport where
available, and include it in backup and credential-rotation policy.

## Start

Before rollout:

1. Verify chain ID, Core, router, deployment block, whitelist expectation,
   implementation address, and runtime-code hash.
2. Verify RPC archive range and realtime subscription limits.
3. Size queues and timeouts for the provider and expected event rate.
4. Start replicas independently and wait for readiness.

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
```

## Monitoring

- /healthz is a process liveness signal.
- /readyz is the traffic-routing signal.
- /metrics exposes Prometheus metrics.

Alert on sustained non-readiness, repeated recovery, queue saturation, source
disconnects, quote errors, and checkpoint failures. A checkpoint failure
affects restart speed; it does not invalidate a running ready replica.

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

## Graceful shutdown

Send SIGTERM and set the orchestrator grace period above
shutdown_timeout_seconds. The service revokes readiness, stops background
work, closes HTTP gracefully, and writes a final checkpoint when configured.

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

## Release verification

Before publishing or deploying:

```sh
make pre-push
docker build --build-arg NETWORK_FEATURES=base .
```

Publishing a vX.Y.Z GitHub Release reruns the release gate before registry
publication.
