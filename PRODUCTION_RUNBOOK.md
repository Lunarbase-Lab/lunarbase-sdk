# LunarBase indexer production runbook

## Deployment contract

Run one deployment per `(chain_id, Core address)` Redis namespace. Multiple
replicas are supported: Redis elects one active writer and all other replicas
remain HTTP-live standbys. A standby returns `503` from readiness and quote
endpoints until it acquires the writer lease.

Every replica for a deployment must use identical Core identity, runtime code
hash, snapshot policy, lane/router scope, and Redis URL. Give each process a
unique `LUNARBASE_WRITER_ID` (pod name or instance ID). The process-generated
fallback is unique but is less useful in logs.

## Start

1. Copy `config/production.base.toml` and replace all placeholder deployment
   values.
2. Verify the configured runtime bytecode hash against the selected snapshot
   block.
3. Set `LUNARBASE_ALERT_WEBHOOK_URL` through the secret manager.
4. Start Redis with persistence and backups.
5. Start at least two indexer replicas.

For the local production-shaped stack:

```sh
docker compose up --build -d
curl -fsS http://127.0.0.1:8080/health/live
curl -fsS http://127.0.0.1:8080/health/ready
curl -fsS http://127.0.0.1:8080/metrics
```

The Compose file is a topology example, not a source of real deployment
addresses or RPC credentials.

## Probes and alerts

- Liveness: `GET /health/live`; use only for process restart decisions.
- Readiness: `GET /health/ready`; route quote traffic only on `200`.
- Metrics: `GET /metrics`; Prometheus text format.

Alert on:

- no active `lunarbase_indexer_role{role="active"}` for longer than one lease
  TTL;
- `lunarbase_writer_lease_lost_total` or lease failure increases;
- sustained `lunarbase_indexer_ready == 0` on the active replica;
- source gaps, reconnects, recovery failures, Redis failures, alert failures,
  or shutdown failures increasing;
- queue depth above 80% of capacity;
- quote p99 breaching the partner SLO;
- indexed block lag exceeding the network-specific tolerance.

Standby readiness `503` is expected. Standby liveness must remain `200`.

## Failover

On lease renewal failure, the active replica removes its client from the HTTP
runtime before cleanup, stops reducer/source tasks, and returns to standby.
Another replica can acquire the lease after release or TTL expiry.

To test failover:

1. Confirm exactly one active and at least one standby.
2. Send `SIGTERM` to the active process.
3. Confirm it writes a final checkpoint and releases its owner-checked lease.
4. Confirm one standby becomes active and ready.
5. Compare checkpoint block, quote output, and error counters before restoring
   normal capacity.

Never delete the lease key to force failover while the writer is healthy.
Terminate or isolate the writer and let owner checks/TTL preserve fencing.

## Gap or reorg

The active replica deliberately returns `503` while canonical recovery runs.
Inspect source gap/reconnect/recovery counters and RPC health. If recovery
continues to fail:

1. Remove the replica from traffic.
2. Verify canonical RPC block/log availability and Core code hash.
3. Verify Redis latency and checkpoint compatibility metadata.
4. Restart one replica. Do not clear Redis unless compatibility is proven and
   a full resnapshot is acceptable.

## Graceful shutdown

Use `SIGTERM`; allow more than `[shutdown].timeout_seconds` in the orchestrator
termination grace period. The service stops quote availability, cooperatively
joins source/reducer tasks, writes a final checkpoint, releases its lease, and
then exits. A forced kill can only rely on lease TTL and the preceding durable
checkpoint.

## Capacity validation

Run the checked-in harness against a staging deployment:

```sh
cargo run -p lunarbase-tools --bin lunarbase-load -- \
  --indexer-url http://127.0.0.1:8080 \
  --vectors fixtures/load/quotes.json \
  --lanes 15 --pairs 100 --requests 20000 --concurrency 128 \
  --pid "$(pgrep -n lunarbase-indexer)"
```

For a real Monad node/parser soak:

```sh
cargo run -p lunarbase-tools --bin lunarbase-monad-validate -- \
  --indexer-url http://127.0.0.1:8081 \
  --parser-ws-url ws://127.0.0.1:8080/ws/subscriptions \
  --parser-ready-url http://127.0.0.1:8080/readyz \
  --rpc-url http://127.0.0.1:8545 \
  --vectors fixtures/monad/live-validation.json \
  --duration-seconds 86400 \
  --report monad-soak-report.json
```

Each Monad vector can contain a quote request and a Solidity `eth_call`.
The validator requires the selected `amountIn` or `amountOut` word to match
the service decimal result exactly as a `uint256`, and also audits parser
sequence/commitment transitions, readiness, lag, gaps, and recovery counters.

## Release

CI checks all network feature combinations. Tag builds produce separate
Base/Monad/Arbitrum binaries. Registry publication is manual and requires
`CARGO_REGISTRY_TOKEN` and `NPM_TOKEN`; publish leaves, network clients, and
facades in dependency order. Verify package contents with:

```sh
make release-check
```
