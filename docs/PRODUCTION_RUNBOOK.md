# LunarBase indexer production runbook

## Deployment model

Run one independently indexing service per `(chain_id, Core, router)` profile.
Every replica subscribes to realtime data, maintains its own in-memory state,
and serves quotes. Put two or more ready replicas behind a load balancer; there
is no leader, writer lease, fencing token, or standby role.

Redis is optional restart acceleration. One schema-v4 key stores the complete
checkpoint without TTL. Redis outages and concurrent best-effort writes do not
affect a running replica or its readiness.

## Start

1. Copy `config/production.base.toml` and replace all deployment, implementation,
   RPC, and realtime values.
2. Verify that the configured ERC-1967 implementation and its code hash belong to the pinned contract
   compatibility revision.
3. Configure Redis only if faster restarts are useful.
4. Start at least two replicas and route traffic only to ready instances.

For the local production-shaped stack:

```sh
docker compose up --build -d
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
curl -fsS http://127.0.0.1:8080/metrics
```

The Compose file is a topology example, not a source of real deployment
addresses or RPC credentials.

## Probes and external alerts

- Liveness: `GET /healthz`; use only for process restart decisions.
- Readiness: `GET /readyz`; route quote traffic only on `200`.
- Metrics: `GET /metrics`; Prometheus text format.

Import `config/prometheus-alerts.yml` into Prometheus and route those alerts
through the deployment's Alertmanager. At minimum monitor sustained
not-readiness, gaps/recovery loops, queue saturation, quote errors and
checkpoint failures. A checkpoint failure means slower restart only.

## Gap, reorg, or reconnect

Each affected replica deliberately returns `503` while canonical recovery
runs. Other healthy replicas continue serving. If one replica cannot recover:

1. Remove that replica from traffic.
2. Verify RPC block/log availability and the Core implementation identity.
3. Inspect gap, reconnect, queue, and recovery metrics.
4. Restart the replica. An invalid or forked Redis checkpoint is ignored
   automatically, so deleting Redis data is normally unnecessary.

## Graceful shutdown

Use `SIGTERM` and make the orchestrator grace period longer than
`shutdown_timeout_seconds`. The process stops accepting quote traffic, joins
its source/reducer tasks, and best-effort writes one final checkpoint. A forced
kill only loses the latest restart checkpoint; another active replica is not
affected.

## Capacity validation

Run the checked-in harness against staging. Without `--vectors`, it generates
the requested 15-lane/100-pair request topology; for a real deployment, pass a
deployment-specific JSON vector file:

```sh
cargo run -p lunarbase-tools --bin lunarbase-load -- \
  --indexer-url http://127.0.0.1:8080 \
  --lanes 15 --pairs 100 --requests 20000 --concurrency 128 \
  --pid "$(pgrep -n lunarbase-indexer)"
```

The report contains throughput, p50/p95/p99 latency, RSS estimates, indexed
block progress, and checkpoint activity.

For a real Monad node/parser soak:

```sh
cargo run -p lunarbase-tools --bin lunarbase-monad-validate -- \
  --indexer-url http://127.0.0.1:8081 \
  --parser-ws-url ws://127.0.0.1:8080/ws/subscriptions \
  --parser-ready-url http://127.0.0.1:8080/readyz \
  --rpc-url http://127.0.0.1:8545 \
  --duration-seconds 86400 \
  --report monad-soak-report.json
```

Add `--vectors /absolute/path/to/live-validation.json` when Solidity
`eth_call` comparisons are available for the deployed private contracts.

The official native event-ring SDK currently targets Linux x86_64. Build the
colocated image with `make docker-build-monad-native`; the portable `monad`
feature remains available for parser WebSocket development on other hosts.

Monad and Arbitrum remain experimental until their node-based soak gates pass.
Base artifacts are the only production release artifacts for now.

## Release

`make verify` validates source size, formatting, builds, lint, tests, and docs.
`make release-check` inspects publishable crate and npm contents. The canonical
Solidity/Rust/TypeScript differential suite is run from
from a separately checked-out private contracts repository. From this SDK
workspace, pass its absolute location explicitly:

```sh
make ffi CONTRACTS_DIR=/absolute/path/to/lunarbase-contracts
```
