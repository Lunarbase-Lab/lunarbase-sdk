# LunarBase off-chain quoting

This repository implements the quote model pinned to contract commit
`24db47b866e8150a0d91cffd80efe49df85179b5`.

The module map and dependency boundaries are documented in
[`ARCHITECTURE.md`](ARCHITECTURE.md).

The math packages are independent from RPC, Redis, clocks, workers, and
filesystem state. Runtime responsibilities are split into publishable units:

- `crates/lunarbase-math` uses `ruint::U256`/`U512` and exposes exact Solidity
  rounding, slot0 codecs, direct/route quotes, fee adjustment and fee split.
- `packages/math` is the equivalent `bigint` implementation. No monetary path
  accepts a JavaScript `number`.
- `crates/lunarbase-client-core` and `packages/client-core` own the universal
  runtime, ordered reducer, snapshot handoff, execution-reader boundary,
  checkpoint namespaces, gap handling, and persistence.
- `lunarbase-client-base`, `lunarbase-client-monad`, and
  `lunarbase-client-arbitrum` contain network-specific Rust code. Matching
  `@lunarbase/client-*` packages provide the TypeScript clients.
- `crates/lunarbase-client` and `packages/client` are compatibility facades.
- `crates/lunarbase-indexer` is the runnable Rust process that composes the
  universal runtime, one selected network adapter, optional Redis persistence,
  and the quote HTTP API. Its default Cargo feature is `base`.
- `crates/lunarbase-tools` contains real-process E2E, 10–15 lane / 50–100 pair
  load, and live Monad parser/RPC/Solidity soak runners.

`LaneState.slot0` is kept as the canonical `ruint::U256`/TypeScript `bigint`
word. `LaneSlot0` is a decode/encode view used only at boundaries; quote-path
accessors mask the four fields they need directly. This avoids allocating a
decoded struct for every lane and avoids converting a 256-bit storage word to
an array-backed bitfield representation. The checkpoint codec also stores the
ABI widths directly (`uint8` delay and `uint32` slippage K).

The implementation intentionally does not add a runtime bitfield dependency:
the relevant crates either target primitive representations up to `u128`, or
represent wider values as byte arrays. That is useful for ergonomic schemas,
but it adds a conversion/array boundary around the already-canonical 32-byte
word. Manual mask/shift accessors are constant-time, inlinable, `no_std`
compatible at the math layer, and match Solidity's layout exactly.

## Checks

```sh
make build
make test
make verify
```

The root `Makefile` is the canonical entry point for the repository:
`build` compiles every Rust workspace target and all TypeScript packages;
`test` runs the Rust and TypeScript suites; `lint`, `docs`, `fmt-check`, and
`verify` provide the corresponding CI checks. The Solidity differential FFI
suite can be run with `make ffi` when the sibling `lunarbase-contracts`
checkout is available.

Formatting and lint policy is versioned with the source: TypeScript uses the
flat ESLint config in `eslint.config.mjs` and Prettier settings in
`prettier.config.mjs`; Rust uses `rustfmt.toml`, `clippy.toml`, and shared
workspace lints in `Cargo.toml`. Run `make fmt` to apply formatting,
`make fmt-check` to validate it, and `make lint` to run ESLint plus Clippy with
warnings treated as errors. TypeScript tooling is pinned in `package.json` and
`pnpm-lock.yaml`; `make build` and other Node targets transparently use
Corepack when a standalone `pnpm` binary is not on `PATH`.

Network adapters implement common source contracts from `client-core`. A
source gap, reorg, removed log, code-hash mismatch, or impossible principal
transition makes the snapshot unavailable until canonical recovery.

## Run the indexer

Edit the deployment identity and endpoints in `config/base.toml`, then start
the complete Base indexer and quote API:

```sh
make run
```

The same binary can be compiled for another network without pulling unused
adapters into the build:

```sh
make run NETWORK=monad
make run NETWORK=arbitrum
```

The service exposes `GET /health/live`, `GET /health/ready`, `GET /metrics`,
and `POST /v1/quote` on the configured bind address. Monetary values and block
numbers are JSON strings so callers never cross a JavaScript-number boundary.

```sh
curl -s http://127.0.0.1:8080/v1/quote \
  -H 'content-type: application/json' \
  -d '{
    "router":"0x0000000000000000000000000000000000000001",
    "assetIn":"0x0000000000000000000000000000000000000002",
    "assetOut":"0x0000000000000000000000000000000000000003",
    "amount":"1000000000000000000",
    "mode":"exactIn",
    "executionBlockNumber":"123456",
    "minimumCommitment":"realtime",
    "maxAgeBlocks":2
  }'
```

`config/monad.toml` points at the local execution-events parser path used by
the Monad adapter. Redis is disabled in the templates; enable it to persist
checkpoints and the bounded recovery stream.

## Horizontal replicas

When Redis is enabled, writer leasing is enabled by default. Every process
starts its HTTP server immediately, but only the owner of the deployment's
Redis lease creates an indexing client and serves quotes. Other replicas stay
live as standbys and return `503` from readiness/quote endpoints. Use a unique
`LUNARBASE_WRITER_ID` for each replica.

Acquire uses `SET NX PX`; renew and release are owner-checked Lua operations.
Renewal errors and lost ownership fail closed: the client is removed from HTTP
state before source/reducer cleanup. `SIGTERM` writes a final checkpoint and
releases the lease; if Redis is unavailable, TTL remains the fencing fallback.

Prometheus metrics include process role/readiness, block/lag/commitment,
source reconnect/gap/recovery counters, quote latency/errors, Redis
checkpoint latency/failures, queue utilization, lease transitions, alert
delivery failures, and shutdown failures.

## Graceful shutdown and alerts

`lunarbase-indexer` handles both `SIGTERM` and `Ctrl-C` during startup,
readiness waiting, and normal HTTP serving. Shutdown proceeds in this order:

1. Stop accepting new HTTP traffic and drain active requests.
2. Mark the quote reducer unavailable.
3. Cooperatively cancel source subscription, stream reads, reconnect waits,
   recovery, and reducer work.
4. Join the source and reducer tasks within `[shutdown].timeout_seconds`.
5. Persist one final checkpoint with the same bounded deadline.
6. Abort remaining tasks only as an emergency timeout fallback.

Cancelling startup while the RPC snapshot is in progress also aborts the
already-started realtime source pump; it cannot remain detached.
Synchronous Redis commits run on Tokio's blocking pool instead of occupying an
async runtime worker. Redis connect/read/write operations use
`[redis].io_timeout_milliseconds`; configuration validation ensures two
attempts fit within the process shutdown timeout.

Operational failures are always written as structured tracing records with
`alert=true`, a stable `code`, severity, network, chain id, and Core address.
The supervisor reports:

- source subscription/stream failures and unexpected stream closure;
- reducer transition and canonical recovery failures;
- Redis/final-checkpoint failures;
- unexpected background-task termination or panic;
- a reducer remaining not-ready beyond the configured threshold;
- process panics and shutdown deadline overruns;
- recovery after a prolonged not-ready period.

Set the webhook through the environment rather than committing a secret:

```sh
LUNARBASE_ALERT_WEBHOOK_URL=https://alerts.example/internal/lunarbase \
make run
```

The webhook receives generic JSON containing `service`, `severity`, `code`,
`message`, deployment identity, timestamp, and a human-readable `text` field.
Repeated alerts are deduplicated by code according to
`[alerts].repeat_interval_seconds`. Failed webhook delivery is logged and
becomes immediately eligible for retry.

Setting `[alerts].enabled = false` disables readiness polling and webhook
delivery. Runtime failures and panics are still emitted to structured logs.

## Monad parser smoke test

The Monad client includes a real WebSocket reader for the local
`monad-exec-events-parser` protocol. It subscribes to `logs` plus the parser's
`all` stream, maps `proposed/finalized/verified` to
`Realtime/Canonical/Finalized`, and converts `subscriptionGap`, expired ring,
and stalled-reader signals into a mandatory normalized `Gap`.

```sh
LUNARBASE_CORE=0x... \
LUNARBASE_MONAD_PARSER_WS=ws://127.0.0.1:8080/ws/subscriptions \
cargo run -p lunarbase-client-monad --example monad-parser-smoke
```

The parser's `seqno` is global across all execution events. Filtered `logs`
therefore have sparse seqnos; the adapter only rejects regressions and
duplicates, while complete raw-ring readers may use strict contiguous gap
detection.

The parser reader feeds the universal Rust `MonadExecutionEngine`. A future
native shared-memory reader only needs to implement `ExecutionEventReader`.
TypeScript exposes the same boundary through `MonadExecutionEventsSource` and
`MonadSidecarBackend`.

For Base, use `BaseFlashblocksBackend` with the documented `pendingLogs` and
`newFlashblocks` subscriptions. For Arbitrum, use `ArbitrumNitroBackend` on
executed Nitro state; it fails closed when a realtime head omits the
EVM-visible parent block context.

The high-level clients start realtime ingestion before the block-tagged
snapshot, apply a bounded handoff, persist checkpoints after accepted updates,
and resnapshot/backfill after gaps, reorgs, removed logs, or code-hash
mismatches. Redis itself is an external service; the library manages keys,
leases, atomic checkpoint/stream writes, deduplication, health, and shutdown,
but does not spawn a Redis server.

## Operational validation and delivery

Build and run the full local RPC/WebSocket/Redis/process/failover scenario:

```sh
make test-process-e2e
```

Run the default target topology benchmark:

```sh
make load LANES=15 PAIRS=100 REQUESTS=20000 CONCURRENCY=128
```

`lunarbase-monad-validate` subscribes directly to the parser's `all` stream,
audits global sequence and proposed/finalized/verified transitions, samples
RPC/indexer lag and readiness, and compares configured service quote fields
with Solidity `eth_call` words as exact `uint256` values. `make
monad-live-validate` runs it for one hour by default; use `SOAK_SECONDS=86400`
for a day.

The production-shaped container topology is:

```sh
docker compose up --build -d
```

Replace every placeholder in `config/production.base.toml` first. CI validates
all Base/Monad/Arbitrum feature builds and runs the real-process E2E harness.
Tagged releases build per-network binaries; registry publication is a manual,
secret-gated workflow. See [`PRODUCTION_RUNBOOK.md`](PRODUCTION_RUNBOOK.md).
