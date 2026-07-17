# LunarBase SDK

LunarBase SDK `0.2.0` provides bit-exact off-chain quote math, embeddable
realtime clients, and a runnable Rust indexer.

The quote hot path is deliberately small:

```text
realtime stream → normalize → ordered reducer → in-memory state → quote/quoteMany
```

RPC is used only for bootstrap and canonical recovery. Redis is optional and
stores one full restart checkpoint. Neither dependency is touched while a
quote is calculated.

## Packages

Pure math:

- `lunarbase-math` — Rust `U256/U512` quote implementation.
- `@lunarbase/math` — TypeScript `bigint` implementation.

Embeddable clients:

- `lunarbase-client-core` / `@lunarbase/client-core` — common source contract,
  ordered reducer, in-memory runtime, and generic RPC/WS transport.
- `lunarbase-client-base` / `@lunarbase/client-base` — Base
  `pendingLogs + newHeads`.
- `lunarbase-client-monad` / `@lunarbase/client-monad` — Monad parser WS; Rust
  additionally supports the native execution-event ring on Linux.
- `lunarbase-client-arbitrum` / `@lunarbase/client-arbitrum` — executed Nitro
  logs and EVM execution-block context.

Runnable component:

- `lunarbase-indexer` — Rust HTTP service with optional Redis checkpointing,
  Prometheus metrics, and graceful shutdown.

There are no aggregate facade packages. Consumers depend only on core plus the
network adapter they actually use.

## Release status

| Component | Status |
| --- | --- |
| Pure Rust/TypeScript math | parity-gated |
| Common clients | ready for integration testing |
| Base adapter | release candidate; stable after deployment live smoke |
| Monad adapter | experimental until native-node soak |
| Arbitrum adapter | experimental until Nitro-node validation |

The math compatibility baseline is pinned to
`lunarbase-contracts@24db47b866e8150a0d91cffd80efe49df85179b5:math-v1`.
Canonical Solidity/Rust/TypeScript differential tests live in
`lunarbase-contracts`.

## Build and verify

Prerequisites are stable Rust, Node.js 22+, pnpm, and Foundry for FFI tests.
The Makefile falls back to Corepack when `pnpm` is not installed directly.

```bash
make build
make test
make verify
```

Useful focused commands:

```bash
make test-runtime
make test-process-e2e
make load
make ffi
make monad-live-validate
```

`make source-size-check` enforces the 500-line source-file limit.

## Run the indexer

Edit `config/base.toml`, especially `core`, `router`, code hash, and endpoints,
then run:

```bash
make run
```

Equivalent explicit command:

```bash
cargo run -p lunarbase-indexer \
  --no-default-features --features base \
  -- --config config/base.toml
```

Base is the default feature. Experimental adapters can be built with
`NETWORK=monad` or `NETWORK=arbitrum`. A native Monad deployment beside the
node uses the Linux x86_64-only `monad-native` feature. Build its production
image with `make docker-build-monad-native`; the explicit platform also makes
the command work from an Apple Silicon development machine.

Docker Compose starts the Base indexer and optional Redis acceleration:

```bash
make docker-up
```

Every process independently indexes and serves quotes. Run multiple replicas
behind a load balancer without a writer lease or leader election.

## HTTP API

`POST /v1/quote`:

```json
{
  "assetIn": "0x0000000000000000000000000000000000000001",
  "assetOut": "0x0000000000000000000000000000000000000002",
  "amount": "1000000000000000000",
  "mode": "exactIn"
}
```

The configured router, execution block, commitment, and freshness policy are
runtime-owned. A successful response contains the exact cursor and execution
block used:

```json
{
  "cursor": {
    "chainId": 8453,
    "blockNumber": 123,
    "executionBlockNumber": 123,
    "blockHash": "0x...",
    "commitment": "realtime",
    "sourceSequence": null
  },
  "executionBlockNumber": 123,
  "result": {
    "status": "available",
    "amountIn": "1000000000000000000",
    "amountOut": "998000000000000000",
    "feeAsset": "0x...",
    "feeAmount": "2000000000000000",
    "partnerFee": "0",
    "treasuryFee": "2000000000000000"
  }
}
```

`POST /v1/quotes` accepts either an array or `{ "requests": [...] }`, with a
maximum of 256 requests. Every result in the response is computed while
holding one shared state snapshot and therefore has one cursor.

Operational endpoints:

- `GET /healthz` — process liveness.
- `GET /readyz` — quote readiness and cursor.
- `GET /metrics` — Prometheus exposition.

A gap, reorg, removed log, queue overflow, invalid code hash, or failed state
transition makes quote endpoints return `503` until canonical recovery
completes.

## Embedding

TypeScript Base:

```ts
import { connectBase } from "@lunarbase/client-base";

const client = await connectBase(config, optionalCheckpoint);
const quote = client.quote(request);
const batch = client.quoteMany(requests);
await client.shutdown();
```

Rust clients expose matching high-level network constructors such as
`lunarbase_client_base::connect_base`. The lower-level core constructor accepts
a custom `ChainDataSource`:

```rust
let client = ConnectedQuoteClient::connect(config, source, checkpoint).await?;
let quote = client.quote(&request)?;
let batch = client.quote_many(&requests)?;
client.shutdown().await;
```

Client-core does not depend on Redis. Applications may persist the explicit
versioned checkpoint returned by `checkpoint()` using their own storage.

## Redis fallback

Redis belongs only to `lunarbase-indexer`. It uses one key:

```text
lunarbase:v3:{chainId}:{core}:{router}
```

The value is a versioned JSON DTO containing the complete quote state and has
no TTL. Writes are atomic full-value `SET`s. There are no Streams, leases,
dedup keys, fencing tokens, or standby roles.

At startup the checkpoint is accepted only when schema/math version, code
hash, deployment identity, router profile, and canonical block hash match.
Failure or unavailability falls back to an RPC snapshot. Redis errors never
revoke readiness of an already running process.

## Observability and alerts

Structured logs are emitted through `tracing`. `/metrics` includes readiness,
head/execution block, lag, commitment, queue utilization, reconnect/gap/recovery
counters, quote count/errors/latency, and checkpoint success/failure.

Alert delivery is intentionally external. Example Prometheus rules are in
`config/prometheus-alerts.yml`; route them through your existing Alertmanager.

## Performance invariants

- Rust hot state uses a short synchronous `RwLock`; quotes take a shared guard
  and do not clone state.
- TypeScript computes synchronously in one event-loop turn and does not expose
  mutable state maps.
- `Lane.slot0` remains one `U256`; mask/shift accessors avoid an extra bitfield
  dependency.
- Boundary views use native widths (`u128/u64/u32/u8/bool`) and convert to
  `U256` only at packing or arithmetic boundaries.
- Queues and reorder buffers are bounded. Overflow fails closed and triggers a
  complete canonical recovery.
