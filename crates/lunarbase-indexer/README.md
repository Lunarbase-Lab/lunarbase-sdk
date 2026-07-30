# `lunarbase-indexer`

Runnable Rust HTTP service built from the embeddable LunarBase clients.

## Run Base

Set deployment identity and source endpoints through `LUNARBASE_*`:

```bash
LUNARBASE_NETWORK=base \
LUNARBASE_CHAIN_ID=8453 \
LUNARBASE_CORE=0x... \
LUNARBASE_ROUTER=0x... \
LUNARBASE_EXPECTED_IMPLEMENTATION=0x... \
LUNARBASE_EXPECTED_IMPLEMENTATION_CODE_HASH=0x... \
LUNARBASE_HTTP_RPC_URL=https://... \
LUNARBASE_REALTIME_URL=wss://... \
make run
```

Every value also has a kebab-case CLI flag. An optional TOML is a lower
precedence base layer:

```bash
cargo run -p lunarbase-indexer \
  --no-default-features --features base \
  -- --config /absolute/path/to/deployment.toml \
     --http-rpc-url https://override.example
```

Precedence is CLI, then environment, then TOML, then safe operational defaults.
Repository profiles under `examples/indexer/config` are examples only.

Base is the default feature. Experimental builds use `monad`, `monad-native`,
or `arbitrum`.

## API

- `POST /v1/quote` calculates one quote.
- `POST /v1/quotes` calculates at most 256 quotes on one state snapshot.
- `GET /healthz` reports process liveness.
- `GET /readyz` reports quote readiness and the current cursor.
- `GET /metrics` exposes Prometheus metrics.

The configured router, execution block, commitment, and freshness policy are
runtime-owned and cannot be overridden by an HTTP caller. Gap, reorg, removed
log, queue overflow, incompatible implementation, or reducer failure revokes
readiness until canonical recovery completes.

## Redis

Redis is optional restart acceleration. One versioned checkpoint is stored per
chain, Core, router, and schema, without TTL. Redis is never used on the quote
path, and an unavailable Redis instance does not revoke readiness after the
service has started.

## Operations

Every replica independently indexes and serves quotes; no writer lease or
leader election is used. See the repository `PRODUCTION_RUNBOOK.md` for
Docker, graceful shutdown, alerts, recovery, and deployment checks.
