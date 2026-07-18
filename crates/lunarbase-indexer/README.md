# `lunarbase-indexer`

Runnable Rust HTTP service built from the embeddable LunarBase clients.

## Run Base

Edit `config/base.toml`, including Core, router, code hash, RPC, and realtime
endpoints:

```bash
make run
```

Equivalent command:

```bash
cargo run -p lunarbase-indexer \
  --no-default-features --features base \
  -- --config config/base.toml
```

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
log, queue overflow, incompatible code hash, or reducer failure revokes
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
