# lunarbase-indexer

Runnable Rust quote service built from the LunarBase realtime client.

## Run

Provide deployment identity and endpoints through LUNARBASE_* variables, CLI
flags, or an operator-owned TOML file:

```sh
LUNARBASE_NETWORK=base \
LUNARBASE_CHAIN_ID=8453 \
LUNARBASE_CORE=0x... \
LUNARBASE_FEE_CLASS=whitelisted \
LUNARBASE_EXPECTED_IMPLEMENTATION=0x... \
LUNARBASE_EXPECTED_IMPLEMENTATION_CODE_HASH=0x... \
LUNARBASE_HTTP_RPC_URL=https://... \
LUNARBASE_REALTIME_URL=wss://... \
make run
```

CLI values override environment values, which override TOML and operational
defaults. Deployment identity and source endpoints are always explicit.

Base is the default feature. Select evm, monad, monad-native, or arbitrum with
--no-default-features and --features.

## API

- POST /v1/quote calculates one quote.
- POST /v1/quotes calculates up to 256 quotes at one state position.
- GET /healthz reports process liveness.
- GET /readyz reports quote readiness and the current cursor.
- GET /metrics exposes Prometheus metrics.

Deployment identity, fee policy, execution context, and freshness policy
cannot be overridden by HTTP requests. When source continuity or deployment
identity is uncertain, readiness is revoked until canonical recovery succeeds.

`LUNARBASE_VERIFIED_ROUTER` is optional. Without it, quotes use only the
selected fee class and skip router whitelist/partner RPCs. Set it only when the
API must include a chain-verified partner/treasury allocation.

## Event delivery

The quote service does not create an event queue, format protocol logs, write
stdout events, or wait for an event consumer. Run the standalone
[`lunarbase-event-worker`](../lunarbase-event-worker/README.md) for durable
Redis Stream delivery. It owns independent source connections, resource
limits, health, and metrics, so event backpressure cannot delay quotes.

Applications embedding `ConnectedQuoteClient` may explicitly enable its
embedded best-effort observer. That observer uses nonblocking delivery and drops
when its channel is full or closed; `event_observer_drops` records those drops.
It is intended for diagnostics, never for required event processing.

## Checkpoints

Redis is optional restart acceleration and is not used during quote
calculation. Protect Redis with network controls and authentication. Missing,
malformed, incompatible, or non-canonical checkpoint data is ignored in favor
of a canonical snapshot.

## Operations

Each replica indexes independently. Run multiple replicas behind a load
balancer and route traffic only to ready instances. See the
[production runbook](../../docs/PRODUCTION_RUNBOOK.md) for deployment,
monitoring, recovery, capacity validation, and graceful shutdown.
