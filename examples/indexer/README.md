# Runnable indexer configuration examples

`lunarbase-indexer` does not require repository-owned configuration. Supply
deployment identity and endpoints through CLI flags, `LUNARBASE_*` environment
variables, or an optional TOML file maintained by the operator.

The files under [`config`](config) are examples and live-test fixtures only.
Copy one into deployment-owned infrastructure before changing it:

```sh
cargo run -p lunarbase-indexer \
  --no-default-features --features base \
  -- --config /absolute/path/to/deployment.toml
```

For containers and secret-managed environments, omit `--config` and provide
the same values directly:

```sh
LUNARBASE_NETWORK=base \
LUNARBASE_CHAIN_ID=8453 \
LUNARBASE_CORE=0x... \
LUNARBASE_ROUTER=0x... \
LUNARBASE_EXPECTED_IMPLEMENTATION=0x... \
LUNARBASE_EXPECTED_IMPLEMENTATION_CODE_HASH=0x... \
LUNARBASE_HTTP_RPC_URL=https://... \
LUNARBASE_REALTIME_URL=wss://... \
LUNARBASE_EVENT_LOG_MIN_COMMITMENT=realtime \
lunarbase-indexer
```

Explicit CLI flags override environment values, which override the optional
TOML. Operational defaults cover bind address, queue bounds, reconnect timing,
checkpoint cadence, and shutdown timeout; deployment identity and source
endpoints are always explicit.

The Core event logger accepts `realtime`, `canonical`, or `finalized` through
`LUNARBASE_EVENT_LOG_MIN_COMMITMENT`. Commitment comes from the source; choosing
`canonical` on Base therefore suppresses normal realtime live logs.

The adjacent [`prometheus-alerts.yml`](prometheus-alerts.yml) is an example for
external monitoring and is not loaded by the indexer.

For the Compose topology, copy [`.env.example`](.env.example) to `.env`,
fill its required deployment values, and pass it with `--env-file`. The mounted
TOML supplies Base runtime defaults; the environment supplies deployment
identity and source endpoints.
