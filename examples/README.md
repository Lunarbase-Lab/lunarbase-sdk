# Runtime wiring examples

The pure math packages remain transport-free. A production service wires the
client boundary to an HTTP RPC snapshot provider, one realtime backend, and an
optional Redis checkpoint store.

Rust Monad parser smoke test:

```sh
LUNARBASE_CORE=0x... \
LUNARBASE_MONAD_PARSER_WS=ws://127.0.0.1:8080/ws/subscriptions \
cargo run -p lunarbase-client-monad --example monad-parser-smoke
```

TypeScript selects `MonadExecutionEventsSource`, `BaseFlashblocksSource`, or
`ArbitrumNitroSource`. Their backends accept a bounded `WebSocketFactory`
appropriate to the runtime. Canonical state still comes from
`RpcSnapshotProvider`; a source gap is a recovery signal, never a reason to
serve an unverified stale quote.

The production Rust composition is available as `lunarbase-indexer`. After
editing `config/base.toml`, run the default Base service with:

```sh
make run
```

Select another compiled adapter with `make run NETWORK=monad` or
`make run NETWORK=arbitrum`.

Production deployments should scrape `/metrics` and load
`config/prometheus-alerts.yml` into their Prometheus/Alertmanager stack.
`SIGTERM` drains HTTP requests, cooperatively stops runtime workers, and
best-effort writes a final checkpoint before the process exits.
