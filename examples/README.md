# Runtime wiring examples

The examples are grouped by language and implement the same realtime quote
logger:

- [`rust/quote-logger`](rust/quote-logger/README.md) is a binary crate built on
  `lunarbase-client`.
- [`typescript/quote-logger`](typescript/quote-logger/README.md) is a private
  pnpm workspace package built on `@lunarbase/client`.

The [`typescript/activity-actor`](typescript/activity-actor/README.md) example
is a separate BSC Testnet-only service. It creates a dedicated local wallet,
checks pool readiness, mints permissionless mock tokens when needed, and sends
small sequential exact-input swaps after an explicit double broadcast gate.

Both load `RPC_URL` and `CORE_ADDRESS` from `.env`, bootstrap state through
RPC, subscribe to WebSocket updates, and log exact-input quotes in both
directions for every active lane. Each interval uses one `quoteMany` snapshot.

## Rust

```sh
cp examples/rust/quote-logger/.env.example examples/rust/quote-logger/.env
make quote-logger-rust
```

## TypeScript

```sh
cp examples/typescript/quote-logger/.env.example \
  examples/typescript/quote-logger/.env
make quote-logger-ts
```

The legacy `make quote-logger` alias continues to run the Rust example.

See each example's README for optional router, WebSocket, deployment-block,
network, quote-amount, and logging configuration.

The pure math packages remain transport-free. A production service wires the
client boundary to an HTTP RPC snapshot provider, one realtime backend, and an
optional Redis checkpoint store.

Rust Monad parser smoke test:

```sh
LUNARBASE_CORE=0x... \
LUNARBASE_MONAD_PARSER_WS=ws://127.0.0.1:8080/ws/subscriptions \
cargo run -p lunarbase-source-monad --example monad-parser-smoke
```

TypeScript composes the common `connect` runtime with
`createBaseFlashblocksSource`, `createMonadParserSource`, or
`ArbitrumNitroSource`. The source packages accept a bounded
`WebSocketFactory` appropriate to the runtime. Canonical state still comes
from RPC snapshots; a source gap is a recovery signal, never a reason to serve
an unverified stale quote.

The production Rust composition is available as `lunarbase-indexer`. After
supplying deployment parameters through CLI flags, `LUNARBASE_*`, or an
operator-owned TOML, run the default Base service with:

```sh
make run
```

Select another compiled source with `make run NETWORK=monad` or
`make run NETWORK=arbitrum`.

Production deployments should scrape `/metrics` and load
[`indexer/prometheus-alerts.yml`](indexer/prometheus-alerts.yml) into their
Prometheus/Alertmanager stack. Example TOML profiles live under
[`indexer/config`](indexer/config).
`SIGTERM` drains HTTP requests, cooperatively stops runtime workers, and
best-effort writes a final checkpoint before the process exits.
