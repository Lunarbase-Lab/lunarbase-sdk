# Runtime wiring examples

The pure math packages remain transport-free. A production service wires the
client boundary to an HTTP RPC snapshot provider, one realtime backend, and an
optional Redis checkpoint store.

Rust Monad sidecar smoke test:

```sh
LUNARBASE_CORE=0x... \
LUNARBASE_MONAD_PARSER_WS=ws://127.0.0.1:8080/ws/subscriptions \
cargo run -p lunarbase-monad-sidecar --example monad-parser-smoke
```

TypeScript selects `MonadSidecarBackend`, `BaseFlashblocksBackend`, or
`ArbitrumNitroBackend` and injects a bounded `WebSocketFactory` appropriate to
the runtime. Canonical state still comes from `RpcSnapshotProvider`; a source
gap is a recovery signal, never a reason to serve an unverified stale quote.
