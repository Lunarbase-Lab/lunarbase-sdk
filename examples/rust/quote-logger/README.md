# Rust realtime quote logger

Runnable Rust example for RPC bootstrap, WebSocket updates, and batched quotes.

## Configure

```bash
cp examples/rust/quote-logger/.env.example examples/rust/quote-logger/.env
```

Set `RPC_URL` and `CORE_ADDRESS`. Set `WS_URL` when it cannot be derived from
`RPC_URL`.

Use `SOURCE_PROFILE=evm` for standard `logs` and `newHeads`, or
`SOURCE_PROFILE=base-flashblocks` for Base Flashblocks. Optional settings
include `VERIFIED_ROUTER_ADDRESS`, `DEPLOYMENT_BLOCK`, and `LANE_ASSETS`.
`FEE_CLASS` is required; the verified router is needed only for an exact
partner/treasury allocation.

## Run

```bash
make quote-logger-rust
```

Each interval evaluates one `quote_many` batch against one state cursor.
Press `Ctrl+C` for graceful shutdown.
