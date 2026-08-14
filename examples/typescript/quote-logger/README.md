# TypeScript realtime quote logger

Runnable TypeScript example for RPC bootstrap, WebSocket updates, and batched quotes.

## Configure

```bash
cp examples/typescript/quote-logger/.env.example examples/typescript/quote-logger/.env
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
make quote-logger-ts
```

Each interval evaluates one `quoteMany` batch against one state cursor.
`SIGINT` and `SIGTERM` trigger graceful shutdown.
