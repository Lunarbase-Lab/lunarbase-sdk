# Realtime quote logger

This runnable crate demonstrates the complete embeddable Rust EVM client flow:

```text
RPC snapshot + WebSocket updates → ordered client state → quoteMany → tracing
```

## Configure

Copy the example environment file:

```bash
cp examples/rust/quote-logger/.env.example examples/rust/quote-logger/.env
```

Only two variables are required:

```dotenv
RPC_URL=https://your-http-rpc
CORE_ADDRESS=0x...
```

`WS_URL` is derived by replacing `http` with `ws`. Set it explicitly when the
provider uses a different WebSocket endpoint.

`SOURCE_PROFILE=evm` (the default) consumes canonical `logs + newHeads` on
standard EVM chains. Set `SOURCE_PROFILE=base-flashblocks` for Base
`pendingLogs + newHeads`.

For exact partner fees, set `ROUTER_ADDRESS` and its actual
`EXPECT_WHITELISTED` value. Without a router, the example uses a fixed
non-whitelisted demonstration address.

The example reads chain and implementation identity through
`lunarbase-source-evm` and injects the selected source into
`lunarbase-client`. The common client then discovers cash and active lanes.
`DEPLOYMENT_BLOCK` limits the lane-discovery range. On a non-archive public
RPC, set comma-separated `LANE_ASSETS` to avoid historical discovery calls.

## Run

From the repository root:

```bash
make quote-logger-rust
```

The process logs one exact-input quote in both directions for every active
lane. All quotes in one interval are computed with `quote_many` against one
state cursor. Press `Ctrl+C` for graceful shutdown.
