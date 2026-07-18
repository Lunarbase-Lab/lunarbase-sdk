# Realtime quote logger

This runnable crate demonstrates the complete embeddable Rust Base client flow:

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

For exact partner fees, set `ROUTER_ADDRESS` and its actual
`EXPECT_WHITELISTED` value. Without a router, the example uses a fixed
non-whitelisted demonstration address.

The high-level `lunarbase-client-base` constructor discovers chain ID, cash,
and all active lanes and uses Base's official `pendingLogs + newHeads` stream.
`DEPLOYMENT_BLOCK`
is optional but strongly recommended for production deployments because it
limits the lane-discovery log range.

## Run

From the repository root:

```bash
make quote-logger-rust
```

The process logs one exact-input quote in both directions for every active
lane. All quotes in one interval are computed with `quote_many` against one
state cursor. Press `Ctrl+C` for graceful shutdown.
