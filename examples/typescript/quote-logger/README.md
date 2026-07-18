# TypeScript realtime quote logger

This private workspace package demonstrates the complete embeddable TypeScript
Base client flow:

```text
RPC snapshot + WebSocket updates → ordered client state → quoteMany → terminal
```

## Configure

```bash
cp examples/typescript/quote-logger/.env.example \
  examples/typescript/quote-logger/.env
```

Only `RPC_URL` and `CORE_ADDRESS` are required. `WS_URL` is derived by replacing
`http` with `ws`; set it explicitly when the provider uses another WebSocket
endpoint.

For exact partner fees, set `ROUTER_ADDRESS` and its actual
`EXPECT_WHITELISTED` value. Without a router, the example uses a fixed
non-whitelisted demonstration address.

The high-level `@lunarbase/client-base` constructor discovers chain ID, cash,
and active lanes and uses Base's official `pendingLogs + newHeads` stream.
`DEPLOYMENT_BLOCK` is optional but strongly recommended to limit the
lane-discovery log range.

## Run

From the repository root:

```bash
make quote-logger-ts
```

The process logs exact-input quotes in both directions for every lane. One
interval uses one synchronous `quoteMany` snapshot. `SIGINT` and `SIGTERM`
trigger cooperative client shutdown.
