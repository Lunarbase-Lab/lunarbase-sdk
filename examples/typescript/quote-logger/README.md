# TypeScript realtime quote logger

This private workspace package demonstrates the complete embeddable TypeScript
EVM client flow:

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

`SOURCE_PROFILE=evm` (the default) consumes canonical `logs + newHeads` on
standard EVM chains. Set `SOURCE_PROFILE=base-flashblocks` for Base
`pendingLogs + newHeads`.

For exact partner fees, set `ROUTER_ADDRESS` and its actual
`EXPECT_WHITELISTED` value. Without a router, the example uses a fixed
non-whitelisted demonstration address.

The example reads chain and implementation identity through
`@lunarbase/source-evm` and injects the selected source into
`@lunarbase/client`. The common client then discovers cash and active lanes.
`DEPLOYMENT_BLOCK` limits the lane-discovery range. On a non-archive public
RPC, set comma-separated `LANE_ASSETS` to avoid historical discovery calls.

## Run

From the repository root:

```bash
make quote-logger-ts
```

The process logs exact-input quotes in both directions for every lane. One
interval uses one synchronous `quoteMany` snapshot. `SIGINT` and `SIGTERM`
trigger cooperative client shutdown.
