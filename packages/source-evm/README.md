# `@lunarbase-lab/pmm-v2-source-evm`

EVM HTTP and WebSocket data source for the LunarBase client.

Status: **fully supported** for standard EVM networks and Base Flashblocks.

## Install

```bash
npm install @lunarbase-lab/pmm-v2-math @lunarbase-lab/pmm-v2-client @lunarbase-lab/pmm-v2-source-evm
```

## Use

```ts
import { connect } from "@lunarbase-lab/pmm-v2-client";
import { createBaseFlashblocksSource } from "@lunarbase-lab/pmm-v2-source-evm";

const source = createBaseFlashblocksSource({
  httpRpcUrl,
  realtimeUrl,
  chainId: config.deployment.chainId,
});
const client = await connect(config, source, optionalCheckpoint);
const quote = client.quote(request);
await client.shutdown();
```

For Base, use `wss://mainnet-preconf.base.org` or
`wss://sepolia-preconf.base.org` as the application-facing realtime
endpoint. These endpoints provide `pendingLogs` and progressive `newHeads`;
see the [Base Flashblocks API](https://docs.base.org/base-chain/api-reference/flashblocks-api/flashblocks-api-overview).

Use `EvmRpcSource` for standard EVM `logs` and `newHeads` streams.

## Guarantees

- HTTP RPC provides bootstrap, backfill, and canonical recovery.
- WebSocket updates are normalized into ordered client events.
- The configured chain ID binds cursors and checkpoints to one deployment.
