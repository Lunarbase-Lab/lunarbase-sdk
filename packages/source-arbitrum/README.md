# `@lunarbase-lab/pmm-v2-source-arbitrum`

Arbitrum Nitro data source for the LunarBase client.

Status: **maintenance**. Updates focus on compatibility, reliability, and security fixes.

## Install

```bash
npm install @lunarbase-lab/pmm-v2-math @lunarbase-lab/pmm-v2-client @lunarbase-lab/pmm-v2-source-arbitrum
```

## Use

```ts
import { connect } from "@lunarbase-lab/pmm-v2-client";
import { ArbitrumNitroSource } from "@lunarbase-lab/pmm-v2-source-arbitrum";

const source = new ArbitrumNitroSource({
  httpRpcUrl,
  realtimeUrl,
  chainId: config.deployment.chainId,
});
const client = await connect(config, source, optionalCheckpoint);
const quote = client.quote(request);
await client.shutdown();
```

## Behavior

- HTTP RPC provides bootstrap, backfill, and canonical recovery.
- A compatible Nitro HTTP endpoint must expose `l1BlockNumber` for every backfilled block.
- The realtime endpoint must include `l1BlockNumber` in `newHeads` notifications.
- Custom Fetch and WebSocket transports can be supplied by the application.
