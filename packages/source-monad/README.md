# `@lunarbase-lab/pmm-v2-source-monad`

Monad execution-event data source for the LunarBase client.

Status: **maintenance**. Updates focus on compatibility, reliability, and security fixes.

## Install

```bash
npm install @lunarbase-lab/pmm-v2-math @lunarbase-lab/pmm-v2-client @lunarbase-lab/pmm-v2-source-monad
```

## Use

```ts
import { connect } from "@lunarbase-lab/pmm-v2-client";
import { createMonadParserSource } from "@lunarbase-lab/pmm-v2-source-monad";

const source = createMonadParserSource({
  httpRpcUrl,
  realtimeUrl,
  chainId: config.deployment.chainId,
});
const client = await connect(config, source, optionalCheckpoint);
const quote = client.quote(request);
await client.shutdown();
```

`realtimeUrl` must point to a compatible Monad parser WebSocket endpoint.

## Behavior

- HTTP RPC provides bootstrap, backfill, and canonical recovery.
- Parser sequence positions are retained in client updates.
- Gaps and reconnects require canonical recovery before readiness.
- WebSocket frame, handshake-prefetch, and pending proposal queues are bounded
  by both count and bytes; overflow is reported as a gap, never silently dropped.
- Parser socket queues use fixed-capacity ring buffers; handshake timeout and
  cancellation paths release their socket and AbortSignal listeners.
