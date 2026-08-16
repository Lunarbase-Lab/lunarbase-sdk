# `@lunarbase-lab/pmm-v2-source-monad`

Monad parser compatibility adapter for the LunarBase client.

Status: **workspace-only maintenance**. This package is built and tested in the
SDK workspace but is not published to npm.

## Availability

The TypeScript runtime cannot access Monad's native Event Ring directly. This
adapter supports the compatibility parser WebSocket protocol only; it does not
implement protocol-v2 identity, proposal lifecycle, ACK/resume, or disk-backed
ring gap semantics. Do not use it as a production Monad Event Ring source.

## Workspace use

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

`realtimeUrl` must point to a compatible Monad parser WebSocket
endpoint. Production protocol-v2 integrations currently require the Rust
workspace source and the external execution-events parser.

## Behavior

- HTTP RPC provides bootstrap, backfill, and canonical recovery.
- Parser sequence positions are retained in client updates.
- Gaps and reconnects require canonical recovery before readiness.
- WebSocket frame and handshake-prefetch queues are bounded by both count and
  bytes; overflow is reported as a gap, never silently dropped.
- Parser socket queues use fixed-capacity ring buffers; handshake timeout and
  cancellation paths release their socket and AbortSignal listeners.
