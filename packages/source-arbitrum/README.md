# `@lunarbase/source-arbitrum`

Experimental Arbitrum Nitro source.

## Install

```bash
pnpm add @lunarbase/math @lunarbase/client @lunarbase/source-arbitrum
```

## Connect

```ts
import { connect } from "@lunarbase/client";
import { ArbitrumNitroSource } from "@lunarbase/source-arbitrum";

const source = new ArbitrumNitroSource({
  httpRpcUrl,
  realtimeUrl,
  chainId: config.deployment.chainId,
});
const client = await connect(config, source, optionalCheckpoint);
const quote = client.quote(request);
await client.shutdown();
```

The source consumes executed logs and preserves EVM execution-block context
separately from stream ordering. Fetch and WebSocket implementations can be
injected through `ArbitrumSourceOptions`.

Keep this package behind an experimental gate until Nitro-node execution
semantics and canonical recovery pass live validation.
