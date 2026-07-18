# `@lunarbase/client-arbitrum`

Experimental Arbitrum Nitro client.

## Install

```bash
pnpm add @lunarbase/math @lunarbase/client-core @lunarbase/client-arbitrum
```

## Connect

```ts
import { connectArbitrum } from "@lunarbase/client-arbitrum";

const client = await connectArbitrum(config, optionalCheckpoint);
const quote = client.quote(request);
await client.shutdown();
```

The adapter consumes executed logs and preserves EVM execution-block context
separately from stream ordering. Fetch and WebSocket implementations can be
injected through `ArbitrumClientOptions`.

Keep this package behind an experimental gate until Nitro-node execution
semantics and canonical recovery pass live validation.
