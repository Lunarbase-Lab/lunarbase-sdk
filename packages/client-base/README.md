# `@lunarbase/client-base`

Ready-to-embed Base client using official `pendingLogs + newHeads`.

## Install

```bash
pnpm add @lunarbase/math @lunarbase/client-core @lunarbase/client-base
```

## Connect

```ts
import { connectBase } from "@lunarbase/client-base";

const client = await connectBase(config, optionalCheckpoint);
const quote = client.quote(request);
await client.shutdown();
```

HTTP RPC is used for bootstrap and canonical recovery. The configured
Flashblocks WebSocket endpoint supplies realtime pending logs and progressive
heads. Transport dependencies can be injected through `BaseClientOptions` for
testing or an existing application runtime.
