# `@lunarbase/client`

Universal TypeScript reducer and embeddable realtime LunarBase client.

## Install

```bash
pnpm add @lunarbase/math @lunarbase/client
```

Applications normally add one network package as well.

## Connect a source

```ts
import { connect } from "@lunarbase/client";

const client = await connect(config, dataSource, optionalCheckpoint);
const single = client.quote(request);
const batch = client.quoteMany(requests);
const health = client.health();
const checkpoint = client.checkpoint();
await client.shutdown();
```

Implement `ChainDataSource` only for a custom transport. It combines snapshot,
backfill, realtime subscription, and checkpoint validation.

`quote` and `quoteMany` are synchronous in-memory operations. A batch is
evaluated in one event-loop turn against one state snapshot. The client does
not expose mutable state maps and does not depend on Redis.
`DeploymentConfig` is transport-free; endpoint configuration belongs to the
injected source.
