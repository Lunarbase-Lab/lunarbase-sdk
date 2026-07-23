# `@lunarbase/source-monad`

Experimental portable Monad execution-events source.

## Install

```bash
pnpm add @lunarbase/math @lunarbase/client @lunarbase/source-monad
```

## Connect

```ts
import { connect } from "@lunarbase/client";
import { createMonadParserSource } from "@lunarbase/source-monad";

const source = createMonadParserSource({
  httpRpcUrl,
  realtimeUrl,
  chainId: config.deployment.chainId,
});
const client = await connect(config, source, optionalCheckpoint);
const quote = client.quote(request);
await client.shutdown();
```

TypeScript consumes the parser WebSocket and uses HTTP RPC for bootstrap and
canonical recovery. Native event-ring access is intentionally Rust-only.

Keep this package behind an experimental gate until sequencing, commitment,
gap recovery, reconnect, and soak behavior have been validated against a live
Monad node.
