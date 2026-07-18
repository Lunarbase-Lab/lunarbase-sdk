# `@lunarbase/client-monad`

Experimental portable Monad execution-events client.

## Install

```bash
pnpm add @lunarbase/math @lunarbase/client-core @lunarbase/client-monad
```

## Connect

```ts
import { connectMonad } from "@lunarbase/client-monad";

const client = await connectMonad(config, optionalCheckpoint);
const quote = client.quote(request);
await client.shutdown();
```

TypeScript consumes the parser WebSocket and uses HTTP RPC for bootstrap and
canonical recovery. Native event-ring access is intentionally Rust-only.

Keep this package behind an experimental gate until sequencing, commitment,
gap recovery, reconnect, and soak behavior have been validated against a live
Monad node.
