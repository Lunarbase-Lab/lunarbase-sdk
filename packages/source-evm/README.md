# `@lunarbase/source-evm`

Generic EVM HTTP/WS implementation of the common `ChainDataSource` interface.
Base Flashblocks is a configured profile of this source, not a separate client.

## Install

```bash
pnpm add @lunarbase/math @lunarbase/client @lunarbase/source-evm
```

## Connect

```ts
import { connect } from "@lunarbase/client";
import { createBaseFlashblocksSource } from "@lunarbase/source-evm";

const source = createBaseFlashblocksSource({
  httpRpcUrl,
  realtimeUrl,
  chainId: config.deployment.chainId,
});
const client = await connect(config, source, optionalCheckpoint);
const quote = client.quote(request);
await client.shutdown();
```

HTTP RPC is used for bootstrap and canonical recovery. The configured
Flashblocks WebSocket endpoint supplies realtime pending logs and progressive
heads. Transport dependencies can be injected through
`BaseFlashblocksOptions` for tests or an existing application runtime. Use
`EvmRpcSource` directly for standard EVM `logs + newHeads` streams.
