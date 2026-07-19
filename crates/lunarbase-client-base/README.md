# `lunarbase-client-base`

Ready-to-embed Base adapter using the official `pendingLogs + newHeads`
realtime interface.

## Install

```toml
[dependencies]
lunarbase-client-base = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

## Connect

```rust
use lunarbase_client_base::prelude::connect_base;

let client = connect_base(config, optional_checkpoint).await?;
let quote = client.quote(&request)?;
client.shutdown().await;
```

The adapter uses HTTP RPC for bootstrap and recovery and the configured
Flashblocks WebSocket endpoint for realtime `pendingLogs` and progressive
`newHeads`. Base is the default network of `lunarbase-indexer`.
