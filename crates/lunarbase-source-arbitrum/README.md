# `lunarbase-pmm-v2-source-arbitrum`

Arbitrum Nitro data source for `lunarbase-pmm-v2-client`.

Status: **maintenance**. Updates focus on compatibility, reliability, and security fixes.

## Install

```toml
[dependencies]
lunarbase-client = { package = "lunarbase-pmm-v2-client", version = "0.3.0" }
lunarbase-source-arbitrum = { package = "lunarbase-pmm-v2-source-arbitrum", version = "0.3.0" }
```

## Use

```rust
use std::sync::Arc;

use lunarbase_client::prelude::ConnectedQuoteClient;
use lunarbase_source_arbitrum::prelude::ArbitrumNitroSource;

let source = Arc::new(ArbitrumNitroSource::from_urls(
    http_rpc_url,
    realtime_url,
    config.deployment.chain_id,
)?);
let client = ConnectedQuoteClient::connect(config, source, optional_checkpoint).await?;
```

## Behavior

- HTTP RPC provides bootstrap, backfill, and canonical recovery.
- A compatible Nitro HTTP endpoint must expose `l1BlockNumber` for every backfilled block.
- The realtime endpoint must include `l1BlockNumber` in `newHeads` notifications.
- The configured chain ID binds cursors and checkpoints to one deployment.
