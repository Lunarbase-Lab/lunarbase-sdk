# `lunarbase-pmm-v2-source-monad`

Monad execution-event data source for `lunarbase-pmm-v2-client`.

Status: **maintenance**. Updates focus on compatibility, reliability, and security fixes.

## Install

```toml
[dependencies]
lunarbase-client = { package = "lunarbase-pmm-v2-client", version = "0.3.0" }
lunarbase-source-monad = { package = "lunarbase-pmm-v2-source-monad", version = "0.3.0" }
```

## Use

```rust
use std::sync::Arc;

use lunarbase_client::prelude::ConnectedQuoteClient;
use lunarbase_source_monad::prelude::{MonadParserConfig, MonadParserSource};

let source = Arc::new(MonadParserSource::new(
    MonadParserConfig {
        ws_url: realtime_url,
        core: config.deployment.core,
        chain_id: config.deployment.chain_id,
        ..Default::default()
    },
    http_rpc_url,
)?);
let client = ConnectedQuoteClient::connect(config, source, optional_checkpoint).await?;
```

`ws_url` must point to a compatible Monad parser WebSocket endpoint.

## Behavior

- HTTP RPC provides bootstrap, backfill, and canonical recovery.
- Parser sequence positions are retained in client updates.
- Gaps and reconnects require canonical recovery before readiness.
