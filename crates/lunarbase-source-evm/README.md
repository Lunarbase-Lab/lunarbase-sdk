# `lunarbase-pmm-v2-source-evm`

EVM HTTP and WebSocket data source for `lunarbase-pmm-v2-client`.

Status: **fully supported** for standard EVM networks and Base Flashblocks.

## Install

```toml
[dependencies]
lunarbase-client = { package = "lunarbase-pmm-v2-client", version = "0.4.1" }
lunarbase-source-evm = { package = "lunarbase-pmm-v2-source-evm", version = "0.4.1" }
```

## Use

```rust
use std::sync::Arc;

use lunarbase_client::prelude::ConnectedQuoteClient;
use lunarbase_source_evm::prelude::{EvmRpcSource, RpcHttpClient};

let source = Arc::new(EvmRpcSource::base_flashblocks(
    RpcHttpClient::new(http_rpc_url)?,
    realtime_url,
    config.deployment.chain_id,
));
let client = ConnectedQuoteClient::connect(config, source, optional_checkpoint).await?;
let quote = client.quote(&request)?;
client.shutdown().await;
```

For Base, use `wss://mainnet-preconf.base.org` or
`wss://sepolia-preconf.base.org` as the application-facing WebSocket
endpoint. These endpoints provide `pendingLogs` and progressive `newHeads`;
see the [Base Flashblocks API](https://docs.base.org/base-chain/api-reference/flashblocks-api/flashblocks-api-overview).

Use `EvmRpcSource::new` for standard EVM `logs` and `newHeads` streams.

## Guarantees

- HTTP RPC provides bootstrap, backfill, and canonical recovery.
- WebSocket updates are normalized into ordered client events.
- The configured chain ID binds cursors and checkpoints to one deployment.
- WebSocket frames, handshake prefetch, reordering, HTTP bodies, and normalized
  backfills have independent count+byte budgets and fail closed on overflow.
- Canonical backfill starts with 1,000-block pages and bisects only a page that
  the provider or local HTTP response limit rejects.
