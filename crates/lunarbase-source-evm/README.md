# `lunarbase-source-evm`

Generic EVM HTTP/WS implementation of `lunarbase_client::source::ChainDataSource`.
Base Flashblocks is a configured profile of this source, not a separate client.

## Install

```toml
[dependencies]
lunarbase-client = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
lunarbase-source-evm = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

## Base Flashblocks

```rust
use std::sync::Arc;
use lunarbase_client::prelude::ConnectedQuoteClient;
use lunarbase_source_evm::prelude::{EvmRpcSource, RpcHttpClient};

let rpc = RpcHttpClient::new(http_rpc_url)?;
let source = Arc::new(EvmRpcSource::base_flashblocks(
    rpc,
    realtime_url,
    config.deployment.chain_id,
));
let client = ConnectedQuoteClient::connect(config, source, optional_checkpoint).await?;
let quote = client.quote(&request)?;
client.shutdown().await;
```

The source uses HTTP RPC for bootstrap and recovery and the configured
Flashblocks WebSocket endpoint for realtime `pendingLogs` and progressive
`newHeads`. Use `EvmRpcSource::new` or `EvmRpcSource::with_config` for standard
EVM `logs + newHeads` streams. Base is the default network of
`lunarbase-indexer`.
