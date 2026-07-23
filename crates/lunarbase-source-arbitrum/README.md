# `lunarbase-source-arbitrum`

Experimental Arbitrum Nitro implementation of the common `ChainDataSource`
contract.

## Install and connect

```toml
[dependencies]
lunarbase-client = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
lunarbase-source-arbitrum = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

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

The source consumes executed logs and keeps the EVM execution-block context
separate from stream ordering metadata.

Do not mark this package production-ready until execution `block.number`
semantics, reconnect behavior, and canonical recovery have been validated
against a real Nitro node.
