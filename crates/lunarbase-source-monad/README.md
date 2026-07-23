# `lunarbase-source-monad`

Experimental Monad execution-events implementations of the common
`ChainDataSource` contract.

## Portable parser client

```toml
[dependencies]
lunarbase-client = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
lunarbase-source-monad = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

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

The portable implementation consumes the parser WebSocket and uses RPC for
bootstrap and canonical recovery.

## Native event ring

On Linux, enable the colocated shared-memory reader:

```toml
lunarbase-source-monad = {
  git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git",
  rev = "<approved-revision>",
  features = ["native-event-ring"]
}
```

Then construct `MonadEventRingSource` from `lunarbase_source_monad::prelude`
and pass it to `ConnectedQuoteClient::connect`. The native feature depends on
the official Monad execution-events crates and is intended to run beside a
Monad node.

This package remains experimental until parser and native-ring sequencing,
gap recovery, commitment transitions, and long-running node soak tests pass.
