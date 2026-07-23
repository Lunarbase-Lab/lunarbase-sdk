# `lunarbase-client`

Network-independent LunarBase reducer and embeddable realtime client runtime.

## Install

```toml
[dependencies]
lunarbase-client = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

Applications normally install this crate together with one source package.

## Runtime model

`ChainDataSource` owns snapshot, backfill, realtime subscription, and
checkpoint validation. `ConnectedQuoteClient` owns ordering, fail-closed
recovery, compact in-memory state, and quote access.

```rust
use lunarbase_client::prelude::{ChainDataSource, ConnectedQuoteClient};

let client = ConnectedQuoteClient::connect(config, source, checkpoint).await?;
let single = client.quote(&request)?;
let batch = client.quote_many(&requests)?;
let health = client.health();
let checkpoint = client.checkpoint();
client.shutdown().await;
```

All entities are also available through explicit canonical modules, for
example `lunarbase_client::source::ChainDataSource` and
`lunarbase_client::indexer::client::ConnectedQuoteClient`.

`quote_many` evaluates the full batch under one shared state snapshot. Quote
methods do not call the data source and do not perform RPC or persistence I/O.
`DeploymentConfig` contains only deployment identity and quote policy;
transport endpoints belong to the injected source.

## Implementing a source

Implement `ChainDataSource` for a transport that can:

1. produce a canonical bootstrap snapshot;
2. backfill an explicit cursor range;
3. stream ordered normalized updates;
4. validate a checkpoint against the canonical chain.

Prefer an existing Base, Monad, or Arbitrum package when possible.
