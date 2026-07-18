# `lunarbase-client-core`

Network-independent LunarBase reducer and embeddable realtime client runtime.

## Install

```toml
[dependencies]
lunarbase-client-core = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

Applications normally install this crate together with one network adapter.

## Runtime model

`ChainDataSource` owns snapshot, backfill, realtime subscription, and
checkpoint validation. `ConnectedQuoteClient` owns ordering, fail-closed
recovery, compact in-memory state, and quote access.

```rust
let client = ConnectedQuoteClient::connect(config, source, checkpoint).await?;
let single = client.quote(&request)?;
let batch = client.quote_many(&requests)?;
let health = client.health();
let checkpoint = client.checkpoint();
client.shutdown().await;
```

`quote_many` evaluates the full batch under one shared state snapshot. Quote
methods do not call the data source and do not perform RPC or persistence I/O.

## Implementing a source

Implement `ChainDataSource` for a transport that can:

1. produce a canonical bootstrap snapshot;
2. backfill an explicit cursor range;
3. stream ordered normalized updates;
4. validate a checkpoint against the canonical chain.

Prefer an existing Base, Monad, or Arbitrum package when possible.
