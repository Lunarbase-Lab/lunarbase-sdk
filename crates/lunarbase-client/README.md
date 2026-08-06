# `lunarbase-pmm-v2-client`

Realtime Rust client for LunarBase quotes.

Status: **fully supported**.

## Install

```toml
[dependencies]
lunarbase-client = { package = "lunarbase-pmm-v2-client", version = "0.3.1" }
```

Install one network source crate alongside the client.

## Use

```rust
use lunarbase_client::prelude::ConnectedQuoteClient;

let client = ConnectedQuoteClient::connect(config, source, checkpoint).await?;
let quote = client.quote(&request)?;
let quotes = client.quote_many(&requests)?;
let health = client.health();
client.shutdown().await;
```

## Guarantees

- Quote calls read one coherent in-memory state snapshot.
- `ChainDataSource` covers bootstrap, backfill, ordered updates, and checkpoint validation.
- Gaps and canonical mismatches suspend readiness until recovery completes.
