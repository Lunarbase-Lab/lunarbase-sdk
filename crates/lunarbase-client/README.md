# `lunarbase-pmm-v2-client`

Realtime Rust client for LunarBase quotes.

Status: **fully supported**.

## Install

```toml
[dependencies]
lunarbase-client = { package = "lunarbase-pmm-v2-client", version = "0.4.0" }
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
- The source/reducer handoff is bounded by both update count and retained bytes;
  overflow fails closed into canonical recovery.
- `connect` creates no event-delivery queue. The explicitly enabled event
  observer is best-effort and nonblocking; use `lunarbase-event-worker` for
  durable logs.
