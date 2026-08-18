# `lunarbase-pmm-v2-source-monad`

Monad execution-event data source for `lunarbase-pmm-v2-client`.

Status: **maintenance**. Updates focus on compatibility, reliability, and security fixes.

## Availability

This crate is built and tested as part of the SDK workspace but is not
published by the crates.io release workflow. Protocol v2 depends on pinned
upstream Git packages that crates.io cannot accept. Rust Monad deployments
must pin the SDK repository or build the workspace applications. The TypeScript
Monad compatibility adapter is also workspace-only and is not published to npm.

## Workspace use

```toml
[dependencies]
lunarbase-client = { package = "lunarbase-pmm-v2-client", path = "../lunarbase-client" }
lunarbase-source-monad = { package = "lunarbase-pmm-v2-source-monad", path = "../lunarbase-source-monad", features = ["protocol-v2"] }
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
        ..MonadParserConfig::durable_v2()
    },
    http_rpc_url,
)?);
let client = ConnectedQuoteClient::connect(config, source, optional_checkpoint).await?;
```

Build with `--features protocol-v2` on Linux and point `ws_url` at the parser's
`/ws/v2` endpoint. `LegacyV1` and `/ws/subscriptions` remain available for
portable consumers that have not migrated yet.

## Behavior

- HTTP RPC provides bootstrap, backfill, and canonical recovery.
- Protocol v2 verifies chain and durable stream identity before subscription.
- The first connection starts at the retained tail while HTTP builds the
  authoritative snapshot. Reconnects resume from the last parser-confirmed ACK.
- Raw execution descriptors are decoded against the pinned official Monad ABI.
  Irrelevant transaction payloads are not materialized on the lifecycle path.
- `Realtime`, `BlockOrdered`, and `Finalized` select when logs become visible.
  Competing proposals produce explicit reorg updates only if their branch was
  already published.
- `emit_removed_logs` is enabled by the standalone event worker: published Core
  logs remain under the same count+byte budgets until finality, allowing an
  abandoned branch to emit removals before its reorg. The quote indexer leaves
  it disabled, so realtime quoting retains no log payloads.
- Proposal, matching-log, matching-payload, frame, and prefetch bounds fail
  closed into canonical recovery; required records are never silently dropped.
- Parser gaps, stream replacement, rejected proposals, ABI mismatch, and
  non-contiguous sequences make the client unready until recovery completes.

The optional `protocol-v2` feature uses `monad-event-ring` and
`monad-exec-events`, licensed under GPL-3.0-or-later. Account for those terms
when distributing binaries built with this feature.
