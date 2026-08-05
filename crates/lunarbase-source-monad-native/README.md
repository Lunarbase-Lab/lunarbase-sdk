# `lunarbase-source-monad-native`

Linux Monad execution-event adapter for `lunarbase-indexer`.

Status: **maintenance**. Updates focus on compatibility, reliability, and security fixes.

## Use

```bash
cargo build --locked -p lunarbase-indexer --no-default-features --features monad-native
```

## Requirements

- Linux on a supported architecture.
- Access to a Monad execution-event source.
- An HTTP RPC endpoint for bootstrap and canonical recovery.

## Licensing

The optional `native-event-ring` feature depends on `monad-event-ring` and
`monad-exec-events`, licensed under GPL-3.0-or-later. Account for those terms
when distributing binaries built with this feature.
