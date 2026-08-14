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

## Lifecycle delivery

`Realtime` keeps the low-allocation descriptor path used by the quote service.
`BlockOrdered` and `Finalized` pass native descriptors through Monad's official
`CommitStateBlockBuilder`, preserve proposal identity, and surface abandoned
published branches as removals followed by reorgs when `emit_removed_logs` is
enabled. The standalone event worker enables that policy; the quote service
keeps the allocation-light realtime path. Core address, topic filter, chain id,
and integer positions are validated before publication; any descriptor gap or
invalid lifecycle fails closed into canonical recovery.

## Licensing

The optional `native-event-ring` feature depends on `monad-event-ring` and
`monad-exec-events`, licensed under GPL-3.0-or-later. Account for those terms
when distributing binaries built with this feature.
