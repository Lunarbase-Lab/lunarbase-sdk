# LunarBase off-chain quoting

This repository implements the quote model pinned to contract commit
`24db47b866e8150a0d91cffd80efe49df85179b5`.

The module map and dependency boundaries are documented in
[`ARCHITECTURE.md`](ARCHITECTURE.md).

The pure packages are deliberately independent from RPC, Redis, clocks,
workers, and filesystem state:

- `crates/lunarbase-math` uses `ruint::U256`/`U512` and exposes exact Solidity
  rounding, slot0 codecs, direct/route quotes, fee adjustment and fee split.
- `packages/math` is the equivalent `bigint` implementation. No monetary path
  accepts a JavaScript `number`.
- `crates/lunarbase-client` and `packages/client` own normalized source
  boundaries, ordered reducers, immutable snapshots, checkpoint namespaces,
  gap handling, and bounded in-memory Redis-compatible stores.
- `crates/lunarbase-monad-sidecar` is the safe normalized boundary for a
  colocated Monad execution-events reader.

`LaneState.slot0` is kept as the canonical `ruint::U256`/TypeScript `bigint`
word. `LaneSlot0` is a decode/encode view used only at boundaries; quote-path
accessors mask the four fields they need directly. This avoids allocating a
decoded struct for every lane and avoids converting a 256-bit storage word to
an array-backed bitfield representation. The checkpoint codec also stores the
ABI widths directly (`uint8` delay and `uint32` slippage K).

The implementation intentionally does not add a runtime bitfield dependency:
the relevant crates either target primitive representations up to `u128`, or
represent wider values as byte arrays. That is useful for ergonomic schemas,
but it adds a conversion/array boundary around the already-canonical 32-byte
word. Manual mask/shift accessors are constant-time, inlinable, `no_std`
compatible at the math layer, and match Solidity's layout exactly.

## Checks

```sh
cargo test --workspace
pnpm install --offline
pnpm --filter @lunarbase/math build
pnpm --filter @lunarbase/client build
node --test packages/math/dist/index.test.js
```

The source adapters intentionally receive a transport/sidecar backend. This
keeps network I/O out of the pure math library while allowing Base Flashblocks,
Monad execution-events, and executed Arbitrum Nitro logs to share one client
interface. A source gap, reorg, removed log, code-hash mismatch, or impossible
principal transition makes the snapshot unavailable until canonical recovery.

## Monad parser smoke test

The sidecar includes a real WebSocket client for the local
`monad-exec-events-parser` protocol. It subscribes to `logs` plus the parser's
`all` stream, maps `proposed/finalized/verified` to
`Realtime/Canonical/Finalized`, and converts `subscriptionGap`, expired ring,
and stalled-reader signals into a mandatory normalized `Gap`.

```sh
LUNARBASE_CORE=0x... \
LUNARBASE_MONAD_PARSER_WS=ws://127.0.0.1:8080/ws/subscriptions \
cargo run -p lunarbase-monad-sidecar --example monad-parser-smoke
```

The parser's `seqno` is global across all execution events. Filtered `logs`
therefore have sparse seqnos; the adapter only rejects regressions and
duplicates, while complete raw-ring readers may use strict contiguous gap
detection.
