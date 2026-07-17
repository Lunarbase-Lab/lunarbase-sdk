# Project structure

The repository has one pure math kernel and one universal runtime per language.
Network-specific clients are separate publishable crates/packages. Compatibility
facades preserve the original `lunarbase-client` and `@lunarbase/client`
imports.

```text
.
├── crates/
│   ├── lunarbase-math/                 # pure Rust quote math
│   ├── lunarbase-client-core/          # universal Rust runtime
│   │   └── src/
│   │       ├── model.rs                # cursors, updates, config and errors
│   │       ├── bootstrap.rs            # bounded snapshot handoff
│   │       ├── indexer.rs              # public lifecycle facade
│   │       ├── indexer/                # engine, client, tasks and checkpoints
│   │       ├── persistence.rs          # persistence facade
│   │       ├── persistence/            # Redis/in-memory stores and fencing
│   │       ├── source.rs               # common source/backend contracts
│   │       ├── execution/
│   │       │   ├── engine.rs           # ExecutionEventReader boundary
│   │       │   └── monad.rs            # universal Monad execution engine
│   │       ├── transport/
│   │       │   ├── rpc.rs              # canonical RPC facade
│   │       │   ├── rpc/                # client, backend, snapshot and codec
│   │       │   └── ws.rs               # bounded generic WebSocket source
│   │       ├── protocol/               # Core ABI and binary codecs
│   │       └── state/                  # ordering and single-writer reducer
│   ├── lunarbase-client-base/          # Flashblocks normalizer + transport
│   ├── lunarbase-client-monad/         # parser reader + canonical RPC adapter
│   ├── lunarbase-client-arbitrum/      # Nitro normalizer + transport
│   ├── lunarbase-client/               # compatibility facade
│   ├── lunarbase-indexer/              # executable composition + HTTP API
│   │   └── src/
│   │       ├── runtime.rs              # runtime facade
│   │       ├── runtime/                # handle, factory, lease and supervisor
│   │       ├── config.rs               # configuration facade
│   │       ├── config/                 # types, validation and parsing
│   │       ├── metrics.rs              # Prometheus counters/histograms
│   │       ├── alerts.rs               # webhook + panic supervision
│   │       └── api/                    # health, metrics, quote HTTP API
│   └── lunarbase-tools/                # E2E, load, Monad live validation
├── packages/
│   ├── math/                           # pure TypeScript bigint quote math
│   ├── client-core/                    # universal TypeScript runtime
│   │   └── src/
│   │       ├── model.ts
│   │       ├── bootstrap.ts
│   │       ├── indexer.ts              # public lifecycle exports
│   │       ├── indexer/                # engine and connected client
│   │       ├── persistence.ts
│   │       ├── source.ts
│   │       ├── execution/              # reader contract + Monad engine
│   │       ├── transport/              # generic RPC/WebSocket
│   │       ├── protocol/
│   │       └── state/
│   ├── client-base/                    # Base client
│   ├── client-monad/                   # Monad parser client
│   ├── client-arbitrum/                # Arbitrum client
│   └── client/                         # compatibility facade
├── abi/                                # pinned Core ABI
├── fixtures/                           # cross-language vectors/replays
├── schemas/                            # stable wire schemas
├── config/                             # network and production templates
└── solidity-reference/                 # Foundry differential reference
```

Source files under `crates/`, `packages/`, and `scripts/` are limited to 500
lines. `make source-size-check` enforces this boundary in `make verify`, so a
growing context must be split by responsibility before it becomes difficult
to review.

## Dependency direction

The dependency graph is intentionally one-way:

```text
math
  ↑
client-core
  ↑
client-base  client-monad  client-arbitrum
  ↑              ↑              ↑
  └──────── client facade ───────┘
  └────── lunarbase-indexer ─────┘
```

`client-core` owns mutable runtime behavior but no provider-specific payload
schema. Network packages depend on its public contracts and emit normalized
`ChainUpdate` values. The compatibility facade only re-exports packages and
must not contain runtime logic. The executable selects network packages through
Cargo features; `base` is the default, while Monad and Arbitrum are opt-in.

The Monad execution engine belongs to `client-core`. It accepts an
`ExecutionEventReader`, so the parser WebSocket implementation can later be
replaced by a native hugetlbfs/event-ring reader without changing reducer,
recovery, persistence, or high-level client APIs.

## Runtime lifecycle and observability

`ConnectedQuoteClient` owns a bounded cancellation channel, source/reducer
task handles, and a bounded broadcast channel of operational events. Startup
uses an abort-on-drop guard because realtime ingestion begins before the RPC
snapshot; cancelling bootstrap therefore cannot leak a detached source task.

The executable consumes runtime events in alert and metrics supervisors. Readiness
monitoring and optional webhook delivery stay outside `client-core`, keeping
the publishable runtime independent from HTTP alert vendors. The core only
emits stable failure codes and remains fail-closed by marking reducer state
unavailable.

`lunarbase-indexer` places a `RuntimeHandle` between HTTP and the connected
client. With Redis persistence, an owner-checked expiring lease elects exactly
one active writer. Standbys keep liveness and metrics online but hold no
network/reducer client. Lease renewal failure atomically removes the client
from the HTTP handle before cleanup, so quotes fail closed. Runtime counters
from retired client instances are accumulated across lease loss/reacquisition.

On shutdown, HTTP draining and writer teardown begin together after the active
client is removed from availability. The runtime cooperatively cancels and
joins its workers, persists a final checkpoint, releases its owner-checked
lease, and uses task abort only after the configured deadline. Process panics
are forwarded through a non-blocking channel to the same alert sink while
preserving Rust's previous panic hook. Synchronous Redis commits and lease
operations are isolated on the blocking pool and the underlying socket has
explicit connect/read/write timeouts.

## Representation rules

`Lane.slot0` remains one raw 256-bit word in hot state. Field access uses masks
and shifts; `LaneSlot0` is a boundary/debug view only. Storage-width fields
retain their ABI widths in checkpoints.

The binary codec is versioned (`LBQ1`) and is the only persistence wire format.
Schema or math changes must update the compatibility version, golden vectors,
and Rust/TypeScript implementations together.
