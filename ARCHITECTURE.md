# Project structure

The repository is split into a pure quote kernel and a stateful client. The
same boundaries exist in Rust and TypeScript so that the two implementations
can be checked against the same vectors and wire formats.

```text
.
├── abi/                         # pinned Core ABI
├── fixtures/                    # cross-language quote vectors
├── crates/
│   ├── lunarbase-math/src/
│   │   ├── types.rs             # U256, addresses, public state/request types
│   │   ├── arithmetic.rs        # checked arithmetic and full-width mulDiv
│   │   ├── slot0.rs              # raw Lane.slot0 masks and boundary codec
│   │   ├── fees.rs               # lane, router, spread and split fees
│   │   ├── quote.rs              # direct/route exact-in and exact-out engine
│   │   ├── state.rs              # quote state and context
│   │   └── lib.rs                # small public facade
│   ├── lunarbase-client/src/
│   │   ├── model.rs              # cursors, updates, deployment and errors
│   │   ├── sources.rs            # Base/Monad/Arbitrum normalization
│   │   ├── bootstrap.rs          # snapshot handoff and bounded buffering
│   │   ├── abi.rs                # pinned Core event decoder
│   │   ├── reducer.rs             # ordered single-writer state transitions
│   │   ├── codec.rs               # checkpoint/update binary wire format
│   │   ├── persistence.rs         # Redis and in-memory stores
│   │   ├── indexer.rs              # lifecycle, freshness and quote facade
│   │   └── lib.rs                 # client facade/re-exports
│   └── lunarbase-monad-sidecar/  # normalized execution-event boundary
├── packages/
│   ├── math/src/                 # bigint counterpart of lunarbase-math
│   └── client/src/                # TypeScript counterpart of lunarbase-client
└── SPECIFICATION.md              # protocol and acceptance requirements
```

## Dependency direction

`math` has no network, Redis, clock, async runtime, or filesystem dependency.
`client` depends on `math` and owns all mutable state, source adapters,
recovery, persistence, and freshness metadata. The source adapters emit only
normalized `ChainUpdate` values; the reducer never knows which chain or RPC
transport produced them. The indexer is the only layer that combines source,
ABI, reducer, persistence, and quote operations.

The public `lib.rs`/`index.ts` files are intentionally facades. New code
belongs in the narrowest module above; cross-module imports should use public
types and should not recreate quote arithmetic in the client layer.

## Representation rules

`Lane.slot0` remains one raw 256-bit word in hot state. Field access uses masks
and shifts; `LaneSlot0` is a boundary/debug view only. Storage-width fields
keep their natural widths (`u8`/`uint8` and `u32`/`uint32`) in checkpoints.
Maps are serialized in deterministic key order in TypeScript so Rust and
TypeScript checkpoints have the same bytes.

The binary codec is versioned (`LBQ1`) and is the only persistence wire
format. Any schema or math change must update the compatibility version,
golden vectors, and both codec implementations together.
