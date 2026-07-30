# LunarBase SDK architecture

## System boundary

The repository has three layers:

```text
pure math
  └─ no async, RPC, persistence, or network semantics

embeddable clients
  ├─ one common ordered reducer/runtime
  ├─ generic EVM HTTP/WS source
  └─ network-specific sources only where stream semantics differ

lunarbase-indexer
  └─ HTTP + metrics + optional Redis restart checkpoint
```

The Rust and TypeScript math libraries mirror the pinned Solidity behavior.
The common client libraries own state transitions but not persistence. The
indexer is the only ready-to-run service.

## Repository layout

```text
crates/
  lunarbase-math/
  lunarbase-client/
  lunarbase-source-evm/
  lunarbase-source-monad/
  lunarbase-source-arbitrum/
  lunarbase-indexer/
  lunarbase-tools/

packages/
  math/
  client/
  source-evm/
  source-monad/
  source-arbitrum/

fixtures/
  quote-vectors.json

examples/
  indexer/
    config/
      base.toml
      monad.toml
      arbitrum.toml
    docker-compose.yml
    prometheus-alerts.yml
```

There are no per-network client wrappers or all-network facade packages.
Applications always use the one common client and inject a concrete source.
This prevents a Base-only integration from pulling Monad or Arbitrum
dependencies while keeping lifecycle and reducer behavior identical.
Indexer deployment values are supplied externally through CLI flags,
environment variables, or an operator-owned file. Checked-in profiles exist
only as examples and test fixtures.

## Data path

```text
RPC snapshot ─────────────┐
                         v
realtime source → normalize → bounded queue → ordered reducer → hot state
                                                                  │
                                                       quote / quoteMany
                                                                  │
                                                      cursor-bound result
```

Subscription begins before the initial snapshot. Updates received while RPC is
building the snapshot remain in the bounded handoff queue and are replayed in
cursor order. This closes the snapshot/subscription race.

The source exposes one contract:

- `snapshot(deployment)`
- `backfill(range)`
- `subscribe(filter)`
- `canonical_head()`
- `validate_checkpoint(checkpoint)`

Deployment identity passed to the common client contains no RPC or WebSocket
URLs. Endpoints and transport resource limits belong to the selected source;
the runnable indexer owns their service-level configuration.

Normalized updates are only `Head`, `Log`, `Reorg`, and `Gap`.
`sourceSequence` orders source messages; `executionBlockNumber` independently
records the EVM-visible block used by quote validity.

## State and quote path

Each lane is one compact object:

```text
slot0: U256
assetReserve: u128
totalPrincipalAmount: u128
```

Reserve and principal are colocated with packed `slot0`, avoiding secondary map lookups. The runtime
contains exactly one configured router and one `FeeProfile`. Whitelisted
routers use multiplier `1`; non-whitelisted deployments track the global
blacklist multiplier. Partner fee events are retained only for the configured
router.

Rust stores the reducer behind `std::sync::RwLock`. A quote takes a short shared
guard and performs no RPC, Redis access, serialization, or state clone. The
single reducer takes a short write guard. TypeScript relies on single-threaded
event-loop ordering and does not expose mutable maps.

`quoteMany` is synchronous and limited to 256 requests. It reads the cursor
once, computes every result from the same state, and returns that one cursor.

## Network sources

`lunarbase-source-evm` owns reusable HTTP snapshot/backfill and WebSocket
`logs + newHeads` behavior. Base is its `pendingLogs + progressive newHeads`
profile, not another client package. Base emits `newHeads` at Flashblock
cadence, so the source avoids decoding the larger `newFlashblocks` payload.

Monad has two Rust source implementations in the same network package:

- portable parser WebSocket for development and remote deployments;
- Linux native event-ring reader using official `monad-exec-events` and
  `monad-event-ring`.

TypeScript consumes the parser/RPC WebSocket and does not bind hugetlbfs.

Arbitrum extends the generic EVM source with Nitro execution-context
resolution and records parent-chain execution context separately from the L2
stream height.

Monad and Arbitrum are experimental until their node-based soak gates pass.

## Failure and recovery

The runtime fails closed on:

- stream gap, disconnect, or queue overflow;
- cursor regression or block-hash discontinuity;
- reorg or removed log;
- incompatible ERC-1967 implementation address or implementation code hash;
- malformed quote-critical event;
- arithmetic/state invariant failure.

Readiness is revoked before recovery starts. The source keeps buffering into a
bounded queue while a canonical RPC snapshot is built. State is published
again only after the snapshot and handoff replay both succeed.

Shutdown cancels source/reducer tasks, stores a final checkpoint when Redis is
configured, closes HTTP gracefully, and waits within the configured deadline.
Bootstrap is cancellation-safe: SIGTERM cannot leave a detached subscription.

## Horizontal scaling

Every replica independently consumes the source, maintains hot state, and
serves quotes. There is no leader, lease, fencing token, or standby role.
Throughput scales behind a normal load balancer.

Replicas may best-effort overwrite the same Redis checkpoint because the value
is a complete deployment-bound snapshot and startup always validates its
canonical block hash. Redis is restart acceleration, not inter-replica
coordination.

## Persistence

The indexer stores one no-TTL key per chain/Core/router/schema:

```text
lunarbase:v4:{chainId}:{core}:{router}
```

Only `GET` and atomic `SET` are used. A missing, malformed, incompatible,
forked, or unusable checkpoint is ignored in favor of a full RPC snapshot.
Redis outage does not affect a running indexer's readiness.

Embeddable clients expose `checkpoint()` but contain no Redis dependency.

## Verification gates

- Shared deterministic math corpus in Rust and TypeScript.
- Canonical Solidity/Rust/TypeScript FFI in `lunarbase-contracts`.
- Quote-path source-call counters.
- Same-cursor `quoteMany` parity.
- Reducer replay without `SwapExecuted`.
- Checkpoint identity/canonicality and Redis-unavailable bootstrap.
- Two simultaneously ready process replicas.
- Gap/reorg recovery and SIGTERM bootstrap shutdown.
- 10–15 lane, 50–100 pair load profile.
- Base payload fixtures.
- Separate Monad and Arbitrum live-validation gates.

All Rust and TypeScript source files are limited to 500 non-comment code lines
so documentation can remain detailed without encouraging oversized modules.
Modules retain a reviewable context boundary.
