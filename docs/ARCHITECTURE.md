# LunarBase SDK architecture

## Components

The SDK is divided into four public capabilities and one workspace service:

1. Quote math evaluates exact-input and exact-output requests without network
   access.
2. Realtime clients maintain a coherent quote state and expose synchronous
   quote operations.
3. Network sources provide snapshots, ordered updates, recovery data, and
   canonicality checks.
4. lunarbase-indexer packages the client as an HTTP service with health,
   readiness, and metrics endpoints.
5. lunarbase-event-worker independently persists raw Core logs to a durable
   Redis Stream for at-least-once downstream processing.

Rust and TypeScript implement the same compatibility profile and share the
same deterministic quote vectors.

## Data flow

```text
canonical snapshot ----+
                       v
realtime updates -> ordered state -> quote / quoteMany
                         |
                         +-> checkpoint
```

A realtime subscription is established before snapshot publication. Updates
observed during bootstrap are buffered and applied in order before readiness
is enabled. Every quote includes the cursor, execution block, implementation
code hash, and math compatibility profile used for evaluation.

quoteMany accepts at most 256 requests and evaluates the batch against one
state position.

Durable event delivery is a separate data plane:

```text
dedicated HTTP recovery + dedicated realtime source
                         |
                         v
             bounded event worker -> atomic Redis Stream + cursor
                                             |
                                             v
                                      consumer groups
```

Redis commands, event formatting, consumer backpressure, and event-worker
queues do not share quote request execution. A slow Redis instance makes the
event worker unready and eventually backpressures its own source instead of
discarding accepted events.

## Consistency and recovery

The client rejects cross-chain updates, cursor regressions, conflicting block
hashes, removed logs, gaps, incompatible implementations, malformed events,
and invalid state transitions.

Readiness is disabled whenever state continuity is uncertain. Recovery builds
a canonical snapshot and replays buffered updates before quotes resume.
Checkpoints are accepted only when their schema, deployment identity,
configuration, state invariants, and canonical block still match. Redis is
optional restart acceleration and must be protected as service data.

## Network sources

- EVM covers standard JSON-RPC and WebSocket providers. The Base profile
  supports progressive block delivery.
- Monad supports parser WebSocket delivery; Rust deployments may also select
  the colocated Linux adapter.
- Arbitrum resolves Nitro execution context while retaining the L2 ordering
  position.

Applications combine the common client with exactly one source implementation.

## Deployment

Each indexer replica maintains its own state and serves only while ready.
Multiple replicas can run behind a normal load balancer. Deployment identity,
RPC endpoints, implementation address, code hash, and source-specific settings
are supplied by the operator.

Graceful shutdown stops quote traffic, joins background work, and writes a
final checkpoint when checkpointing is configured.

The event worker has its own readiness and resource limits. Its Redis Stream,
stable event-ID registry, and monotonic cursor are updated atomically. Redis
must use AOF with `appendfsync always`; downstream processors acknowledge
consumer-group entries only after idempotent side effects commit.

## Verification

The release gate covers:

- Rust and TypeScript formatting, linting, compilation, documentation, and
  unit tests
- deterministic cross-language quote vectors
- source ordering, bootstrap, recovery, checkpoint, and process-level tests
- event-worker/Redis crash replay, duplicate delivery, consumer reclaim, queue
  saturation, EVM reorg, and Monad competing-proposal/parser-gap tests
- same-runner quote throughput, p99, peak RSS, allocation, and mixed quote/event
  no-regression comparisons
- Base, Monad, Arbitrum, and Linux adapter feature builds
- dependency policy and production npm advisory checks
- exact crate and npm package contents
- workflow syntax, repository hygiene, and Docker image construction
