# LunarBase SDK specification

This document defines the observable behavior shared by the Rust and
TypeScript SDKs.

## Compatibility

The current math compatibility profile is lunarbase-pmm-v2. A client accepts
state only when the configured profile, implementation address, and
implementation runtime-code hash match the deployment.

All token quantities are unsigned 256-bit integers. HTTP quantities are
decimal strings. EVM addresses contain exactly 20 bytes and hashes contain
exactly 32 bytes.

## Deployment identity

A runtime is configured for one network, chain ID, Core address, router,
expected router whitelist status, deployment block, implementation address,
implementation code hash, and optional explicit lane set.

Updates from another chain or Core are rejected. Configuration changes require
a new bootstrap.

## Source contract

A data source provides:

- a coherent canonical snapshot
- canonical logs for an inclusive block range
- an acknowledged realtime stream
- the latest canonical head
- checkpoint canonicality validation

Snapshot reads use one block reference. Realtime updates are normalized as a
head, log, reorganization, or gap. The source preserves provider order and
includes an EVM execution block for block-dependent quote evaluation.

## Cursor

Every applied state position has:

- chain ID
- source block number
- EVM execution block number
- commitment: realtime, canonical, or finalized
- block hash when supplied by the source
- transaction and log positions when applicable
- source sequence positions when required by the transport

Cursors never move backward. Conflicting hashes at one block, removed logs,
reorganizations, gaps, and missing ordering data revoke readiness and start
canonical recovery.

## Runtime lifecycle

Bootstrap opens the realtime stream before publishing a canonical snapshot.
Updates received during snapshot construction are buffered and applied in
order. Readiness becomes true only after configuration validation, snapshot
validation, and buffered replay succeed.

Quotes are served only while ready. Recovery and graceful shutdown revoke
readiness before changing state.

## Quote request

A quote request contains:

- assetIn: input ERC-20 address
- assetOut: output ERC-20 address
- amount: fixed input or output quantity
- mode: ExactIn or ExactOut

The deployment router and freshness policy are runtime configuration and
cannot be overridden by a quote request.

## Quote outcome

An available result contains amountIn, amountOut, feeAsset, feeAmount,
partnerFee, and treasuryFee.

An unavailable result contains one deterministic reason:

- zeroAmount
- equalAssets
- missingLane
- pausedLane
- staleLane
- zeroPrincipal
- zeroAnchor
- spreadConsumesAnchor
- insufficientOutputReserve

Unavailable market state is a normal quote outcome. Invalid input, arithmetic
failure, incompatible state, or a non-ready runtime is an error.

Every client quote also returns the exact cursor, execution block,
implementation code hash, and compatibility profile used. quoteMany accepts
at most 256 requests and returns results from one shared state position.

## HTTP API

> **Note:** The HTTP API is under development and may change before it is
> declared stable.

POST /v1/quote accepts one request:

```json
{
  "assetIn": "0x0000000000000000000000000000000000000001",
  "assetOut": "0x0000000000000000000000000000000000000002",
  "amount": "1000000",
  "mode": "exactIn"
}
```

POST /v1/quotes accepts either an array of requests or an object with a
requests array. Unknown request fields are rejected.

A successful response contains result or results plus cursor,
executionBlockNumber, implementationCodeHash, and mathCompatibilityVersion.
Invalid input returns 400. A non-ready indexer returns 503. Health, readiness,
and Prometheus metrics are available at /healthz, /readyz, and /metrics.

## Checkpoints

Checkpoints are restart data, not a public interchange format. A checkpoint
is used only after schema, compatibility, deployment configuration, structural
invariants, and canonical block validation succeed. Any failure falls back to
a canonical snapshot.

Checkpoint storage must be access-controlled. Storage failure may slow a
restart but does not change the state of a running ready process.

## Guarantees

- Quote math performs no network or persistence access.
- Rust and TypeScript use the same deterministic compatibility vectors.
- A successful batch is evaluated from one state position.
- The runtime fails closed when source continuity or deployment identity is
  uncertain.
- Readiness describes quote availability; liveness describes only the process.
- Package release gates validate exact registry contents before publication.
