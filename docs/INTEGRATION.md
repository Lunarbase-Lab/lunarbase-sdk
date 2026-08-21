# Integration guide

## Select packages

Every integration uses the math package and may add the realtime client plus
one network source:

| Network     | Rust source                      | npm source                              |
| ----------- | -------------------------------- | --------------------------------------- |
| EVM or Base | lunarbase-pmm-v2-source-evm      | @lunarbase-lab/pmm-v2-source-evm        |
| Monad       | Workspace/Git only               | Not published (workspace compatibility) |
| Arbitrum    | lunarbase-pmm-v2-source-arbitrum | @lunarbase-lab/pmm-v2-source-arbitrum   |

Use package version 0.4.1 consistently.

The Monad sources are not part of the registry release inventory. Rust protocol
v2 uses pinned upstream Git dependencies. The TypeScript workspace adapter
cannot access the native Monad Event Ring and does not implement protocol-v2
identity, lifecycle, ACK, or resume semantics. Pin the SDK repository or build
the workspace applications for Rust Monad deployments; do not use the
compatibility-only TypeScript adapter as a production Event Ring source.

## Configure a deployment

Provide the network, chain ID, Core address, mandatory fee class, deployment
block, implementation address, implementation runtime-code hash, and RPC
endpoints. Use explicit lane assets when the deployment inventory is already
known. A verified router is optional and only enables exact partner/treasury
allocation.

Validate these values in deployment automation. Do not accept quote-request
overrides for deployment identity or fee policy.

## Start the client

Construct the selected source, create the connected quote client, and wait for
bootstrap to finish before routing traffic. Package README files contain the
language-specific constructors.

A ready client serves synchronous quote and quoteMany calls. Each response
contains the state cursor, execution block, implementation code hash, and
compatibility profile. Retain these fields in logs when a quote is used to
build a transaction.

## Handle outcomes

Available is a complete quote. Unavailable describes deterministic market
state and should not be retried without a newer state cursor. Runtime errors
and non-readiness indicate that the service cannot currently guarantee state
continuity.

Route traffic only while readiness is true. Keep retry and timeout policy
outside the quote math layer.

## Run the HTTP indexer

For service deployments, configure lunarbase-indexer through command-line
arguments, LUNARBASE_* environment variables, or an operator-owned TOML file.
Deploy at least two independently indexing replicas and route only to ready
instances.

See [the indexer guide](../crates/lunarbase-indexer/README.md) and
[the production runbook](PRODUCTION_RUNBOOK.md).
