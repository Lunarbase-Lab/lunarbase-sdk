<div align="center">
  <p><strong>◐ LUNARBASE</strong></p>
  <p>
    <a href="https://github.com/Lunarbase-Lab/lunarbase-sdk">Repository</a>
    ·
    <a href="https://spdx.org/licenses/MIT.html">MIT</a>
    ·
    <a href="https://spdx.org/licenses/Apache-2.0.html">Apache-2.0</a>
  </p>
</div>

# LunarBase SDK

Repository: `lunarbase-sdk`

## About

LunarBase SDK is the integration monorepository for bit-exact off-chain quote
math, embeddable realtime clients, network data sources, and the
production-oriented Rust indexer.

The hot path is intentionally small:

```text
realtime stream → normalize → ordered reducer → in-memory state → quote/quoteMany
```

RPC and optional Redis checkpointing are limited to bootstrap and canonical
recovery. Quote calculation performs no RPC, Redis access, state serialization,
or full-state clone.

The current SDK version is `0.3.0`. Its math compatibility baseline is
`lunarbase-contracts@4bbf4d4666ac29412d7fbd946fd7a0fba8f9ac6d:math-v4`.
Canonical Solidity/Rust/TypeScript differential tests live in
`lunarbase-contracts`.

## Packages

| Layer                      | Rust                                                                      | TypeScript                                                         | Status              |
| -------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------- |
| Pure quote math            | [`lunarbase-math`](crates/lunarbase-math/README.md)                       | [`@lunarbase/math`](packages/math/README.md)                       | parity-gated        |
| Common reducer and runtime | [`lunarbase-client`](crates/lunarbase-client/README.md)                   | [`@lunarbase/client`](packages/client/README.md)                   | integration-ready   |
| Generic EVM + Base profile | [`lunarbase-source-evm`](crates/lunarbase-source-evm/README.md)           | [`@lunarbase/source-evm`](packages/source-evm/README.md)           | release candidate   |
| Monad sources              | [`lunarbase-source-monad`](crates/lunarbase-source-monad/README.md)       | [`@lunarbase/source-monad`](packages/source-monad/README.md)       | experimental        |
| Arbitrum Nitro source      | [`lunarbase-source-arbitrum`](crates/lunarbase-source-arbitrum/README.md) | [`@lunarbase/source-arbitrum`](packages/source-arbitrum/README.md) | experimental        |
| Runnable indexer           | [`lunarbase-indexer`](crates/lunarbase-indexer/README.md)                 | —                                                                  | Base is the default |
| Validation tooling         | [`lunarbase-tools`](crates/lunarbase-tools/README.md)                     | —                                                                  | internal            |

There are no per-network client wrappers or aggregate facade packages.
Integrators depend only on the pure math, the common client, and the source
implementation they use. Monad and Arbitrum packages must remain behind an
explicit experimental gate until their node-level live validation is
complete.

## Who this repository is for

- LunarBase partners embedding realtime off-chain quoting into an existing
  Rust or TypeScript service.
- Integrators that need a ready-to-run HTTP indexer with health, readiness, and
  Prometheus endpoints.
- Source authors connecting a new ordered data stream to the common
  reducer.
- LunarBase maintainers verifying bit-for-bit parity against the pinned
  Solidity implementation.

The repository is not a generic EVM indexer. Its state model, event reducer,
fee profile, and recovery rules are specific to LunarBase quoting.

## Quick installation

### Workspace

Prerequisites: stable Rust 1.97+, Node.js 22+, Corepack or pnpm, and Foundry for the
cross-language FFI suite.

```bash
git clone git@github.com:Lunarbase-Lab/lunarbase-sdk.git
cd lunarbase-sdk
corepack pnpm install --frozen-lockfile
make verify
```

Use `make help` for focused build, test, process E2E, load, FFI, Docker, and
Monad validation commands.

### Rust libraries

Until crates are published, pin the repository revision and select only the
packages needed by the application:

```toml
[dependencies]
lunarbase-math = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
lunarbase-client = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
lunarbase-source-evm = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

See the linked crate README files above for constructors, features, and
runtime guarantees.

### TypeScript libraries

After the `0.3.0` packages are published to the configured npm registry:

```bash
pnpm add @lunarbase/math @lunarbase/client @lunarbase/source-evm
```

Use `@lunarbase/source-monad` or `@lunarbase/source-arbitrum` only for
experimental validation. Package-specific imports and examples are documented
in each package README.

### Runnable client examples

The [`examples`](examples/README.md) directory contains equivalent Rust and
TypeScript realtime quote loggers. After creating the language-specific
`.env`, run `make quote-logger-rust` or `make quote-logger-ts`.

### Runnable indexer

Supply deployment identity and endpoints with CLI flags or `LUNARBASE_*`
environment variables, then run:

```bash
make run
```

An optional operator-owned TOML can provide a base layer. Checked-in profiles
exist only under [`examples/indexer`](examples/indexer/README.md) as runnable
examples and live-test fixtures.

Base is the default feature. The service exposes `POST /v1/quote`,
`POST /v1/quotes`, `GET /healthz`, `GET /readyz`, and `GET /metrics`. See the
[`lunarbase-indexer` guide](crates/lunarbase-indexer/README.md) and
[`PRODUCTION_RUNBOOK.md`](docs/PRODUCTION_RUNBOOK.md) before deployment.

---

<div align="center">
  <p>
    <a href="docs/ARCHITECTURE.md">Architecture</a>
    ·
    <a href="docs/SPECIFICATION.md">Specification</a>
    ·
    <a href="docs/PRODUCTION_RUNBOOK.md">Production runbook</a>
  </p>
  <p>
    Licensed under
    <a href="https://spdx.org/licenses/MIT.html">MIT</a>
    or
    <a href="https://spdx.org/licenses/Apache-2.0.html">Apache-2.0</a>.
  </p>
  <p>© LunarBase Lab</p>
</div>
