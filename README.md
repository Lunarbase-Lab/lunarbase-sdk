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
math, embeddable realtime clients, network data-source adapters, and the
production-oriented Rust indexer.

The hot path is intentionally small:

```text
realtime stream → normalize → ordered reducer → in-memory state → quote/quoteMany
```

RPC and optional Redis checkpointing are limited to bootstrap and canonical
recovery. Quote calculation performs no RPC, Redis access, state serialization,
or full-state clone.

The current SDK version is `0.2.0`. Its math compatibility baseline is
`lunarbase-contracts@cfeb6b86f425c5207f3cf80c8b40adde07d6a60b:math-v2`.
Canonical Solidity/Rust/TypeScript differential tests live in
`lunarbase-contracts`.

## Packages

| Layer                      | Rust                                                                      | TypeScript                                                         | Status              |
| -------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------- |
| Pure quote math            | [`lunarbase-math`](crates/lunarbase-math/README.md)                       | [`@lunarbase/math`](packages/math/README.md)                       | parity-gated        |
| Common reducer and runtime | [`lunarbase-client-core`](crates/lunarbase-client-core/README.md)         | [`@lunarbase/client-core`](packages/client-core/README.md)         | integration-ready   |
| Base adapter               | [`lunarbase-client-base`](crates/lunarbase-client-base/README.md)         | [`@lunarbase/client-base`](packages/client-base/README.md)         | release candidate   |
| Monad adapter              | [`lunarbase-client-monad`](crates/lunarbase-client-monad/README.md)       | [`@lunarbase/client-monad`](packages/client-monad/README.md)       | experimental        |
| Arbitrum adapter           | [`lunarbase-client-arbitrum`](crates/lunarbase-client-arbitrum/README.md) | [`@lunarbase/client-arbitrum`](packages/client-arbitrum/README.md) | experimental        |
| Runnable indexer           | [`lunarbase-indexer`](crates/lunarbase-indexer/README.md)                 | —                                                                  | Base is the default |
| Validation tooling         | [`lunarbase-tools`](crates/lunarbase-tools/README.md)                     | —                                                                  | internal            |

There are no aggregate facade packages. Integrators depend only on the pure
math, common client, and network adapter they use. Monad and Arbitrum packages
must remain behind an explicit experimental gate until their node-level live
validation is complete.

## Who this repository is for

- LunarBase partners embedding realtime off-chain quoting into an existing
  Rust or TypeScript service.
- Integrators that need a ready-to-run HTTP indexer with health, readiness, and
  Prometheus endpoints.
- Network-adapter authors connecting a new ordered data stream to the common
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
lunarbase-client-core = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
lunarbase-client-base = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

See the linked crate README files above for constructors, features, and
runtime guarantees.

### TypeScript libraries

After the `0.2.0` packages are published to the configured npm registry:

```bash
pnpm add @lunarbase/math @lunarbase/client-core @lunarbase/client-base
```

Use `@lunarbase/client-monad` or `@lunarbase/client-arbitrum` only for
experimental validation. Package-specific imports and examples are documented
in each package README.

### Runnable client examples

The [`examples`](examples/README.md) directory contains equivalent Rust and
TypeScript realtime quote loggers. After creating the language-specific
`.env`, run `make quote-logger-rust` or `make quote-logger-ts`.

### Runnable indexer

Configure `config/base.toml`, especially the Core proxy, implementation identity, router,
RPC, and realtime endpoints, then run:

```bash
make run
```

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
