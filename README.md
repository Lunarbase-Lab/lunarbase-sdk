# LunarBase SDK

LunarBase SDK provides deterministic PMM v2 quote math, realtime state clients,
network data sources, and a runnable quote indexer for Rust and TypeScript.

## Packages

| Purpose             | Rust                                                                           | npm                                                                         | Status          |
| ------------------- | ------------------------------------------------------------------------------ | --------------------------------------------------------------------------- | --------------- |
| Quote math          | [lunarbase-pmm-v2-math](crates/lunarbase-math/README.md)                       | [@lunarbase-lab/pmm-v2-math](packages/math/README.md)                       | Fully supported |
| Realtime client     | [lunarbase-pmm-v2-client](crates/lunarbase-client/README.md)                   | [@lunarbase-lab/pmm-v2-client](packages/client/README.md)                   | Fully supported |
| EVM and Base source | [lunarbase-pmm-v2-source-evm](crates/lunarbase-source-evm/README.md)           | [@lunarbase-lab/pmm-v2-source-evm](packages/source-evm/README.md)           | Fully supported |
| Monad source        | [lunarbase-pmm-v2-source-monad](crates/lunarbase-source-monad/README.md)       | [@lunarbase-lab/pmm-v2-source-monad](packages/source-monad/README.md)       | Maintenance     |
| Arbitrum source     | [lunarbase-pmm-v2-source-arbitrum](crates/lunarbase-source-arbitrum/README.md) | [@lunarbase-lab/pmm-v2-source-arbitrum](packages/source-arbitrum/README.md) | Maintenance     |

All public packages use version 0.3.1.

Maintenance packages remain publishable; updates focus on compatibility,
reliability, and security fixes.

## Install

Rust:

```sh
cargo add lunarbase-pmm-v2-math@0.3.1
cargo add lunarbase-pmm-v2-client@0.3.1
cargo add lunarbase-pmm-v2-source-evm@0.3.1
```

TypeScript:

```sh
pnpm add @lunarbase-lab/pmm-v2-math@0.3.1
pnpm add @lunarbase-lab/pmm-v2-client@0.3.1
pnpm add @lunarbase-lab/pmm-v2-source-evm@0.3.1
```

Choose source-monad or source-arbitrum instead of source-evm when integrating
those networks.

## Indexer

The lunarbase-indexer workspace application exposes:

- POST /v1/quote
- POST /v1/quotes
- GET /healthz
- GET /readyz
- GET /metrics

Deployment identity and endpoints are supplied through command-line arguments,
LUNARBASE_* environment variables, or an operator-owned TOML file.

```sh
make run NETWORK=base CONFIG=/absolute/path/to/deployment.toml
```

See the [indexer guide](crates/lunarbase-indexer/README.md) and
[production runbook](docs/PRODUCTION_RUNBOOK.md).

## Development

Prerequisites are Rust 1.97.1, Node.js 22 or newer, pnpm 9.15.0, Docker,
and the validation tools listed in [CONTRIBUTING.md](CONTRIBUTING.md).

```sh
make install
make pre-push
```

make install installs locked workspace dependencies and enables the tracked
pre-push hook. make pre-push runs every reproducible CI, package-content,
process, supply-chain, formatting, lint, test, and documentation gate.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Specification](docs/SPECIFICATION.md)
- [Integration guide](docs/INTEGRATION.md)
- [Production runbook](docs/PRODUCTION_RUNBOOK.md)
- [Examples](examples/README.md)
- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)

## Releases

Publishing a GitHub Release with a vX.Y.Z tag runs the complete release gate
and publishes the five Rust crates to crates.io and the five scoped packages
to npm in dependency order. The tag must match every public package version.

## License

Licensed under either Apache-2.0 or MIT at your option. See [LICENSE](LICENSE).
