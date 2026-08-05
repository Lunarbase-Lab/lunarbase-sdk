# Contributing

## Setup

Use Rust 1.97.1, Node.js 22 or newer, and pnpm 9.15.0. The full
pre-push gate also requires a reachable Docker daemon, cargo-deny
0.20.2, actionlint 1.7.12, redis-server, and the native build dependencies reported by
make check-ci-tools.

```sh
make install
make pre-push
```

make install installs locked dependencies and enables the repository pre-push
hook. make pre-push runs every locally reproducible CI and release check.

## Changes

- Keep public APIs and Rust/TypeScript behavior aligned.
- Add tests for observable behavior and failure paths.
- Keep documentation concise, current, and focused on supported behavior.
- Do not include deployment secrets or unpublished package names in public docs.
- Run make fmt before the final gate.

Public Rust packages use the lunarbase-pmm-v2-* prefix. Public npm packages
use the @lunarbase-lab/pmm-v2-* scope and prefix. All public package versions
advance together.
