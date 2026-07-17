# LunarBase math/client build entry point.
#
# The Makefile keeps the Rust workspace, TypeScript packages, documentation,
# and cross-language verification commands discoverable from the repository
# root. Individual targets remain composable for local development and CI.

SHELL := /bin/sh

CARGO ?= cargo
PNPM ?= pnpm
PNPM_VERSION ?= 9.15.0
NODE ?= node
CONTRACTS_DIR ?= ../lunarbase-contracts
RUSTDOCFLAGS ?= -D warnings
NETWORK ?= base
CONFIG ?= config/$(NETWORK).toml
INDEXER_FEATURES ?= $(NETWORK)

# Prefer a directly installed pnpm, but support Node installations where pnpm
# is exposed through Corepack instead of being present on PATH.
PNPM_CMD := $(shell if command -v "$(PNPM)" >/dev/null 2>&1; then printf '%s' "$(PNPM)"; elif command -v corepack >/dev/null 2>&1; then printf '%s' "corepack pnpm@$(PNPM_VERSION)"; fi)

.DEFAULT_GOAL := build

.PHONY: help install build build-rust build-ts build-release build-indexer run run-indexer \
	check check-rust check-ts fmt fmt-rust fmt-ts fmt-check fmt-check-rust fmt-check-ts lint lint-rust lint-ts \
	test test-rust test-ts test-runtime test-process-e2e load monad-live-validate docs docs-rust ffi \
	monad-parser-smoke docker-build docker-up docker-down release-artifacts release-check source-size-check verify ci clean check-pnpm

help:
	@echo "LunarBase build targets:"
	@echo "  make build          Build all Rust crates and TypeScript packages"
	@echo "  make build-release  Build all Rust targets in release mode plus TypeScript"
	@echo "  make build-indexer  Build the selected indexer network in release mode"
	@echo "  make run            Run lunarbase-indexer (Base by default)"
	@echo "                      Select another network with NETWORK=monad|arbitrum"
	@echo "  make check          Run Rust and TypeScript compile checks"
	@echo "  make test           Run Rust and TypeScript tests"
	@echo "  make test-runtime   Test only client runtime crates/packages"
	@echo "  make test-process-e2e  Run real-process RPC/WS/Redis/failover scenarios"
	@echo "  make load           Benchmark 15 lanes / 100 pairs by default"
	@echo "  make monad-live-validate  Run the real Monad parser/RPC/indexer soak"
	@echo "  make lint           Run Rust clippy and TypeScript ESLint"
	@echo "  make fmt            Format Rust and TypeScript sources"
	@echo "  make fmt-check      Verify Rust and TypeScript formatting"
	@echo "  make docs           Build Rust API documentation with warnings as errors"
	@echo "  make ffi            Run Solidity differential FFI from lunarbase-contracts"
	@echo "  make monad-parser-smoke  Connect the Rust Monad client to a local parser"
	@echo "  make docker-up      Build and start indexer + Redis"
	@echo "  make release-check  Validate Rust/npm package contents"
	@echo "  make source-size-check  Enforce the 500-line source-file limit"
	@echo "  make verify         Run formatting, checks, lint, tests, and docs"
	@echo "  make install        Install locked pnpm dependencies"
	@echo "  make clean          Remove Rust and TypeScript build artifacts"

install: check-pnpm
	$(PNPM_CMD) install --frozen-lockfile

build: build-rust build-ts

build-rust:
	$(CARGO) build --workspace --all-targets

build-ts: check-pnpm
	$(PNPM_CMD) build

build-release: check-pnpm
	$(CARGO) build --workspace --all-targets --release
	$(PNPM_CMD) build

build-indexer:
	$(CARGO) build -p lunarbase-indexer --no-default-features --features "$(INDEXER_FEATURES)" --release

run: run-indexer

run-indexer:
	$(CARGO) run -p lunarbase-indexer --no-default-features --features "$(INDEXER_FEATURES)" -- --config "$(CONFIG)"

check: check-rust check-ts

check-rust:
	$(CARGO) check --workspace --all-targets

check-ts: build-ts

fmt: fmt-rust fmt-ts

fmt-rust:
	$(CARGO) fmt --all

fmt-ts: check-pnpm
	$(PNPM_CMD) exec prettier --write "packages/**/*.ts"

fmt-check: fmt-check-rust fmt-check-ts

fmt-check-rust:
	$(CARGO) fmt --all -- --check

fmt-check-ts: check-pnpm
	$(PNPM_CMD) exec prettier --check "packages/**/*.ts"

lint: lint-rust lint-ts

lint-rust:
	$(CARGO) clippy --workspace --all-targets -- -D warnings

lint-ts: check-pnpm
	$(PNPM_CMD) exec eslint packages --max-warnings=0

test: test-rust test-ts

test-rust:
	$(CARGO) test --workspace

test-ts: build-ts
	$(NODE) --test packages/math/dist/*.test.js packages/client/dist/tests/*.test.js

test-runtime: build-ts
	$(CARGO) test -p lunarbase-client-core -p lunarbase-client-base -p lunarbase-client-monad -p lunarbase-client-arbitrum
	$(NODE) --test packages/client/dist/tests/*.test.js

test-process-e2e:
	$(CARGO) build -p lunarbase-indexer -p lunarbase-tools
	$(CARGO) run -p lunarbase-tools --bin lunarbase-e2e -- --indexer-bin target/debug/lunarbase-indexer

load:
	$(CARGO) run -p lunarbase-tools --bin lunarbase-load -- \
		--indexer-url "$${INDEXER_URL:-http://127.0.0.1:8080}" \
		--lanes "$${LANES:-15}" --pairs "$${PAIRS:-100}" \
		--requests "$${REQUESTS:-20000}" --concurrency "$${CONCURRENCY:-128}"

monad-live-validate:
	$(CARGO) run -p lunarbase-tools --bin lunarbase-monad-validate -- \
		--indexer-url "$${INDEXER_URL:-http://127.0.0.1:8081}" \
		--parser-ws-url "$${MONAD_PARSER_WS:-ws://127.0.0.1:8080/ws/subscriptions}" \
		--parser-ready-url "$${MONAD_PARSER_READY:-http://127.0.0.1:8080/readyz}" \
		--rpc-url "$${MONAD_RPC_URL:-http://127.0.0.1:8545}" \
		--duration-seconds "$${SOAK_SECONDS:-3600}"

docs: docs-rust

docs-rust:
	RUSTDOCFLAGS="$(RUSTDOCFLAGS)" $(CARGO) doc --workspace --no-deps

ffi:
	$(MAKE) -C "$(CONTRACTS_DIR)" differential-ffi

monad-parser-smoke:
	$(CARGO) run -p lunarbase-client-monad --example monad-parser-smoke

docker-build:
	docker compose build

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

release-artifacts:
	mkdir -p dist
	$(CARGO) build --locked --release -p lunarbase-indexer --no-default-features --features base
	cp target/release/lunarbase-indexer dist/lunarbase-indexer-base
	$(CARGO) build --locked --release -p lunarbase-indexer --no-default-features --features monad
	cp target/release/lunarbase-indexer dist/lunarbase-indexer-monad
	$(CARGO) build --locked --release -p lunarbase-indexer --no-default-features --features arbitrum
	cp target/release/lunarbase-indexer dist/lunarbase-indexer-arbitrum

release-check: build-ts
	mkdir -p dist
	$(CARGO) package --locked --offline --no-verify -p lunarbase-math --allow-dirty
	$(CARGO) package --offline --list -p lunarbase-client-core --allow-dirty
	$(CARGO) package --offline --list -p lunarbase-client-base --allow-dirty
	$(CARGO) package --offline --list -p lunarbase-client-monad --allow-dirty
	$(CARGO) package --offline --list -p lunarbase-client-arbitrum --allow-dirty
	$(CARGO) package --offline --list -p lunarbase-client --allow-dirty
	$(NODE) scripts/check-release-dist.mjs
	$(PNPM_CMD) -r --filter "./packages/**" pack --pack-destination dist

source-size-check:
	$(NODE) scripts/check-source-lines.mjs

verify: source-size-check fmt-check check lint test docs

ci: verify

clean: check-pnpm
	$(CARGO) clean
	$(PNPM_CMD) exec tsc -b packages/math/tsconfig.json packages/client-core/tsconfig.json packages/client-base/tsconfig.json packages/client-monad/tsconfig.json packages/client-arbitrum/tsconfig.json packages/client/tsconfig.json --clean

check-pnpm:
	@if [ -n "$(PNPM_CMD)" ]; then :; else \
		echo "pnpm is required. Install pnpm or enable Corepack: corepack enable"; \
		exit 1; \
	fi
