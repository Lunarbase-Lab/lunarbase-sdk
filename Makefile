# LunarBase SDK build entry point.
#
# The Makefile keeps the Rust workspace, TypeScript packages, documentation,
# and cross-language verification commands discoverable from the repository
# root. Individual targets remain composable for local development and CI.

SHELL := /bin/sh

.NOTPARALLEL: ci pre-push release-check performance-baseline

CARGO ?= cargo
PNPM ?= pnpm
PNPM_VERSION ?= 9.15.0
NODE ?= node
GIT ?= git
ACTIONLINT ?= actionlint
ACTIONLINT_VERSION ?= 1.7.12
CARGO_DENY_VERSION ?= 0.20.2
CONTRACTS_DIR ?=
RUSTDOCFLAGS ?= -D warnings
NETWORK ?= base
CONFIG ?=
INDEXER_ARGS ?=
EVENT_WORKER_ARGS ?=
INDEXER_FEATURES ?= $(NETWORK)
COMPOSE ?= docker compose -f examples/indexer/docker-compose.yml
CARGO_PUBLISH_PACKAGES := lunarbase-pmm-v2-math lunarbase-pmm-v2-client lunarbase-pmm-v2-source-evm lunarbase-pmm-v2-source-monad lunarbase-pmm-v2-source-arbitrum
NPM_PUBLISH_DIRS := packages/math packages/client packages/source-evm packages/source-monad packages/source-arbitrum
CARGO_PACKAGE_PATCHES := \
	--config 'patch.crates-io.lunarbase-pmm-v2-math.path="$(CURDIR)/crates/lunarbase-math"' \
	--config 'patch.crates-io.lunarbase-pmm-v2-client.path="$(CURDIR)/crates/lunarbase-client"' \
	--config 'patch.crates-io.lunarbase-pmm-v2-source-evm.path="$(CURDIR)/crates/lunarbase-source-evm"' \
	--config 'patch.crates-io.lunarbase-pmm-v2-source-monad.path="$(CURDIR)/crates/lunarbase-source-monad"' \
	--config 'patch.crates-io.lunarbase-pmm-v2-source-arbitrum.path="$(CURDIR)/crates/lunarbase-source-arbitrum"'

# Use the workspace-pinned pnpm through Corepack when available. Fall back to
# an installed binary or npx so the hook also works with Corepack-free Node.
PNPM_CMD := $(shell if command -v corepack >/dev/null 2>&1; then printf '%s' "corepack pnpm@$(PNPM_VERSION)"; elif command -v "$(PNPM)" >/dev/null 2>&1; then printf '%s' "$(PNPM)"; elif command -v npx >/dev/null 2>&1; then printf '%s' "npx --yes pnpm@$(PNPM_VERSION)"; fi)

.DEFAULT_GOAL := build

.PHONY: help install hooks-install build build-rust build-ts build-math-ts build-release build-indexer build-event-worker run run-indexer run-event-worker \
	check check-rust check-ts check-network-feature check-network-features check-monad-native \
	fmt fmt-rust fmt-ts fmt-check fmt-check-rust fmt-check-ts lint lint-rust lint-ts \
	test test-rust test-ts test-runtime test-process-e2e audit audit-rust audit-rust-ci audit-ts load performance-baseline quote-benchmark quote-allocation-benchmark monad-live-validate docs docs-rust ffi \
	quote-logger quote-logger-rust quote-logger-ts activity-actor activity-actor-inspect activity-actor-wallet monad-parser-smoke docker-build docker-image-check docker-build-monad-native docker-up docker-down release-artifacts release-check release-version-check release-check-rust release-check-ts source-size-check repository-check public-api-check check-scripts check-ci-tools verify ci-rust ci-ts ci-supply-chain ci pre-push clean check-pnpm

help:
	@echo "LunarBase build targets:"
	@echo "  make build          Build all Rust crates and TypeScript packages"
	@echo "  make build-release  Build all Rust targets in release mode plus TypeScript"
	@echo "  make build-indexer  Build the selected indexer network in release mode"
	@echo "  make build-event-worker  Build the durable event worker"
	@echo "  make run            Run lunarbase-indexer from CLI/LUNARBASE_* values"
	@echo "  make run-event-worker  Run from CLI/LUNARBASE_EVENT_* values"
	@echo "                      Select source features with NETWORK=evm|monad|arbitrum"
	@echo "                      Optional example: CONFIG=examples/indexer/config/bsc-testnet.toml"
	@echo "  make check          Run Rust and TypeScript compile checks"
	@echo "  make test           Run Rust and TypeScript tests"
	@echo "  make test-runtime   Test only client runtime crates/packages"
	@echo "  make test-process-e2e  Run real-process RPC/WS/Redis/multi-replica scenarios"
	@echo "  make load           Benchmark 15 lanes / 100 pairs by default"
	@echo "  make performance-baseline  Run the reproducible quote timing/allocation matrix"
	@echo "  make monad-live-validate  Run the real Monad parser/RPC/indexer soak"
	@echo "  make lint           Run Rust clippy and TypeScript ESLint"
	@echo "  make audit          Check Rust advisories/licenses/sources and npm advisories"
	@echo "  make fmt            Format Rust and TypeScript sources"
	@echo "  make fmt-check      Verify Rust and TypeScript formatting"
	@echo "  make docs           Build Rust API documentation with warnings as errors"
	@echo "  make ffi            Run Solidity differential FFI from lunarbase-contracts"
	@echo "  make quote-logger-rust  Run the Rust realtime quote example"
	@echo "  make quote-logger-ts    Run the TypeScript realtime quote example"
	@echo "  make activity-actor-wallet  Generate a local testnet-only actor wallet"
	@echo "  make activity-actor-inspect Inspect the BSC testnet pool without transactions"
	@echo "  make activity-actor    Run the BSC testnet activity actor"
	@echo "  make monad-parser-smoke  Connect the Rust Monad source to a local parser"
	@echo "  make docker-up      Build and start indexer + Redis"
	@echo "  make docker-image-check  Build the release Base image"
	@echo "  make docker-build-monad-native  Build the x86_64 native Monad image"
	@echo "  make release-check  Validate Rust/npm package contents"
	@echo "  make source-size-check  Enforce the 500 non-comment code-line limit"
	@echo "  make repository-check  Validate repository release hygiene"
	@echo "  make public-api-check  Enforce the math package export allowlist"
	@echo "  make verify         Run formatting, checks, lint, tests, and docs"
	@echo "  make pre-push       Run every locally reproducible GitHub CI check"
	@echo "  make install        Install locked pnpm dependencies and the Git hook"
	@echo "  make hooks-install  Configure the repository-managed Git hooks"
	@echo "  make clean          Remove Rust and TypeScript build artifacts"

install: check-pnpm hooks-install
	$(PNPM_CMD) install --frozen-lockfile

hooks-install:
	@current="$$( $(GIT) config --local --get core.hooksPath || true )"; \
	if [ -n "$$current" ] && [ "$$current" != ".githooks" ]; then \
		echo "Refusing to replace existing core.hooksPath=$$current"; \
		exit 1; \
	fi
	$(GIT) config --local core.hooksPath .githooks

build: build-rust build-ts

build-rust:
	$(CARGO) build --locked --workspace --all-targets

build-ts: check-pnpm
	$(PNPM_CMD) build

build-math-ts: check-pnpm
	$(NODE) scripts/clean-dist.mjs packages/math/dist
	$(PNPM_CMD) exec tsc -p packages/math/tsconfig.json

build-release: check-pnpm
	$(CARGO) build --locked --workspace --all-targets --release
	$(PNPM_CMD) build

build-indexer:
	$(CARGO) build -p lunarbase-indexer --no-default-features --features "$(INDEXER_FEATURES)" --release

build-event-worker:
	$(CARGO) build -p lunarbase-event-worker --no-default-features --features "$(INDEXER_FEATURES)" --release

run: run-indexer

run-indexer:
	$(CARGO) run -p lunarbase-indexer --no-default-features --features "$(INDEXER_FEATURES)" -- $(if $(strip $(CONFIG)),--config "$(CONFIG)") $(INDEXER_ARGS)

run-event-worker:
	$(CARGO) run -p lunarbase-event-worker --no-default-features --features "$(INDEXER_FEATURES)" -- $(EVENT_WORKER_ARGS)

check: check-rust check-ts

check-rust:
	$(CARGO) check --locked --workspace --all-targets

check-ts: build-ts

check-network-feature:
	$(CARGO) check --locked -p lunarbase-indexer --no-default-features --features "$(NETWORK)" --all-targets
	$(CARGO) check --locked -p lunarbase-event-worker --no-default-features --features "$(NETWORK)" --all-targets

check-network-features:
	@set -eu; for network in base monad arbitrum; do \
		$(MAKE) check-network-feature NETWORK="$$network"; \
	done

check-monad-native:
	$(CARGO) check --locked -p lunarbase-indexer --no-default-features --features monad-native --all-targets
	$(CARGO) clippy --locked -p lunarbase-indexer --no-default-features --features monad-native --all-targets -- -D warnings
	$(CARGO) build --locked -p lunarbase-indexer --no-default-features --features monad-native
	$(CARGO) check --locked -p lunarbase-event-worker --no-default-features --features monad-native --all-targets
	$(CARGO) clippy --locked -p lunarbase-event-worker --no-default-features --features monad-native --all-targets -- -D warnings
	$(CARGO) build --locked -p lunarbase-event-worker --no-default-features --features monad-native

fmt: fmt-rust fmt-ts

fmt-rust:
	$(CARGO) fmt --all

fmt-ts: check-pnpm
	$(PNPM_CMD) run format

fmt-check: fmt-check-rust fmt-check-ts

fmt-check-rust:
	$(CARGO) fmt --all -- --check

fmt-check-ts: check-pnpm
	$(PNPM_CMD) run format:check

lint: lint-rust lint-ts

lint-rust:
	$(CARGO) clippy --locked --workspace --all-targets -- -D warnings

lint-ts: check-pnpm
	$(PNPM_CMD) exec eslint packages examples/typescript scripts --max-warnings=0

audit: audit-rust audit-ts

audit-rust:
	$(CARGO) deny --all-features check
	$(CARGO) machete

audit-rust-ci:
	$(CARGO) deny --all-features check

audit-ts: check-pnpm
	$(PNPM_CMD) audit --prod --audit-level high

test: test-rust test-ts

test-rust:
	$(CARGO) test --locked --workspace

test-ts: build-ts
	$(PNPM_CMD) test:compiled

test-runtime: build-ts
	$(CARGO) test -p lunarbase-pmm-v2-client -p lunarbase-pmm-v2-source-evm -p lunarbase-pmm-v2-source-monad -p lunarbase-pmm-v2-source-arbitrum -p lunarbase-event-worker
	$(NODE) --test packages/client/dist/*.test.js packages/client/dist/**/*.test.js packages/source-evm/dist/*.test.js packages/source-monad/dist/*.test.js packages/source-arbitrum/dist/*.test.js

test-process-e2e:
	$(CARGO) build --locked -p lunarbase-indexer -p lunarbase-tools
	$(CARGO) run --locked -p lunarbase-tools --bin lunarbase-e2e -- --indexer-bin target/debug/lunarbase-indexer

load:
	$(CARGO) run -p lunarbase-tools --bin lunarbase-load -- \
		--indexer-url "$${INDEXER_URL:-http://127.0.0.1:8080}" \
		--lanes "$${LANES:-15}" --pairs "$${PAIRS:-100}" \
		--requests "$${REQUESTS:-20000}" --concurrency "$${CONCURRENCY:-128}"

performance-baseline: quote-benchmark quote-allocation-benchmark

quote-benchmark:
	@set -eu; for lanes in 15 64; do for batch in 1 16 256; do \
		$(CARGO) run --locked --release -p lunarbase-tools --bin lunarbase-quote-bench -- \
			--mode timing --lanes "$$lanes" --pairs 100 --batch-size "$$batch" \
			--concurrency 128 --measured-quotes "$${MEASURED_QUOTES:-1048576}" \
			--warmup-calls "$${WARMUP_CALLS:-4096}"; \
	done; done

quote-allocation-benchmark:
	@set -eu; for lanes in 15 64; do for batch in 1 16 256; do \
		$(CARGO) run --locked --release -p lunarbase-tools --bin lunarbase-quote-bench \
			--features allocation-stats -- \
			--mode allocations --lanes "$$lanes" --pairs 100 --batch-size "$$batch" \
			--concurrency 1 --allocation-calls "$${ALLOCATION_CALLS:-4096}" \
			--warmup-calls "$${WARMUP_CALLS:-4096}"; \
	done; done

monad-live-validate:
	$(CARGO) run -p lunarbase-tools --bin lunarbase-monad-validate -- \
		--indexer-url "$${INDEXER_URL:-http://127.0.0.1:8081}" \
		--parser-ws-url "$${MONAD_PARSER_WS:-ws://127.0.0.1:8080/ws/subscriptions}" \
		--parser-ready-url "$${MONAD_PARSER_READY:-http://127.0.0.1:8080/readyz}" \
		--rpc-url "$${MONAD_RPC_URL:-http://127.0.0.1:8545}" \
		--duration-seconds "$${SOAK_SECONDS:-3600}"

docs: docs-rust

docs-rust:
	RUSTDOCFLAGS="$(RUSTDOCFLAGS)" $(CARGO) doc --locked --workspace --no-deps

ffi:
	@if [ -z "$(CONTRACTS_DIR)" ]; then \
		echo "CONTRACTS_DIR is required; point it to the contracts workspace."; \
		echo "Example: make ffi CONTRACTS_DIR=/absolute/path/to/lunarbase-contracts"; \
		exit 2; \
	fi
	$(MAKE) -C "$(CONTRACTS_DIR)" differential-ffi

quote-logger: quote-logger-rust

quote-logger-rust:
	$(CARGO) run -p lunarbase-quote-logger

quote-logger-ts: check-pnpm
	$(PNPM_CMD) --filter @lunarbase-lab/example-quote-logger build
	$(PNPM_CMD) --filter @lunarbase-lab/example-quote-logger start

activity-actor-wallet: build-math-ts
	$(PNPM_CMD) --filter @lunarbase-lab/example-activity-actor build
	$(PNPM_CMD) --filter @lunarbase-lab/example-activity-actor wallet:new

activity-actor-inspect: build-math-ts
	$(PNPM_CMD) --filter @lunarbase-lab/example-activity-actor build
	$(PNPM_CMD) --filter @lunarbase-lab/example-activity-actor inspect

activity-actor: build-math-ts
	$(PNPM_CMD) --filter @lunarbase-lab/example-activity-actor build
	$(PNPM_CMD) --filter @lunarbase-lab/example-activity-actor run start --live

monad-parser-smoke:
	$(CARGO) run -p lunarbase-pmm-v2-source-monad --example monad-parser-smoke

docker-build:
	$(COMPOSE) build

docker-image-check:
	docker build --build-arg NETWORK_FEATURES=base --tag lunarbase-indexer:ci .

docker-build-monad-native:
	docker build --platform linux/amd64 \
		--build-arg NETWORK_FEATURES=monad-native \
		--tag lunarbase-indexer:monad-native .

docker-up:
	$(COMPOSE) up --build -d

docker-down:
	$(COMPOSE) down

release-artifacts:
	mkdir -p dist
	$(CARGO) build --locked --release -p lunarbase-indexer --no-default-features --features base
	cp target/release/lunarbase-indexer dist/lunarbase-indexer-base

release-check: public-api-check release-version-check release-check-rust release-check-ts

release-version-check:
	$(NODE) scripts/check-release-version.mjs

release-check-rust:
	$(NODE) scripts/check-cargo-publish.mjs
	@set -eu; for package in $(CARGO_PUBLISH_PACKAGES); do \
		$(CARGO) package --locked -p "$$package" --allow-dirty $(CARGO_PACKAGE_PATCHES); \
	done

release-check-ts: build-ts
	$(NODE) scripts/clean-dist.mjs dist
	mkdir -p dist
	$(NODE) scripts/check-release-dist.mjs
	@set -eu; for package_dir in $(NPM_PUBLISH_DIRS); do \
		( cd "$$package_dir" && $(PNPM_CMD) pack --pack-destination "$(CURDIR)/dist" ); \
	done
	$(NODE) scripts/check-packed-packages.mjs

source-size-check:
	$(NODE) scripts/check-source-lines.mjs

repository-check:
	$(NODE) scripts/check-repository-hygiene.mjs

public-api-check:
	$(NODE) scripts/check-math-public-api.mjs

ci-rust: fmt-check-rust check-rust lint-rust test-rust docs-rust

ci-ts: fmt-check-ts check-ts lint-ts test-ts

ci-supply-chain: audit-rust-ci audit-ts

verify: repository-check source-size-check public-api-check check-scripts ci-rust ci-ts

ci: verify check-network-features check-monad-native docker-image-check test-process-e2e ci-supply-chain release-check

pre-push: check-ci-tools ci

clean: check-pnpm
	$(CARGO) clean
	$(PNPM_CMD) exec tsc -b packages/math/tsconfig.json packages/client/tsconfig.json packages/source-evm/tsconfig.json packages/source-monad/tsconfig.json packages/source-arbitrum/tsconfig.json examples/typescript/quote-logger/tsconfig.json examples/typescript/activity-actor/tsconfig.json examples/typescript/quote-oracle/tsconfig.json --clean

check-pnpm:
	@if [ -n "$(PNPM_CMD)" ]; then :; else \
		echo "pnpm is required. Install pnpm, enable Corepack, or install npx"; \
		exit 1; \
	fi
	@actual="$$( $(PNPM_CMD) --version )"; \
	if [ "$$actual" != "$(PNPM_VERSION)" ]; then \
		echo "pnpm $(PNPM_VERSION) is required; found $$actual"; \
		exit 1; \
	fi

check-scripts:
	@set -eu; for script in scripts/*.mjs; do $(NODE) --check "$$script"; done
	bash -n scripts/*.sh
	$(NODE) --test scripts/*.test.mjs
	sh -n .githooks/pre-push
	$(ACTIONLINT)

check-ci-tools: check-pnpm
	@$(CARGO) --version >/dev/null || { echo "Rust/Cargo is required"; exit 1; }
	@command -v docker >/dev/null 2>&1 || { echo "Docker is required for the image gate"; exit 1; }
	@docker info >/dev/null 2>&1 || { echo "A reachable Docker daemon is required for the image gate"; exit 1; }
	@actual="$$( $(ACTIONLINT) -version 2>/dev/null | sed -n '1p' || true )"; \
	if [ "$$actual" != "$(ACTIONLINT_VERSION)" ]; then \
		echo "actionlint $(ACTIONLINT_VERSION) is required; found $${actual:-missing}"; \
		exit 1; \
	fi
	@actual="$$( $(CARGO) deny --version 2>/dev/null || true )"; \
	if [ "$$actual" != "cargo-deny $(CARGO_DENY_VERSION)" ]; then \
		echo "cargo-deny $(CARGO_DENY_VERSION) is required; found $${actual:-missing}"; \
		exit 1; \
	fi
	@command -v redis-server >/dev/null 2>&1 || { echo "redis-server is required for process E2E"; exit 1; }
	@if [ "$$(uname -s)" = "Linux" ]; then \
		for tool in clang cmake; do \
			command -v "$$tool" >/dev/null 2>&1 || { echo "$$tool is required for monad-native"; exit 1; }; \
		done; \
	else \
		echo "Note: monad-native final linking is enforced by the Linux CI gate."; \
	fi
