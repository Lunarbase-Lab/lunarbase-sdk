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

# Prefer a directly installed pnpm, but support Node installations where pnpm
# is exposed through Corepack instead of being present on PATH.
PNPM_CMD := $(shell if command -v "$(PNPM)" >/dev/null 2>&1; then printf '%s' "$(PNPM)"; elif command -v corepack >/dev/null 2>&1; then printf '%s' "corepack pnpm@$(PNPM_VERSION)"; fi)

.DEFAULT_GOAL := build

.PHONY: help install build build-rust build-ts build-release \
	check check-rust check-ts fmt fmt-rust fmt-ts fmt-check fmt-check-rust fmt-check-ts lint lint-rust lint-ts \
	test test-rust test-ts docs docs-rust ffi verify ci clean check-pnpm

help:
	@echo "LunarBase build targets:"
	@echo "  make build          Build all Rust crates and TypeScript packages"
	@echo "  make build-release  Build all Rust targets in release mode plus TypeScript"
	@echo "  make check          Run Rust and TypeScript compile checks"
	@echo "  make test           Run Rust and TypeScript tests"
	@echo "  make lint           Run Rust clippy and TypeScript ESLint"
	@echo "  make fmt            Format Rust and TypeScript sources"
	@echo "  make fmt-check      Verify Rust and TypeScript formatting"
	@echo "  make docs           Build Rust API documentation with warnings as errors"
	@echo "  make ffi            Run Solidity differential FFI from lunarbase-contracts"
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
	$(NODE) --test packages/math/dist/*.test.js packages/client/dist/*.test.js

docs: docs-rust

docs-rust:
	RUSTDOCFLAGS="$(RUSTDOCFLAGS)" $(CARGO) doc --workspace --no-deps

ffi:
	$(MAKE) -C "$(CONTRACTS_DIR)" differential-ffi

verify: fmt-check check lint test docs

ci: verify

clean: check-pnpm
	$(CARGO) clean
	$(PNPM_CMD) exec tsc -b packages/math/tsconfig.json packages/client/tsconfig.json --clean

check-pnpm:
	@if [ -n "$(PNPM_CMD)" ]; then :; else \
		echo "pnpm is required. Install pnpm or enable Corepack: corepack enable"; \
		exit 1; \
	fi
