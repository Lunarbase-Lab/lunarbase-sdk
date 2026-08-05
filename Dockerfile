# syntax=docker/dockerfile:1.7
FROM rust:1.97.1-trixie AS builder

ARG NETWORK_FEATURES=base
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && if [ "${NETWORK_FEATURES}" = "monad-native" ]; then \
         apt-get install -y --no-install-recommends clang libhugetlbfs-dev; \
       fi \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples/rust/quote-logger ./examples/rust/quote-logger
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build \
    --locked \
    --release \
    -p lunarbase-indexer \
    --no-default-features \
    --features "${NETWORK_FEATURES}" \
    && cp /workspace/target/release/lunarbase-indexer /tmp/lunarbase-indexer

FROM debian:trixie-slim AS runtime
ARG NETWORK_FEATURES=base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && if [ "${NETWORK_FEATURES}" = "monad-native" ]; then \
         apt-get install -y --no-install-recommends libhugetlbfs0; \
       fi \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 10001 lunarbase
COPY --from=builder /tmp/lunarbase-indexer /usr/local/bin/lunarbase-indexer
USER lunarbase
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/lunarbase-indexer"]
