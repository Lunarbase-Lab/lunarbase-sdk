# syntax=docker/dockerfile:1.7
FROM rust:1.88-bookworm AS builder

ARG NETWORK_FEATURES=base
WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build \
    --locked \
    --release \
    -p lunarbase-indexer \
    --no-default-features \
    --features "${NETWORK_FEATURES}"

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 10001 lunarbase
COPY --from=builder /workspace/target/release/lunarbase-indexer /usr/local/bin/lunarbase-indexer
USER lunarbase
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/lunarbase-indexer"]
CMD ["--config", "/etc/lunarbase/indexer.toml"]
