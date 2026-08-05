# Quote logger examples

Rust and TypeScript examples show the same realtime quote flow:

```text
RPC snapshot + WebSocket updates → ordered client state → quote batch
```

Both examples load `RPC_URL` and `CORE_ADDRESS` from `.env`, discover active
lanes, and log exact-input quotes in both directions.

## Rust

```bash
cp examples/rust/quote-logger/.env.example examples/rust/quote-logger/.env
make quote-logger-rust
```

See [`rust/quote-logger`](rust/quote-logger/README.md) for source profiles and
optional settings.

## TypeScript

```bash
cp examples/typescript/quote-logger/.env.example examples/typescript/quote-logger/.env
make quote-logger-ts
```

See [`typescript/quote-logger`](typescript/quote-logger/README.md) for source
profiles and optional settings.
