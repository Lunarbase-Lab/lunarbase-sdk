# `lunarbase-math`

Pure Rust implementation of LunarBase quote math with bit-exact Solidity
semantics.

## Install

Pin an approved SDK revision until the crate is published:

```toml
[dependencies]
lunarbase-math = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

## Use

The crate exposes:

- `quote`, `quote_exact_in`, and `quote_exact_out`;
- compact `LaneState`, `FeeProfile`, and `QuoteState` inputs;
- `Lane.slot0` pack, decode, and update helpers;
- full-width and checked `U256` arithmetic;
- Solidity-compatible unavailable and sentinel outcomes.

```rust
use lunarbase_math::{quote, QuoteRequest, QuoteState};

fn evaluate(request: &QuoteRequest, execution_block: u64, state: &QuoteState) {
    let outcome = quote(request, execution_block, state);
    println!("{outcome:?}");
}
```

The caller owns state and supplies the execution block. This crate has no
async runtime, RPC, Redis, filesystem, clock, or network dependency.

## Compatibility

The workspace pins the Solidity compatibility revision in
`MATH_COMPATIBILITY_VERSION`. Shared fixtures and the canonical Foundry FFI
suite verify Rust, TypeScript, and Solidity results bit for bit.
