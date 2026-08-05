# `lunarbase-pmm-v2-math`

Pure Rust quote math with Solidity-compatible results.

Status: **fully supported**.

## Install

```toml
[dependencies]
lunarbase-math = { package = "lunarbase-pmm-v2-math", version = "0.3.0" }
```

## Use

```rust
use lunarbase_math::prelude::{quote, QuoteRequest, QuoteState};

fn evaluate(request: &QuoteRequest, execution_block: u64, state: &QuoteState) {
    let outcome = quote(request, execution_block, state);
    println!("{outcome:?}");
}
```

## Guarantees

- Exact-input and exact-output quotes use checked `U256` arithmetic.
- The caller owns state and supplies the execution block.
- The crate has no RPC, persistence, clock, async-runtime, or network dependency.
