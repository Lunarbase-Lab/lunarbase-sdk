# `lunarbase-pmm-v2-math`

Pure Rust quote math with Solidity-compatible results.

Status: **fully supported**.

## Install

```toml
[dependencies]
lunarbase-math = { package = "lunarbase-pmm-v2-math", version = "0.4.1" }
```

## Use

```rust
use lunarbase_math::prelude::{FeeClass, QuotePolicy, QuoteRequest, QuoteState, quote};

fn evaluate(request: &QuoteRequest, execution_block: u64, state: &QuoteState) {
    let policy = QuotePolicy::base(FeeClass::Whitelisted);
    let outcome = quote(request, execution_block, state, policy);
    println!("{outcome:?}");
}
```

The crate root and `prelude` expose the complete quote data model. Optional
low-level functionality remains grouped under explicit modules:

```rust
use lunarbase_math::arithmetic::full_mul_div_down;
use lunarbase_math::slot0::decode_lane_slot0;
```

Fee stages and quote-engine modules are implementation details.

## Guarantees

- Exact-input and exact-output quotes use checked `U256` arithmetic.
- The caller owns state and supplies the execution block.
- The crate has no RPC, persistence, clock, async-runtime, or network dependency.
