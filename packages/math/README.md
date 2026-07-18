# `@lunarbase/math`

Pure TypeScript `bigint` implementation of LunarBase quote math with bit-exact
Solidity semantics.

## Install

```bash
pnpm add @lunarbase/math
```

## Use

```ts
import { quote, type QuoteRequest, type QuoteState } from "@lunarbase/math";

const outcome = quote(request satisfies QuoteRequest, executionBlock, state satisfies QuoteState);
```

The package exports exact-in/out quoting, fee and slippage helpers, checked
`uint256` arithmetic, compact lane state, and `Lane.slot0` packing helpers.

All values that participate in EVM arithmetic are `bigint`. The package has no
RPC, persistence, clock, async, or network dependency. Shared fixtures and the
canonical Foundry FFI suite verify parity with Rust and Solidity.
