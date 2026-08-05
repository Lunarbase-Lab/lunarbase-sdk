# `@lunarbase-lab/pmm-v2-math`

Pure TypeScript `bigint` quote math with Solidity-compatible results.

Status: **fully supported**.

## Install

```bash
npm install @lunarbase-lab/pmm-v2-math
```

## Use

```ts
import { quote, type QuoteRequest, type QuoteState } from "@lunarbase-lab/pmm-v2-math";

const outcome = quote(request satisfies QuoteRequest, executionBlock, state satisfies QuoteState);
```

Use the exported conversion helpers for decimal model values before calling
quote or state-packing APIs.

## Guarantees

- Exact-input and exact-output quotes use checked `uint256` arithmetic.
- Values used in EVM arithmetic are represented as `bigint`.
- The package has no RPC, persistence, clock, async-runtime, or network dependency.
