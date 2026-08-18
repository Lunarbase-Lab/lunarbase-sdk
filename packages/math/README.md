# `@lunarbase-lab/pmm-v2-math`

Pure TypeScript `bigint` quote math with Solidity-compatible results.

Status: **fully supported**.

## Install

```bash
npm install @lunarbase-lab/pmm-v2-math
```

## Use

```ts
import {
  quote,
  solidityQuoteAmount,
  type QuotePolicy,
  type QuoteRequest,
  type QuoteState,
} from "@lunarbase-lab/pmm-v2-math";

const policy = { feeClass: "Whitelisted" } satisfies QuotePolicy;
const outcome = quote(request satisfies QuoteRequest, executionBlock, state satisfies QuoteState, policy);
const contractAmount = solidityQuoteAmount(request, outcome);
```

The root entry point is intentionally limited to quotes, quote state, and
canonical address helpers. Import optional low-level facilities explicitly:

```ts
import { fullMulDivDown } from "@lunarbase-lab/pmm-v2-math/arithmetic";
import { decodeLaneSlot0, modelQuoteToLaneSlot0Fields } from "@lunarbase-lab/pmm-v2-math/slot0";
```

Fee stages and checked implementation primitives are internal details of the
bit-exact quote engine.

## Guarantees

- Exact-input and exact-output quotes use checked `uint256` arithmetic.
- Values used in EVM arithmetic are represented as `bigint`.
- The package has no RPC, persistence, clock, async-runtime, or network dependency.
