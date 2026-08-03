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

### Pricing-model numbers

Do not multiply JavaScript `number` values by `1e18` before converting them to
`bigint`; that multiplication occurs in binary64 and can change low decimal
digits. Convert the quote-critical model fields through the package instead:

```ts
import { EMPTY_SLOT0, encodeLaneSlot0, modelQuoteToLaneSlot0Fields } from "@lunarbase/math";

const quoteFields = modelQuoteToLaneSlot0Fields({
  anchorPrice: quotes.S,
  askSpreadBps: quotes.spreadAskBps,
  bidSpreadBps: quotes.spreadBidBps,
  cashDecimals,
  assetDecimals,
});

const slot0 = encodeLaneSlot0({
  ...EMPTY_SLOT0,
  ...quoteFields,
  exists: true,
});
```

`anchorPrice` is encoded using the contract's decimal-adjusted WAD convention,
and conventional spread bps are encoded into the protocol's `1_000_000`
denominator. Legacy `feeAskX24` and `feeBidX24` values are Q24 fields and are
not valid inputs for the new `LaneSlot0` fee layout.
`decimalNumberToBigInt` is available for other non-negative model numbers and
supports explicit exact/down/up/nearest rounding.
