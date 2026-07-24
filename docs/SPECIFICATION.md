# LunarBase off-chain quoting: implementation specification

Status: handoff specification for `lunarbase-math`.

Contract reference: `lunarbase-contracts` branch `dev`, commit
[`cfeb6b86f425c5207f3cf80c8b40adde07d6a60b`](https://github.com/Lunarbase-Lab/lunarbase-contracts/tree/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b), inspected on 2026-07-23.

This document is deliberately pinned to a commit. A later `dev` revision must not silently change the
off-chain result. Any contract math or event-schema change requires a new compatibility version and new
cross-language fixtures.

## 1. Goal

Build one `lunarbase-sdk` repository containing:

1. A pure Rust quoting library.
2. A pure TypeScript quoting library.
3. A Rust client that indexes every contract component needed for fully off-chain quoting.
4. A TypeScript client with the same public concepts and behavior.
5. A generic EVM source plus Monad and Arbitrum real-time sources implementing
   one common interface. Base is a profile of the generic EVM source.
6. Bounded client update streams plus optional Redis restart checkpoints owned only by the runnable indexer.
7. Shared golden vectors and differential tests proving bit-for-bit parity with Solidity.

The pure math packages must have no RPC, Redis, filesystem, clock, worker, or network dependency. The client
packages own indexing and state freshness, then pass an immutable snapshot into pure math.

The first implementation target is quote parity. Transaction construction, signing, submission, allowance
management, and custody are out of scope unless added as a separate package later.

## 2. Canonical Solidity source map

### Public API and orchestration

| Area | Canonical source |
| --- | --- |
| Core composition and immutable CASH | [`Core.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/Core.sol#L13-L20), [`Cash.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/modules/Cash.sol#L7-L16) |
| Operator update path and packed write | [`Lanes.update_0x01e44214`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/modules/Lanes.sol#L34-L81) |
| Public `quoteExactIn` and `quoteExactOut` sentinels | [`Lanes.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/modules/Lanes.sol#L89-L102) |
| Swap settlement and emitted quote fields | [`Lanes.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/modules/Lanes.sol#L104-L136), [`LanesLib._settleSwap`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/LanesLib.sol#L233-L251) |
| Lanes ABI and events | [`ILanes.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/interfaces/ILanes.sol#L8-L67) |

### Quote engine and pure helpers

| Area | Canonical source |
| --- | --- |
| `Lane`, `QuoteResult`, quote input structs | [`LanesLib.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/LanesLib.sol#L26-L95) |
| Quote entry points and result assembly | [`quoteExactIn`, `quoteExactOut`, `_quote`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/LanesLib.sol#L125-L193) |
| Direct-vs-route selection and lane validation | [`_getQuote`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/LanesLib.sol#L253-L302) |
| Direct quote | [`_getDirectQuote`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/LanesLib.sol#L304-L330) |
| Routed quote through CASH | [`_getRouteQuote`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/LanesLib.sol#L332-L365) |
| Fee selection, spread, principal valuation | [`LanesLib.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/LanesLib.sol#L367-L475) |
| Lane validity predicate | [`_validate`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/LanesLib.sol#L478-L480) |
| Anchor, fee, slippage, weighted slippage helpers | [`LaneHelpers.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/utils/LaneHelpers.sol#L7-L105) |
| Partner fee adjustment | [`PartnerHelpers.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/utils/PartnerHelpers.sol#L18-L34) |
| Partner/treasury split | [`SwapHelpers.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/utils/SwapHelpers.sol#L7-L23) |
| `BPS`, `WAD`-adjacent constants | [`Constants.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/utils/Constants.sol#L4-L10) |
| Exact Solady multiplication/division semantics | [`FixedPointMathLib.fullMulDiv`](https://github.com/Vectorized/solady/blob/v0.1.26/src/utils/FixedPointMathLib.sol#L452-L560), [`mulDiv`](https://github.com/Vectorized/solady/blob/v0.1.26/src/utils/FixedPointMathLib.sol#L593-L606) |

### State required by quotes

| Area | Canonical source |
| --- | --- |
| Packed lane word layout | [`LaneSlot0.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/types/LaneSlot0.sol#L7-L60) |
| ERC-7201 lanes state | [`LanesLib.State`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/LanesLib.sol#L48-L62) |
| Partner state and quote getters | [`PartnersLib.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/PartnersLib.sol#L9-L41), [`partnerFeeBps`, `feeBpsForRouter`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/PartnersLib.sol#L128-L138) |
| Partner events/getters | [`IPartners.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/interfaces/IPartners.sol#L6-L43) |
| Reserve state and `totalPrincipalAmount` | [`ReservesLib.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/ReservesLib.sol#L8-L38), [`totalPrincipalAmount`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/ReservesLib.sol#L128-L130) |
| Reserve public getter | [`IReserves.reserves`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/interfaces/IReserves.sol#L7-L16) |
| Principal enters active liquidity | [`PositionManagerLib.executeDeposit`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/PositionManagerLib.sol#L200-L222) |
| Principal leaves active liquidity | [`PositionManagerLib.executeWithdrawal`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/libraries/PositionManagerLib.sol#L267-L293) |
| Principal-changing events | [`IPositionManager.sol`](https://github.com/Lunarbase-Lab/lunarbase-contracts/blob/cfeb6b86f425c5207f3cf80c8b40adde07d6a60b/src/interfaces/IPositionManager.sol#L44-L69) |

## 3. Contract quote model

Every lane prices one non-CASH asset against the immutable `CASH` asset. There is no independently stored
asset-to-asset lane:

- `asset -> CASH` and `CASH -> asset` use one lane directly.
- `assetIn -> assetOut`, where neither asset is CASH, routes as `assetIn -> CASH -> assetOut`.

The pushed price convention is:

```text
price = amount of CASH per one unit of lane asset
scale = 10 ** (cashDecimals - assetDecimals) * 1e18
```

All function inputs and outputs are raw token integer units. Off-chain code must not normalize amounts to a
floating-point decimal representation.

`router` is `msg.sender` in the public Solidity quote functions. It changes both fee adjustment and the
partner/treasury split. Every off-chain quote request therefore must contain `router` explicitly.

The complete Solidity `QuoteResult` is:

```text
amountIn
amountOut
feeAsset
feeAmount
partnerFee
treasuryFee
```

The public contract getters expose only `amountOut` for exact-in and `amountIn` for exact-out. The off-chain
libraries should expose the complete result and provide compatibility helpers for the public scalar return.

## 4. Integer domain and exact arithmetic

### 4.1 Constants

```text
WAD                = 1_000_000_000_000_000_000
BPS                = 1_000_000
SLIPPAGE_SCALE     = 10
MAX_SLIPPAGE_BPS   = BPS / SLIPPAGE_SCALE = 100_000
U256_MAX           = 2**256 - 1
```

LunarBase's `BPS` denominator is one million, not ten thousand. Examples:

```text
1 conventional bp = 100 protocol BPS
1%                = 10_000 protocol BPS
100%              = 1_000_000 protocol BPS
```

### 4.2 Required primitives

Implement separate primitives because Solidity uses two different overflow contracts:

```text
fullMulDivDown(x, y, d) = floor(x * y / d), using a full 512-bit product
fullMulDivUp(x, y, d)   = ceil(x * y / d), using a full 512-bit product
mulDivDown256(x, y, d)  = floor(x * y / d), but revert if x*y overflows uint256
```

All three fail when `d == 0`. The full-width variants also fail when the final quotient does not fit in
`uint256`. `fullMulDivUp` fails if rounding the maximum quotient upward overflows.

This distinction matters because anchor, slippage, and spread helpers use Solady `fullMulDiv*`, while
`splitFee` uses Solady `mulDiv` with a checked 256-bit intermediate product.

Rust should use a real `U256` public domain and a `U512` or equivalent full product internally. TypeScript
must use `bigint`, followed by explicit `0 <= value <= U256_MAX` checks. JavaScript `number` is forbidden in
all monetary, price, fee, block-number, and packed-word paths.

All ordinary additions/subtractions in Solidity 0.8.35 are checked unless the source explicitly uses
`unchecked`. The ports must return a typed arithmetic error at the same boundary instead of wrapping.

## 5. Packed `Lane.slot0`

The lane's first storage word has this exact layout:

| Bits | Width | Field | Input type |
| --- | ---: | --- | --- |
| `[0, 112)` | 112 | `price` | `uint112` |
| `[112, 132)` | 20 | `askFeeBps` | `uint20` |
| `[132, 152)` | 20 | `bidFeeBps` | `uint20` |
| `[152, 159)` | 7 | `pricePushThreshold` | `uint7` |
| `[159, 160)` | 1 | `thresholdEnabled` | `bool` |
| `[160, 200)` | 40 | `latestUpdateBlock` | `uint40` |
| `[200, 201)` | 1 | `exists` | `bool` |
| `[201, 202)` | 1 | `paused` | `bool` |
| `[202, 210)` | 8 | `blockDelay` | `uint8` |
| `[210, 242)` | 32 | `slippageKBps` | `uint32` |
| `[242, 243)` | 1 | `corrupted` | `bool` |
| `[243, 256)` | 13 | reserved/preserved | - |

Decode a field as:

```text
value = (word >> shift) & ((1 << width) - 1)
```

`UpdateCalldata.fees` is a `uint40` where the low 20 bits are ask fee and the high 20 bits are bid fee:

```text
fees = askFeeBps | (bidFeeBps << 20)
```

`update_0x01e44214` replaces price, ask fee, bid fee, and `latestUpdateBlock`; it preserves threshold,
control, and reserved fields. It writes `NUMBER & ((1<<40)-1)` into `latestUpdateBlock` and emits the complete
updated word in `LaneUpdated(asset, slot0)`. A strict threshold excess in either price direction zeroes price,
sets `paused` and `corrupted`, and emits `LaneCorruptedSet`. A corrupted lane ignores later pushes until the
owner clears its latch.

The off-chain libraries need `decodeLaneSlot0` and `encodeLaneSlot0` utilities plus property tests for every
field boundary. The event reducer should replace the entire cached `slot0` with the event value, not patch
only price and fees.

## 6. Lane validity

A lane is usable only when all conditions are true:

```text
lane.exists
!lane.paused
executionBlockNumber <= latestUpdateBlock + blockDelay
```

`blockDelay` is an inclusive quote TTL: an update at block `N` is valid through
`N + blockDelay` and becomes stale at `N + blockDelay + 1`. For a route, both
lanes must be valid.

`addLane` sets the packed existence bit. `setBlockDelay`, `setSlippageKBps`, and `setLaneCorrupted` mutate
their packed fields and emit dedicated events. Threshold fields are configured with lane state and preserved
by price pushes. Off-chain state therefore retains the complete packed word.

`executionBlockNumber` means the number visible to the EVM `NUMBER` opcode, not necessarily the native block
height exposed by a chain's JSON-RPC block header. This is especially important on Arbitrum. See section 14.

## 7. Anchor conversion

Let `P = price`, `A = amount`, and `W = WAD`.

If `P == 0`, every anchor helper returns zero before division.

### Exact-in

```text
CASH -> asset: floor(A * W / P)
asset -> CASH: floor(A * P / W)
```

### Exact-out

```text
CASH -> asset: ceil(A * P / W)  // A is desired asset output
asset -> CASH: ceil(A * W / P)  // A is desired CASH output
```

The exact-out path always rounds required input upward.

## 8. Router-adjusted lane fee

Each direct leg begins with a raw fee from packed slot0:

- `CASH -> asset` uses `askFeeBps`.
- `asset -> CASH` uses `bidFeeBps`.

The raw fee is adjusted for `router` by `calculateFeeBpsForRouter`:

```text
fee = min(rawFeeBps, BPS)

if whitelist[router]:
    effectiveFee = fee
else:
    effectiveFee = min(BPS, fee * blacklistFeeMultiplier)
```

The Solidity implementation uses a division guard before multiplication to avoid overflow. Preserve the
observable behavior. A zero `blacklistFeeMultiplier` makes the effective lane fee zero for a non-whitelisted
router. Do not assume an implicit multiplier of one.

For a route:

```text
routeBidFee = adjusted bid fee of assetIn lane
routeAskFee = adjusted ask fee of assetOut lane
```

Partner fee configuration is separate from this adjustment. `partners[router][asset].fee` does not change
the spread; it only determines how the already calculated fee is split between partner and treasury.

## 9. Principal-based slippage

Quotes use active LP principal, not the contract token balance and not `assetReserve`.

For one lane:

```text
principalCashValue = floor(totalPrincipalAmount[asset] * price / WAD)
```

If `totalPrincipalAmount == 0` or the conversion rounds to zero, the quote is unavailable.

For a direct quote, `swapCashValue` is always the anchor-side CASH amount:

| Direction | Mode | `swapCashValue` |
| --- | --- | --- |
| CASH -> asset | exact-in | requested CASH input |
| CASH -> asset | exact-out | anchor CASH input |
| asset -> CASH | exact-in | anchor CASH output |
| asset -> CASH | exact-out | requested CASH output |

The lane slippage calculation is sequentially rounded:

```text
if swapCashValue == 0 or principalCashValue == 0 or slippageKBps == 0:
    slippageBps = 0
else:
    rawBps = ceil(swapCashValue * slippageKBps / principalCashValue)
    slippageBps = ceil(rawBps / SLIPPAGE_SCALE)
    slippageBps = min(slippageBps, MAX_SLIPPAGE_BPS)
```

Implement both ceilings exactly, even if an algebraic simplification looks equivalent.

### Routed slippage

For a route, compute both principal CASH values independently with floor rounding:

```text
P1 = floor(totalPrincipalIn  * priceIn  / WAD)
P2 = floor(totalPrincipalOut * priceOut / WAD)
```

If either is zero, the route is unavailable. Then:

```text
Ptotal = P1 + P2
weightedK = ceil(P1 * K1 / Ptotal) + ceil(P2 * K2 / Ptotal)
weightedK = min(weightedK, BPS)
routeSlippageBps = quoteLaneSlippageBps(intermediateCashAmount, Ptotal, weightedK)
```

The sum of two independently rounded-up terms is intentional and can differ by one from rounding a combined
fraction once.

## 10. Direct quote math

First calculate `anchorAmount` using section 7, `effectiveFeeBps` using section 8, and `slippageBps` using
section 9.

Define the spread helpers:

```text
exactInSpread(anchor, bps)  = 0 if anchor==0 or bps==0
                            = ceil(anchor * bps / (BPS + bps)) otherwise

exactOutSpread(anchor, bps) = 0 if anchor==0 or bps==0
                            = ceil(anchor * bps / BPS) otherwise
```

For exact-in:

```text
feeAmount         = exactInSpread(anchorAmount, effectiveFeeBps)
totalSpreadAmount = exactInSpread(anchorAmount, effectiveFeeBps + slippageBps)
slippageAmount    = totalSpreadAmount - feeAmount
amountIn          = requested amountIn
amountOut         = anchorAmount - totalSpreadAmount
feeAsset          = assetOut
```

If `totalSpreadAmount >= anchorAmount`, Solidity returns a zero `QuoteResult` rather than subtracting.

For exact-out:

```text
feeAmount         = exactOutSpread(anchorAmount, effectiveFeeBps)
totalSpreadAmount = exactOutSpread(anchorAmount, effectiveFeeBps + slippageBps)
slippageAmount    = totalSpreadAmount - feeAmount
amountIn          = anchorAmount + totalSpreadAmount
amountOut         = requested amountOut
feeAsset          = assetIn
```

The `cashToAsset` boolean parameter accepted by the current Solidity fee helper is unused. Ports may keep it
for source-level parity, but it must not alter the result.

## 11. Routed quote math

### Exact-in route

```text
intermediateCash = floor(amountIn * priceIn / WAD)
anchorAmount     = floor(intermediateCash * WAD / priceOut)
```

### Exact-out route

```text
intermediateCash = ceil(amountOut * priceOut / WAD)
anchorAmount     = ceil(intermediateCash * WAD / priceIn)
```

Use `intermediateCash` as the routed `swapCashValue` for slippage.

Base routed fee:

```text
routeFeeBps = routeBidFeeBps + routeAskFeeBps
```

Total routed fee plus slippage is intentionally:

```text
(routeBidFeeBps + routeSlippageBps)
+ (routeAskFeeBps + routeSlippageBps)
```

Therefore routed slippage is added once per leg, or twice in the combined spread.

For exact-in:

```text
feeAmount         = exactInSpread(anchorAmount, routeBidFeeBps + routeAskFeeBps)
totalSpreadAmount = exactInSpread(
    anchorAmount,
    routeBidFeeBps + routeAskFeeBps + 2 * routeSlippageBps
)
amountOut         = anchorAmount - totalSpreadAmount
feeAsset          = assetOut
```

For exact-out, use `exactOutSpread`, set `amountIn = anchorAmount + totalSpreadAmount`, and set
`feeAsset = assetIn`.

## 12. Partner and treasury split

After calculating `feeAmount`, read `partnerFeeBps = partners[router][feeAsset].fee`.

```text
if feeAmount == 0:
    partnerFee = 0
    treasuryFee = 0
else:
    candidatePartnerFee = floor(anchorAmount * partnerFeeBps / BPS)
    partnerFee = min(candidatePartnerFee, feeAmount)
    treasuryFee = feeAmount - partnerFee
```

Important details:

- The partner calculation uses `anchorAmount`, not `feeAmount`.
- It rounds down.
- It uses the checked-256-bit `mulDiv`, not the full-precision variant.
- Partner fee is capped by the actual fee, so treasury fee cannot underflow.
- `partnerFeeBps` is keyed by the fee asset, which differs between exact-in and exact-out.
- Slippage is never credited as treasury or partner fee. It remains in `assetReserve` after settlement.

## 13. Public edge behavior

The pure library should retain a rich internal status, but the Solidity-compatible scalar wrappers must
return these exact values:

| Condition | `quoteExactIn` public result | `quoteExactOut` public result |
| --- | ---: | ---: |
| amount is zero | `0` | `0` |
| `assetIn == assetOut` | `0` | `U256_MAX` |
| missing/paused/stale lane | `0` | `U256_MAX` |
| zero price after preceding arithmetic succeeds | `0` | `U256_MAX` |
| required principal CASH value is zero | `0` | `U256_MAX` |
| exact-in spread consumes anchor | `0` | not applicable |

The internal `LanesLib` reverts for equal assets, but the module intercepts equal assets before calling it.
The external client should mirror the module because that is the user-facing contract behavior.
Lane validation does not preflight a zero price. Route conversions must retain
Solidity evaluation order, so an earlier opposite-leg overflow still reverts
instead of being hidden by a later zero-price sentinel.

Recommended internal result:

```text
QuoteOutcome = Available(QuoteResult) | Unavailable(UnavailableReason)
```

Recommended compatibility helpers:

```text
solidityExactInAmount(outcome)  -> 0 when unavailable
solidityExactOutAmount(outcome) -> U256_MAX when unavailable, except zero request -> 0
```

Arithmetic overflow/division-by-zero is an error/revert equivalent, not an unavailable quote.

## 14. Exact quote-state dependency graph

### Required state

| State | Key | Why needed | Bootstrap getter | Live events |
| --- | --- | --- | --- | --- |
| CASH | deployment | direct/route selection | `cash()` | immutable |
| Lane slot0 | asset | price, ask, bid, latest update | `lane(asset)` | `LaneUpdated` |
| Lane controls | asset | exists, paused, quote TTL, slippage K, corruption | packed `lane(asset)` | `LaneAdded`, `LaneRemoved`, `SlippageKSet`, `BlockDelaySet`, `LaneCorruptedSet` |
| Free output reserve | asset/CASH | settlement availability | `reserves(asset).assetReserve` | `Sync` |
| Active principal | asset | principal CASH value/slippage | `reserves(asset).totalPrincipalAmount` | `DepositExecuted`, `WithdrawalExecuted` |
| Whitelist | router | base fee adjustment | `whitelist(router)` | `WhitelistSet` |
| Blacklist multiplier | deployment | base fee adjustment | `blacklistFeeMultiplier()` | `BlacklistFeeMultiplierSet` |
| Partner fee | router + feeAsset | fee split | `partners(router, feeAsset).fee` | `PartnerInfoSet`, `PartnerFeeSet` |
| EVM block context | quote snapshot | lane delay predicate | source-specific | source head |

### State not required for quote parity

- `treasuryFees`
- `partnerFees` reserve bucket
- `escrowedAssets`
- partner `cumFees`, `operator`, and withdraw timestamp
- position yield, penalties, liabilities, and cooldowns
- LP authority indexes
- operator addresses used to authorize price pushes

The quote checks the free reserve of `assetOut`. Exact-out requires
`amountOut <= outputReserve`; exact-in requires `amountOut + feeAmount <= outputReserve`, matching settlement
where output-denominated fees become liabilities before transfer. It returns the normal unavailable sentinel
instead of reverting when the reserve is insufficient.

### Principal lifecycle

Only active, executed deposits affect quote slippage:

```text
DepositRequested          -> no totalPrincipalAmount change
DepositRequestCancelled   -> no totalPrincipalAmount change
DepositExecuted           -> totalPrincipalAmount += principalAmount
WithdrawalRequested       -> no totalPrincipalAmount change
WithdrawalRequestCancelled-> no totalPrincipalAmount change
WithdrawalExecuted        -> totalPrincipalAmount -= principalAmount
```

In particular, `requestWithdrawal` does not remove principal from reserves.

## 15. Event reducer

Apply logs in `(block, transactionIndex, logIndex)` order. The reducer is single-writer even if decoding is
parallel.

| Event | Quote-state transition |
| --- | --- |
| `LaneAdded(asset)` | Set `lane.exists = true`; preserve any previously observed slot0/meta fields. |
| `LaneRemoved(asset)` | Delete/reset the entire lane, matching Solidity `delete`. |
| `LaneUpdated(asset, slot0)` | Replace the entire packed word. Cache the update even if the lane does not currently exist. |
| `LaneCorruptedSet(asset, _, corrupted)` | Apply the corruption latch, including price-zero/pause semantics. |
| `SlippageKSet(asset, _, newK)` | Replace the packed slippage-K field. |
| `BlockDelaySet(asset, _, ttl)` | Replace the packed quote TTL field. |
| `PartnerInfoSet(router, asset, fee, operator)` | Set partner fee; operator is optional non-quote metadata. |
| `PartnerFeeSet(router, asset, fee)` | Set partner fee. |
| `WhitelistSet(router, flag)` | Set router whitelist flag. |
| `BlacklistFeeMultiplierSet(multiplier)` | Replace global multiplier. |
| `DepositExecuted(_, _, asset, principal)` | Checked add to active principal. |
| `WithdrawalExecuted(_, _, asset, principal, ...)` | Checked subtract from active principal. |
| `Sync(asset, assetReserve, cashReserve)` | Replace lane free reserve and the shared CASH free reserve. |
| `Upgraded(implementation)` | Fail closed and revalidate the ERC-1967 implementation before recovery. |
| `SwapExecuted(...)` | No quote-state change in the pinned implementation. |

All other current events can be ignored by the quote reducer.

Event-only replay is not a complete genesis mechanism because the initial packed lane words, reserves, and
router profile are not fully reconstructable from creation logs. Every process starts from a block-hash
pinned state snapshot or a validated complete checkpoint and then replays events.

## 16. Bootstrap, live handoff, and recovery

Every deployment config must contain:

```text
network family
chainId
Core address
deployment block
expected ERC-1967 implementation address and implementation code hash
contract compatibility version / pinned commit
HTTP RPC URL
real-time source configuration
single configured router and expected whitelist profile
optional explicit lane assets
```

Recommended race-free startup:

1. Start the network live source and buffer normalized updates in a bounded queue.
2. Select a canonical snapshot block `B` and record its hash.
3. Discover lane assets from `LaneAdded`/`LaneRemoved` history starting at deployment block, or use an explicit
   configured asset list and verify it against logs.
4. At exact block hash `B`, read the ERC-1967 implementation slot, verify its runtime code hash, then read
   `cash()`, packed `lane(asset)`, and `reserves(asset)` for every lane.
5. Fetch whitelist, blacklist multiplier when needed, and partner fees for the single configured router at
   the same block hash.
6. Atomically publish the snapshot.
7. Apply buffered updates strictly after `B` in cursor order.
8. Persist a canonical checkpoint and start normal operation.

On disconnect, detected sequence gap, expired ring payload, impossible arithmetic transition, cursor
regression, or block-hash mismatch:

1. Mark state not ready; do not serve a silently stale quote as fresh.
2. Preserve the last good canonical checkpoint.
3. Backfill canonical logs from the last good cursor when possible.
4. If continuity cannot be proven, rebuild from a new block-hash-pinned snapshot.
5. Resume only after implementation identity and state invariants pass.

## 17. Network-independent client model

Use a network family enum in both languages:

```text
Network::Base
Network::Monad
Network::Arbitrum
```

Default mainnet chain IDs are Base `8453`, Monad `143`, and Arbitrum One `42161`, but `chainId` must remain
configurable. Do not encode testnet choice into quote math.

Normalize network confidence into one stable enum:

```text
Commitment::Realtime   // preconfirmed/proposed/unsafe source head
Commitment::Canonical  // sealed/canonical L2 state
Commitment::Finalized  // configured strongest level
```

Normalize all sources into the same records:

```text
ChainCursor {
    chainId,
    blockNumber,
    executionBlockNumber,
    blockHash?,
    transactionIndex?,
    logIndex?,
    sourceSequence?,
    sourceSubIndex?,
    commitment
}

ContractLog {
    address,
    topics,
    data,
    removed,
    cursor
}

ChainUpdate = Head | Log | Reorg | Gap
```

`sourceSequence` and `sourceSubIndex` carry network-specific ordering numbers without changing the public
shape. Consumers must not inspect them to decide network behavior.
`executionBlockNumber` is the EVM-visible `NUMBER` context and must not be
derived from `sourceSequence`.

Recommended Rust boundary:

```rust
pub trait ChainDataSource: Send + Sync {
    fn network(&self) -> Network;
    fn snapshot(
        &self,
        deployment: &DeploymentConfig,
    ) -> impl Future<Output = Result<BootstrapSnapshot, SourceError>> + Send;
    fn backfill(
        &self,
        request: BackfillRequest,
    ) -> impl Future<Output = Result<Vec<ContractLog>, SourceError>> + Send;
    fn subscribe(
        &self,
        filter: ContractFilter,
    ) -> impl Future<Output = Result<SourceStream, SourceError>> + Send;
    fn canonical_head(
        &self,
    ) -> impl Future<Output = Result<ChainCursor, SourceError>> + Send;
    fn validate_checkpoint(
        &self,
        checkpoint: &Checkpoint,
    ) -> impl Future<Output = Result<bool, SourceError>> + Send;
}
```

The runtime is generic over the concrete source, so this native RPITIT
boundary does not need `async-trait` boxing or a `dyn ChainDataSource`.

Recommended TypeScript boundary:

```ts
export interface ChainDataSource {
  readonly network: Network;
  snapshot(deployment: DeploymentConfig): Promise<BootstrapSnapshot>;
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]>;
  subscribe(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate>;
  canonicalHead(): Promise<ChainCursor>;
  validateCheckpoint(checkpoint: Checkpoint): Promise<boolean>;
}
```

The network-independent `QuoteIndexer` owns bootstrap, ABI decoding, reducer
order, state publication, and recovery. Persistence is outside
`lunarbase-client`. Source packages export concrete `ChainDataSource`
implementations and source constructors; they do not wrap or duplicate the
common client lifecycle. `DeploymentConfig` contains only chain/Core/router
identity and quote policy; endpoint configuration belongs to each source and
to the runnable indexer composition root.

## 18. Base source

Preferred implementation: the Base Flashblocks profile of `EvmRpcSource`
(`EvmRpcSource::base_flashblocks` in Rust and
`createBaseFlashblocksSource` in TypeScript).

Use a Flashblocks-aware WebSocket/RPC endpoint:

- Subscribe to [`pendingLogs`](https://docs.base.org/base-chain/api-reference/flashblocks-api/pendingLogs)
  filtered by Core address and relevant topics for low-volume sub-block contract updates.
- Subscribe to `newHeads`, which the Flashblocks endpoint emits at roughly
  200 ms cadence. Multiple heads can advance one L2 block height; their
  changing partial hash is ordered by `sourceSequence` and is not a reorg
  while the parent hash remains stable.
- Use standard RPC/log queries for canonical backfill and reconciliation.

Do not decode the larger `newFlashblocks` payload unless a demonstrated
quote-critical requirement cannot be satisfied by `pendingLogs + newHeads`.
The ordered reducer directly applies normalized preconfirmation logs. A gap,
reorg, removed log, or discontinuity revokes readiness and triggers canonical
RPC recovery; there is no separate provisional overlay.

For production, support either a provider Flashblocks endpoint or a local Flashblocks-aware Base node. The
public endpoints are rate limited.

## 19. Monad source

All Monad-specific code belongs to `lunarbase-source-monad`. The Rust package
provides both the portable parser WebSocket and a Linux native shared-memory
reader for deployment beside the execution node.

Official setup and SDK references:

- [Set up Execution Events](https://docs.monad.xyz/guides/execution-events/setup)
- [Consume Execution Events in Rust](https://docs.monad.xyz/guides/execution-events/consume-rust)
- [Category Labs Monad execution repository](https://github.com/category-labs/monad)

The native feature uses the official `release/exec-events-sdk-v1.0` releases of
`monad-event-ring` and `monad-exec-events`, opens the configured hugetlbfs ring
path, rewinds to the current `BlockStart`, and consumes:

- `BlockStart` / block-end and commitment events for cursors,
- transaction boundaries/hashes,
- `TxnLog` for Solidity events,
- finalized/verified block signals for commitment promotion.

Filter Core address and topics before allocation-heavy ABI decoding.

The local reference parser is:

- repository: [ThomasAqu1nas/monad-exec-events-parser](https://github.com/ThomasAqu1nas/monad-exec-events-parser)
- local checkout: `/Users/ando/Documents/work/lunar_base/monad-exec-events-parser`
- relevant API and gap semantics: `README.md`

The reference demonstrates a dedicated ring-reader thread, bounded channels, parallel payload decoding,
ordered processing, health metrics, and WebSocket subscriptions for `logs`, `newHeads`, `alerts`, and raw
execution events. Its server retains no replay history: a sequence gap or close code `1013` requires a client
resnapshot from an authoritative RPC source.

TypeScript uses `@lunarbase/source-monad` to consume the parser/sidecar over
WebSocket. It does not independently bind hugetlbfs or the native event ring.

On `EventNextResult::Gap` or expired payload, emit normalized `Gap`, reset the ring reader, discard the
provisional state, and recover through RPC snapshot/backfill. Never continue as though the stream were
complete.

## 20. Arbitrum source

Preferred implementation: `ArbitrumNitroSource` connected to a local Nitro node via WebSocket or IPC.

Run the Nitro node against the sequencer feed, optionally through one local feed relay per data center, then
subscribe to filtered EVM `logs` and `newHeads` on the executed local node state.

Primary references:

- [Run a full Nitro node](https://docs.arbitrum.io/run-arbitrum-node/run-full-node)
- [Run a feed relay](https://docs.arbitrum.io/run-arbitrum-node/run-feed-relay)
- [Read the sequencer feed](https://docs.arbitrum.io/run-arbitrum-node/sequencer/read-sequencer-feed)
- [Arbitrum data flow](https://docs.arbitrum.io/run-arbitrum-node/data-availability)

The raw sequencer feed contains accepted and ordered L2 messages/transactions. It does not directly provide
ready EVM receipts and Solidity logs. A standalone client would have to execute the messages to derive those
logs. Therefore v1 must consume executed state from Nitro rather than treating the raw feed as a log stream.

The source should:

1. Subscribe to Core logs and heads on the local Nitro node.
2. Treat the fastest head as `Commitment::Realtime`.
3. Backfill missed logs from the local node after reconnect.
4. Promote state as configured canonical/finality signals become available.
5. Respect standard `removed` logs and block-hash discontinuities.

Arbitrum warning: Solidity/Yul `block.number`/`NUMBER` on Arbitrum represents an approximate parent-chain
block number, while the JSON-RPC block number and `ArbSys.arbBlockNumber()` represent the L2 block height.
See [Arbitrum block numbers and time](https://docs.arbitrum.io/arbitrum-essentials/arbitrum-vs-ethereum/block-numbers-and-time#ethereum-or-parent-chain-block-numbers-within-arbitrum).

Because `blockDelay` is configurable, `ArbitrumNitroSource` must provide the EVM-visible parent block number
from the feed/block context. Do not compare packed `latestUpdateBlock` to the Arbitrum L2 height.

## 21. Redis and process model

Redis is optional restart acceleration owned only by `lunarbase-indexer`.
Embeddable client packages have no Redis dependency. Use one schema-v4 key:

```text
lunarbase:v4:{chainId}:{coreAddress}:{routerAddress}
```

The value is one versioned JSON DTO containing complete state. U256 values are
decimal or fixed-width hexadecimal strings, never JSON numbers. Only `GET` and
an atomic full-value `SET` are required; the key has no TTL.

At startup validate schema/math version, Core implementation identity, chain/Core/router
identity, router profile, and canonical checkpoint block hash. If a checkpoint
is missing, malformed, incompatible, forked, or cannot be safely recovered,
discard it and take a full RPC snapshot.

All replicas independently index state and serve quotes. There is no writer
lease, leader election, fencing, standby role, Redis Stream, update-dedup key,
or cross-language binary update codec. Concurrent checkpoint `SET`s are
best-effort because every value is complete and is revalidated at startup.

Redis failure must not revoke readiness after the indexer is running. The
process logs and counts checkpoint failures and continues with in-memory state.

## 22. Memory, concurrency, and backpressure

The ordering-critical reducer must remain single-threaded. Parallelize only transport/decode work that can be
reordered before reduction.

Rust process:

- one source I/O task or dedicated Monad ring-reader thread,
- one bounded runtime queue and bounded transport reorder buffer,
- one reducer task,
- hot state under a short synchronous `RwLock`,
- shared quote read guards without cloning state,
- one periodic checkpoint task only in the runnable indexer.

TypeScript process:

- one ordered reducer in the main event loop,
- bounded async source queue,
- worker threads only for demonstrated CPU-heavy decoding/batches,
- synchronous quote/quoteMany calls within one event-loop turn,
- no worker per asset or per request.

State size is bounded by one configured deployment/router and discovered lane
assets. Do not maintain dynamic router maps. Principal, slippage, delay, and
compact flags are colocated with the lane's packed `slot0`.

When a queue cannot keep up, block/backpressure where the source permits it. If the source ring or broadcast
can overwrite data, detect the gap and resnapshot. Dropping updates silently is forbidden.

## 23. Monorepo layout

```text
lunarbase-math/
  Cargo.toml
  package.json
  pnpm-workspace.yaml
  README.md
  SPECIFICATION.md

  crates/
    lunarbase-math/             # pure Rust U256 math and quote engine
    lunarbase-client/           # universal Rust runtime and reducer
    lunarbase-source-evm/       # generic EVM source + Base profile
    lunarbase-source-monad/     # parser/native Monad sources
    lunarbase-source-arbitrum/  # executed Nitro source
    lunarbase-indexer/          # runnable Rust HTTP service
    lunarbase-tools/            # E2E, load, and live validators

  packages/
    math/                       # pure TypeScript bigint math and quote engine
    client/                     # universal TypeScript runtime
    source-evm/                 # generic EVM source + Base profile
    source-monad/               # Monad parser source
    source-arbitrum/            # executed Nitro source

  abi/
    Core.json                   # ABI pinned to contract commit/code version

  fixtures/
    quote-vectors.json          # shared deterministic vectors
    base-flashblocks/
    monad-exec-events/
    arbitrum-nitro/

  config/
    base.toml
    monad.toml
    arbitrum.toml
    prometheus-alerts.yml
```

Canonical Solidity FFI remains in the sibling `lunarbase-contracts`
repository; this repository must not duplicate the contract harness.

## 24. Pure-library public API

Both implementations should expose equivalent concepts and field names.

```text
QuoteRequest {
    assetIn,
    assetOut,
    amount,
    mode: ExactIn | ExactOut
}

LaneState {
    slot0: U256,
    assetReserve: u128,
    totalPrincipalAmount: u128
}

FeeProfile {
    whitelisted,
    blacklistFeeMultiplier,
    partnerFeeBpsByAsset
}
```

Core functions:

```text
quote(request, executionBlockNumber, state) -> Result<QuoteOutcome, QuoteError>
decodeLaneSlot0(word)                  -> LaneSlot0
encodeLaneSlot0(fields)                -> Word
```

Keep lower-level pure helpers public enough for direct vector testing:

```text
fullMulDivDown / fullMulDivUp / mulDivDown256
quoteLaneExactIn / quoteLaneExactOut
quoteLaneSlippageBps
quoteLaneWeightedSlippageKBps
quoteLaneExactInFee / quoteLaneExactOutFee
calculateFeeBpsForRouter
splitFee
```

## 25. High-level client API

Common embeddable behavior:

```text
connect(config, dataSource, optionalCheckpoint)
quote(request)
quoteMany(requests)
health()
checkpoint()
shutdown()
```

High-level quote response:

```text
ClientQuote {
    outcome,
    cursor,
    executionBlockNumber
}
```

Router/profile, execution block, and freshness are runtime-owned. Callers
cannot override them per request. A source gap revokes readiness; fallback RPC
state must not be labeled realtime until recovery completes.

## 26. Bit-for-bit parity rules

1. Never use floating point.
2. Preserve every floor/ceil location; do not combine separately rounded operations.
3. Preserve checked uint256 addition/subtraction and final-result bounds.
4. Distinguish Solady full-width `fullMulDiv` from checked-product `mulDiv`.
5. Enforce input-width checks for `uint112`, `uint20`, `uint40`, `uint128`, and packed words.
6. Preserve exact-in `totalSpread >= anchor` zero-result behavior.
7. Preserve exact-out `U256_MAX` public sentinel behavior.
8. Preserve whitelist and zero blacklist-multiplier behavior.
9. Preserve routed double application of slippage BPS, one per leg.
10. Preserve independent ceil terms in weighted route K.
11. Preserve fee-asset selection before loading partner fee.
12. Preserve partner split based on anchor, with floor and cap.
13. Apply the output-reserve check after amount construction and before partner/treasury splitting.
14. Include contract commit and ERC-1967 implementation identity in every persisted state namespace.

## 27. Testing strategy

### Shared golden vectors

Generate one canonical fixture format consumed by Rust and TypeScript. All U256 values must be decimal strings
or fixed-width hex strings, never JSON numbers.

Required vector groups:

- direct CASH-to-asset and asset-to-CASH,
- route asset-to-asset,
- exact-in and exact-out,
- one-wei rounding boundaries,
- zero amount, zero price, zero principal, equal assets,
- missing/paused/stale lane,
- raw fee over BPS clamping,
- whitelist true/false and multipliers 0, 1, large, and overflow-guard boundary,
- slippage zero, cap, and sequential-ceil boundaries,
- weighted route K where two ceils differ from one combined ceil,
- routed double slippage,
- spread consuming exact-in anchor,
- partner fee zero, partial, and capped to all fee,
- every slot0 field boundary and preserved high bits,
- U256 overflow/revert boundaries,
- token decimal asymmetry expressed through pushed price.

### Solidity differential oracle

Pin `lunarbase-contracts` at the reference commit. The Foundry harness should expose the full `QuoteResult`,
not only the public scalar. For deterministic and fuzzed inputs:

```text
Solidity result/error == Rust result/error == TypeScript result/error
```

Use the exact Solady v0.1.26 implementation imported by the contract. Do not rewrite the oracle formulas in
the test generator, because that would only compare two copies of the same off-chain mistake.

### Reducer and integration tests

- Replay canonical event sequences and compare with block-tagged getters.
- Verify `requestWithdrawal` does not reduce principal.
- Verify `SwapExecuted` is not part of quote-critical replay.
- Simulate reordered, removed, and missing logs.
- Simulate Monad ring gap/expired payload and mandatory resnapshot.
- Replay official Base `pendingLogs + newHeads` payload fixtures.
- Simulate Arbitrum WS disconnect and canonical backfill.
- Verify valid, incompatible, stale/forked, and unavailable Redis bootstrap.
- Run two simultaneously ready process replicas and compare state/quotes.
- Prove quote-path source and Redis call counts remain zero.
- Verify `quoteMany` uses one cursor and equals sequential quotes.

## 28. Acceptance criteria

Math v2 is complete when:

- all shared vectors pass in Rust, TypeScript, and Solidity reference tests;
- property/differential tests cover all direct/route modes and overflow outcomes;
- no monetary path uses float/JS number;
- packed slot0 round-trips exactly;
- public sentinel wrappers match `Lanes.sol`.

Client v1 is complete when:

- one high-level client API consumes every source through the shared
  `ChainDataSource` contract;
- a real deployment can bootstrap from a block-tagged snapshot and quote without `eth_call` per request;
- every quote carries a cursor and commitment;
- all quote-critical events update in-memory state atomically/in order;
- source gaps stop freshness claims and trigger deterministic recovery;
- Monad Rust can read a colocated event ring, and TypeScript can consume the local sidecar;
- Base consumes `pendingLogs + newHeads`;
- Arbitrum consumes executed local Nitro state fed by the sequencer feed;
- ERC-1967 implementation address or implementation code-hash mismatch fails closed;
- memory and queues are bounded/configurable;
- two replicas can independently remain ready without leader election;
- Redis loss affects restart speed only;
- Monad and Arbitrum remain experimental until their live-validation gates pass.

## 29. Implementation phases

### Phase 1: schemas and pure math

1. Define U256/address/word types and typed errors.
2. Implement slot0 codec and arithmetic primitives in both languages.
3. Port helper math, then direct and route quote engines.
4. Build Solidity oracle and shared vectors.
5. Reach differential parity before starting network code.

### Phase 2: state model and generic RPC source

1. Define normalized cursors, logs, commitments, and reducer.
2. Add deployment/ERC-1967 implementation validation.
3. Implement block-tagged bootstrap and historical log discovery.
4. Implement generic WebSocket/RPC fallback.
5. Add in-memory quote client.

### Phase 3: Runnable indexer and process safety

1. Add schema-versioned Redis checkpoint.
2. Add HTTP quote/batch APIs, metrics, and graceful shutdown.
3. Add multi-replica and Redis-unavailable process E2E.
4. Add gap/reorg/restart integration tests.

### Phase 4: specialized real-time sources

1. Base Flashblocks source.
2. Monad shared-memory Rust source and local sidecar.
3. Arbitrum local Nitro source and feed-relay deployment profile.
4. Cross-network latency and memory benchmarks.

## 30. Current contract constraints and decisions to revisit

1. `latestUpdateBlock` is network-dependent because it stores EVM `NUMBER`. A seconds-based freshness rule
   would be more portable, but this specification mirrors the current contract.
2. `Sync` carries free lane/CASH reserves after settlement and liquidity transitions; consumers must process
   every ordered `Sync`, including both events emitted by a routed swap.
3. Principal is reconstructed from `DepositExecuted` and `WithdrawalExecuted`, which are currently the only
   production principal mutations.
4. A new contract path that mutates reserves or `totalPrincipalAmount` without the corresponding events requires an event and
   reducer update before deployment.
5. Public quote methods return only one amount and use `msg.sender` as router.
   Each off-chain runtime instance therefore has one configured router/profile;
   callers cannot override it per request.
6. Quote parity includes the contract's free output-reserve check; token-specific transfer behavior remains
   outside pure math.
7. Base preconfirmed state, Monad proposed state, and Arbitrum unsafe state are not equivalent to finality.
   The normalized commitment and cursor must always accompany a quote.
8. Redis is optional restart acceleration in `lunarbase-indexer`; it is not
   shared canonical state and is absent from embeddable clients.

## 31. Release gates

1. Re-run the canonical Solidity/Rust/TypeScript FFI whenever math or the
   pinned contract revision changes.
2. Require a Base deployment smoke test before declaring the Base source
   stable.
3. Require a native execution-event-ring soak before publishing Monad as
   production-ready.
4. Require Nitro-node execution-block validation before publishing Arbitrum as
   production-ready.
5. Run `make verify`, process E2E, and the 15-lane/100-pair load profile before
   each release.
