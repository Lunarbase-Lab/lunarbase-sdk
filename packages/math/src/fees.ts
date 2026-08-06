import { BPS, MAX_SLIPPAGE_BPS, SLIPPAGE_SCALE, WAD } from "./constants.js";
import { checkedAdd, checkedSub, fullMulDivDown, fullMulDivUp, mulDivDown256 } from "./arithmetic.js";

/** Calculates the exact-input fee from an anchor value, rounding upward. */
export function quoteLaneExactInFee(anchor: bigint, feeBps: bigint): bigint {
  if (anchor === 0n || feeBps === 0n) return 0n;
  return fullMulDivUp(anchor, feeBps, checkedAdd(BPS, feeBps));
}
/** Calculates the exact-output fee from an anchor value, rounding upward. */
export function quoteLaneExactOutFee(anchor: bigint, feeBps: bigint): bigint {
  if (anchor === 0n || feeBps === 0n) return 0n;
  return fullMulDivUp(anchor, feeBps, BPS);
}
/** Combines bid and ask fees for a two-lane exact-input route. */
export function quoteLaneRouteExactInFee(anchor: bigint, bidFeeBps: bigint, askFeeBps: bigint): bigint {
  return quoteLaneExactInFee(anchor, checkedAdd(bidFeeBps, askFeeBps));
}
/** Combines bid and ask fees for a two-lane exact-output route. */
export function quoteLaneRouteExactOutFee(anchor: bigint, bidFeeBps: bigint, askFeeBps: bigint): bigint {
  return quoteLaneExactOutFee(anchor, checkedAdd(bidFeeBps, askFeeBps));
}
/** Applies whitelist/blacklist policy and caps the resulting fee at BPS. */
export function calculateFeeBpsForRouter(whitelisted: boolean, blacklistFeeMultiplier: bigint, feeBps: bigint): bigint {
  const fee = feeBps > BPS ? BPS : feeBps;
  if (whitelisted) return fee;
  if (blacklistFeeMultiplier !== 0n && fee > BPS / blacklistFeeMultiplier) return BPS;
  const adjusted = fee * blacklistFeeMultiplier;
  return adjusted > BPS ? BPS : adjusted;
}
/** Converts cash slippage into bounded basis points using upward rounding. */
export function quoteLaneSlippageBps(swapCashValue: bigint, principalCashValue: bigint, slippageKBps: bigint): bigint {
  if (swapCashValue === 0n || principalCashValue === 0n || slippageKBps === 0n) return 0n;
  const raw = fullMulDivUp(swapCashValue, slippageKBps, principalCashValue);
  const rounded = (raw + SLIPPAGE_SCALE - 1n) / SLIPPAGE_SCALE;
  return rounded > MAX_SLIPPAGE_BPS ? MAX_SLIPPAGE_BPS : rounded;
}
/** Calculates principal-weighted route slippage and caps it at BPS. */
export function quoteLaneWeightedSlippageKBps(
  firstPrincipal: bigint,
  firstK: bigint,
  secondPrincipal: bigint,
  secondK: bigint,
): bigint {
  const total = checkedAdd(firstPrincipal, secondPrincipal);
  if (total === 0n) return 0n;
  const weighted = checkedAdd(
    fullMulDivUp(firstPrincipal, firstK, total),
    fullMulDivUp(secondPrincipal, secondK, total),
  );
  return weighted > BPS ? BPS : weighted;
}
/** Splits a fee into partner and treasury portions with downward partner rounding. */
export function splitFee(feeAmount: bigint, partnerFeeBps: bigint): readonly [bigint, bigint] {
  if (feeAmount === 0n) return [0n, 0n];
  const partner = partnerFeeBps === 0n ? 0n : mulDivDown256(feeAmount, partnerFeeBps, BPS);
  return [partner, checkedSub(feeAmount, partner)];
}
/** Converts an exact-input amount through a lane price with floor rounding. */
export function quoteLaneExactIn(price: bigint, amountIn: bigint, cashToAsset: boolean): bigint {
  if (price === 0n) return 0n;
  return cashToAsset ? fullMulDivDown(amountIn, WAD, price) : fullMulDivDown(amountIn, price, WAD);
}
/** Converts an exact-output amount through a lane price with ceiling rounding. */
export function quoteLaneExactOut(price: bigint, amountOut: bigint, cashToAsset: boolean): bigint {
  if (price === 0n) return 0n;
  return cashToAsset ? fullMulDivUp(amountOut, price, WAD) : fullMulDivUp(amountOut, WAD, price);
}
