import { assertU256, U256_MAX, type Address } from "./constants.js";
import { checkedAdd, checkedSub } from "./arithmetic.js";
import {
  laneSlot0AskFeeBps,
  laneSlot0BidFeeBps,
  laneSlot0BlockDelay,
  laneSlot0LatestUpdateBlock,
  laneSlot0Price,
  laneSlot0SlippageKBps,
} from "./slot0.js";
import {
  calculateFeeBpsForRouter,
  quoteLaneExactIn,
  quoteLaneExactInFee,
  quoteLaneExactOut,
  quoteLaneExactOutFee,
  quoteLaneSlippageBps,
  quoteLaneWeightedSlippageKBps,
  splitFee,
} from "./fees.js";
import {
  laneExists,
  lanePaused,
  type FeeAllocation,
  type LaneState,
  type QuoteOutcome,
  type QuotePolicy,
  type QuoteRequest,
  type QuoteState,
  type UnavailableReason,
} from "./types.js";

function laneOrReason(state: QuoteState, asset: Address, executionBlockNumber: bigint): LaneState | UnavailableReason {
  const lane = state.lanes.get(asset.toLowerCase() as Address);
  if (!lane || !laneExists(lane)) return { kind: "MissingLane", asset };
  if (lanePaused(lane)) return { kind: "PausedLane", asset };
  const expiresAt = checkedAdd(laneSlot0LatestUpdateBlock(lane.slot0), BigInt(laneSlot0BlockDelay(lane.slot0)));
  if (executionBlockNumber > expiresAt) return { kind: "StaleLane", asset };
  return lane;
}

function principalCashValue(lane: LaneState): bigint {
  return lane.totalPrincipalAmount === 0n
    ? 0n
    : quoteLaneExactIn(laneSlot0Price(lane.slot0), lane.totalPrincipalAmount, false);
}

function laneSpread(anchor: bigint, feeBps: bigint, slippageBps: bigint, exactIn: boolean): readonly [bigint, bigint] {
  const fee = exactIn ? quoteLaneExactInFee(anchor, feeBps) : quoteLaneExactOutFee(anchor, feeBps);
  const totalBps = checkedAdd(feeBps, slippageBps);
  const total = exactIn ? quoteLaneExactInFee(anchor, totalBps) : quoteLaneExactOutFee(anchor, totalBps);
  return [fee, checkedSub(total, fee)];
}

function assembleQuote(
  state: QuoteState,
  policy: QuotePolicy,
  request: QuoteRequest,
  feeAsset: Address,
  anchor: bigint,
  fee: bigint,
  slippageAmount: bigint,
): QuoteOutcome {
  const totalSpread = checkedAdd(fee, slippageAmount);
  let amountIn: bigint;
  let amountOut: bigint;
  if (request.mode === "ExactIn") {
    if (totalSpread >= anchor) return { kind: "Unavailable", reason: { kind: "SpreadConsumesAnchor" } };
    amountIn = request.amount;
    amountOut = checkedSub(anchor, totalSpread);
  } else {
    amountIn = checkedAdd(anchor, totalSpread);
    amountOut = request.amount;
  }
  const outputReserve =
    request.assetOut.toLowerCase() === state.cash.toLowerCase()
      ? state.cashReserve
      : state.lanes.get(request.assetOut.toLowerCase() as Address)?.assetReserve;
  if (outputReserve === undefined)
    return { kind: "Unavailable", reason: { kind: "MissingLane", asset: request.assetOut } };
  const insufficient = amountOut > outputReserve || (request.mode === "ExactIn" && fee > outputReserve - amountOut);
  if (insufficient)
    return { kind: "Unavailable", reason: { kind: "InsufficientOutputReserve", asset: request.assetOut } };
  let feeAllocation: FeeAllocation | undefined;
  if (policy.verifiedPartnerFeeBps !== undefined) {
    const [partnerFee, treasuryFee] = splitFee(fee, BigInt(policy.verifiedPartnerFeeBps));
    feeAllocation = { partnerFee, treasuryFee };
  }
  return {
    kind: "Available",
    result: { amountIn, amountOut, feeAsset, feeAmount: fee, feeAllocation },
  };
}

function directQuote(
  state: QuoteState,
  policy: QuotePolicy,
  request: QuoteRequest,
  laneAsset: Address,
  lane: LaneState,
  cashToAsset: boolean,
  feeAsset: Address,
): QuoteOutcome {
  const anchor =
    request.mode === "ExactIn"
      ? quoteLaneExactIn(laneSlot0Price(lane.slot0), request.amount, cashToAsset)
      : quoteLaneExactOut(laneSlot0Price(lane.slot0), request.amount, cashToAsset);
  if (anchor === 0n) return { kind: "Unavailable", reason: { kind: "ZeroAnchor" } };
  const principal = principalCashValue(lane);
  if (principal === 0n) return { kind: "Unavailable", reason: { kind: "ZeroPrincipal", asset: laneAsset } };
  const rawFee = cashToAsset ? laneSlot0AskFeeBps(lane.slot0) : laneSlot0BidFeeBps(lane.slot0);
  const feeBps = calculateFeeBpsForRouter(policy.feeClass === "Whitelisted", state.blacklistFeeMultiplier, rawFee);
  const swapCash = cashToAsset
    ? request.mode === "ExactIn"
      ? request.amount
      : anchor
    : request.mode === "ExactIn"
      ? anchor
      : request.amount;
  const slippage = quoteLaneSlippageBps(swapCash, principal, BigInt(laneSlot0SlippageKBps(lane.slot0)));
  const [fee, slippageAmount] = laneSpread(anchor, feeBps, slippage, request.mode === "ExactIn");
  return assembleQuote(state, policy, request, feeAsset, anchor, fee, slippageAmount);
}

function routeQuote(
  state: QuoteState,
  policy: QuotePolicy,
  request: QuoteRequest,
  inputLane: LaneState,
  outputLane: LaneState,
): QuoteOutcome {
  let intermediateCash: bigint;
  let anchor: bigint;
  if (request.mode === "ExactIn") {
    intermediateCash = quoteLaneExactIn(laneSlot0Price(inputLane.slot0), request.amount, false);
    anchor = quoteLaneExactIn(laneSlot0Price(outputLane.slot0), intermediateCash, true);
  } else {
    intermediateCash = quoteLaneExactOut(laneSlot0Price(outputLane.slot0), request.amount, true);
    anchor = quoteLaneExactOut(laneSlot0Price(inputLane.slot0), intermediateCash, false);
  }
  if (anchor === 0n) return { kind: "Unavailable", reason: { kind: "ZeroAnchor" } };
  const firstPrincipal = principalCashValue(inputLane);
  if (firstPrincipal === 0n) return { kind: "Unavailable", reason: { kind: "ZeroPrincipal", asset: request.assetIn } };
  const secondPrincipal = principalCashValue(outputLane);
  if (secondPrincipal === 0n)
    return { kind: "Unavailable", reason: { kind: "ZeroPrincipal", asset: request.assetOut } };
  const weightedK = quoteLaneWeightedSlippageKBps(
    firstPrincipal,
    BigInt(laneSlot0SlippageKBps(inputLane.slot0)),
    secondPrincipal,
    BigInt(laneSlot0SlippageKBps(outputLane.slot0)),
  );
  const slippage = quoteLaneSlippageBps(intermediateCash, checkedAdd(firstPrincipal, secondPrincipal), weightedK);
  const whitelisted = policy.feeClass === "Whitelisted";
  const multiplier = state.blacklistFeeMultiplier;
  const bid = calculateFeeBpsForRouter(whitelisted, multiplier, laneSlot0BidFeeBps(inputLane.slot0));
  const ask = calculateFeeBpsForRouter(whitelisted, multiplier, laneSlot0AskFeeBps(outputLane.slot0));
  const feeBps = checkedAdd(bid, ask);
  const fee = request.mode === "ExactIn" ? quoteLaneExactInFee(anchor, feeBps) : quoteLaneExactOutFee(anchor, feeBps);
  const totalBps = checkedAdd(checkedAdd(bid, slippage), checkedAdd(ask, slippage));
  const total =
    request.mode === "ExactIn" ? quoteLaneExactInFee(anchor, totalBps) : quoteLaneExactOutFee(anchor, totalBps);
  const feeAsset = request.mode === "ExactIn" ? request.assetOut : request.assetIn;
  return assembleQuote(state, policy, request, feeAsset, anchor, fee, checkedSub(total, fee));
}

/**
 * Produces a bit-exact quote from one immutable in-memory state snapshot.
 *
 * `executionBlockNumber` is the EVM-visible block tracked by the runtime.
 * Pure math never reads RPC, persistence, wall-clock freshness, or a router.
 */
export function quote(
  request: QuoteRequest,
  executionBlockNumber: bigint,
  state: QuoteState,
  policy: QuotePolicy,
): QuoteOutcome {
  assertU256(request.amount, "amount");
  assertU256(executionBlockNumber, "executionBlockNumber");
  if (request.amount === 0n) return { kind: "Unavailable", reason: { kind: "ZeroAmount" } };
  if (request.assetIn.toLowerCase() === request.assetOut.toLowerCase())
    return { kind: "Unavailable", reason: { kind: "EqualAssets" } };
  const feeAsset = request.mode === "ExactIn" ? request.assetOut : request.assetIn;
  if (request.assetOut.toLowerCase() === state.cash.toLowerCase()) {
    const lane = laneOrReason(state, request.assetIn, executionBlockNumber);
    return "kind" in lane
      ? { kind: "Unavailable", reason: lane }
      : directQuote(state, policy, request, request.assetIn, lane, false, feeAsset);
  }
  if (request.assetIn.toLowerCase() === state.cash.toLowerCase()) {
    const lane = laneOrReason(state, request.assetOut, executionBlockNumber);
    return "kind" in lane
      ? { kind: "Unavailable", reason: lane }
      : directQuote(state, policy, request, request.assetOut, lane, true, feeAsset);
  }
  const input = laneOrReason(state, request.assetIn, executionBlockNumber);
  if ("kind" in input) return { kind: "Unavailable", reason: input };
  const output = laneOrReason(state, request.assetOut, executionBlockNumber);
  if ("kind" in output) return { kind: "Unavailable", reason: output };
  return routeQuote(state, policy, request, input, output);
}

/**
 * Converts a rich outcome to the scalar returned by the matching Solidity
 * quote function, including exact-out sentinel behavior.
 */
export function solidityQuoteAmount(request: QuoteRequest, outcome: QuoteOutcome): bigint {
  if (request.mode === "ExactIn") return outcome.kind === "Available" ? outcome.result.amountOut : 0n;
  if (request.amount === 0n) return 0n;
  return outcome.kind === "Available" ? outcome.result.amountIn : U256_MAX;
}
