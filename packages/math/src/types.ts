import type { Address, Word } from "./constants.js";

/** Complete logical view of the packed lane slot. */
export interface LaneSlot0 {
  price: bigint;
  askFeeBps: bigint;
  bidFeeBps: bigint;
  pricePushThreshold: bigint;
  thresholdEnabled: boolean;
  latestUpdateBlock: bigint;
  reservedHighBits: bigint;
}

/** Zero-valued lane slot used when constructing an empty state. */
export const EMPTY_SLOT0: LaneSlot0 = Object.freeze({
  price: 0n,
  askFeeBps: 0n,
  bidFeeBps: 0n,
  pricePushThreshold: 0n,
  thresholdEnabled: false,
  latestUpdateBlock: 0n,
  reservedHighBits: 0n,
});

/** Bit values used by the compact lane lifecycle field. */
export const LaneFlags = Object.freeze({
  Exists: 1,
  Paused: 2,
});

/** Compact lane state consumed by the quote engine. */
export interface LaneState {
  slot0: Word;
  totalPrincipalAmount: bigint;
  slippageKBps: number;
  blockDelay: number;
  flags: number;
}

/** Returns whether a compact lane is active. */
export const laneExists = (lane: LaneState): boolean => (lane.flags & LaneFlags.Exists) !== 0;

/** Returns whether a compact lane is paused. */
export const lanePaused = (lane: LaneState): boolean => (lane.flags & LaneFlags.Paused) !== 0;

/** Builds a compact lane while validating native-number fields. */
export function createLaneState(
  slot0: Word,
  totalPrincipalAmount: bigint,
  slippageKBps: number,
  blockDelay: number,
  exists: boolean,
  paused: boolean,
): LaneState {
  if (!Number.isSafeInteger(slippageKBps) || slippageKBps < 0 || slippageKBps > 0xffff_ffff)
    throw new RangeError("slippageKBps does not fit uint32");
  if (!Number.isSafeInteger(blockDelay) || blockDelay < 0 || blockDelay > 0xff)
    throw new RangeError("blockDelay does not fit uint8");
  if (totalPrincipalAmount < 0n || totalPrincipalAmount >= 1n << 128n)
    throw new RangeError("totalPrincipalAmount does not fit uint128");
  return {
    slot0,
    totalPrincipalAmount,
    slippageKBps,
    blockDelay,
    flags: (exists ? LaneFlags.Exists : 0) | (paused ? LaneFlags.Paused : 0),
  };
}

/** Effective fees for the single router configured by this client instance. */
export interface FeeProfile {
  whitelisted: boolean;
  blacklistFeeMultiplier: bigint;
  partnerFeeBps: ReadonlyMap<Address, number>;
}

/** Immutable quote state snapshot shared by math and client layers. */
export interface QuoteState {
  cash: Address;
  lanes: ReadonlyMap<Address, LaneState>;
  feeProfile: FeeProfile;
}

/** Selects whether the caller fixes input or output amount. */
export type QuoteMode = "ExactIn" | "ExactOut";

/** Pure quote request; router and freshness policy belong to the runtime. */
export interface QuoteRequest {
  assetIn: Address;
  assetOut: Address;
  amount: bigint;
  mode: QuoteMode;
}

/** Successful quote amounts and fee attribution. */
export interface QuoteResult {
  amountIn: bigint;
  amountOut: bigint;
  feeAsset: Address;
  feeAmount: bigint;
  partnerFee: bigint;
  treasuryFee: bigint;
}

/** Structured reasons why a quote cannot be produced. */
export type UnavailableReason =
  | { kind: "ZeroAmount" }
  | { kind: "EqualAssets" }
  | { kind: "MissingLane"; asset: Address }
  | { kind: "PausedLane"; asset: Address }
  | { kind: "DelayedLane"; asset: Address }
  | { kind: "ZeroPrice"; asset: Address }
  | { kind: "ZeroPrincipal"; asset: Address }
  | { kind: "ZeroAnchor" }
  | { kind: "SpreadConsumesAnchor" };

/** Discriminated union returned by all quote entry points. */
export type QuoteOutcome =
  { kind: "Available"; result: QuoteResult } | { kind: "Unavailable"; reason: UnavailableReason };

export type { Address, Word };
