import type { Address, Word } from "./constants.js";

/** Complete logical view of the packed lane slot. */
export interface LaneSlot0 {
  /** Fixed-point lane price stored in the low 128 bits of Solidity `slot0`. */
  price: bigint;
  /** Exact-input fee charged when buying the lane asset, in protocol BPS. */
  askFeeBps: bigint;
  /** Exact-input fee charged when selling the lane asset, in protocol BPS. */
  bidFeeBps: bigint;
  /** Minimum price movement required before a thresholded update is accepted. */
  pricePushThreshold: bigint;
  /** Whether Core enforces `pricePushThreshold` for this lane. */
  thresholdEnabled: boolean;
  /** EVM block number of the latest accepted price update. */
  latestUpdateBlock: bigint;
  /** Unassigned high bits preserved for bit-exact slot round trips. */
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
  /** Raw packed Solidity `Lane.slot0` word consumed by quote math. */
  slot0: Word;
  /** Active principal used as the denominator of lane slippage. */
  totalPrincipalAmount: bigint;
  /** Lane-specific slippage coefficient in protocol BPS. */
  slippageKBps: number;
  /** Required execution-block delay after the latest price update. */
  blockDelay: number;
  /** Compact `LaneFlags` bitset for existence and pause state. */
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
  /** Whether the configured router bypasses the global blacklist multiplier. */
  whitelisted: boolean;
  /** Global fee multiplier applied only to non-whitelisted routers. */
  blacklistFeeMultiplier: bigint;
  /** Partner fee for the configured router, keyed by fee asset. */
  partnerFeeBps: ReadonlyMap<Address, number>;
}

/** Immutable quote state snapshot shared by math and client layers. */
export interface QuoteState {
  /** Settlement asset used between two non-cash lanes. */
  cash: Address;
  /** Quote-critical lane state keyed by non-cash asset address. */
  lanes: ReadonlyMap<Address, LaneState>;
  /** Effective fees for the single router configured by this runtime. */
  feeProfile: FeeProfile;
}

/** Selects whether the caller fixes input or output amount. */
export type QuoteMode = "ExactIn" | "ExactOut";

/** Pure quote request; router and freshness policy belong to the runtime. */
export interface QuoteRequest {
  /** ERC-20 asset supplied by the swap caller. */
  assetIn: Address;
  /** ERC-20 asset requested by the swap caller. */
  assetOut: Address;
  /** Fixed input or output quantity selected by `mode`. */
  amount: bigint;
  /** Selects exact-input or exact-output quote evaluation. */
  mode: QuoteMode;
}

/** Successful quote amounts and fee attribution. */
export interface QuoteResult {
  /** Total input required by the quote, including input-side fees. */
  amountIn: bigint;
  /** Net output returned by the quote, after output-side fees. */
  amountOut: bigint;
  /** Asset in which `feeAmount` is denominated. */
  feeAsset: Address;
  /** Full protocol fee before partner/treasury attribution. */
  feeAmount: bigint;
  /** Portion of `feeAmount` assigned to the configured partner. */
  partnerFee: bigint;
  /** Portion of `feeAmount` assigned to the protocol treasury. */
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
