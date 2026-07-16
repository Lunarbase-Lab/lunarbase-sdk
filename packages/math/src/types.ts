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

/** Compact lane state consumed by the quote engine. */
export interface LaneState {
  slot0: Word;
  exists: boolean;
  paused: boolean;
  blockDelay: bigint;
  slippageKBps: bigint;
}
/** Immutable quote state snapshot shared by math and client layers. */
export interface QuoteState {
  cash: Address;
  lanes: ReadonlyMap<Address, LaneState>;
  totalPrincipalAmount: ReadonlyMap<Address, bigint>;
  whitelist: ReadonlyMap<Address, boolean>;
  blacklistFeeMultiplier: bigint;
  partnerFeeBps: ReadonlyMap<string, bigint>;
  stateVersion: bigint;
}
/** Read-only accessor surface for integrations that should not mutate maps. */
export interface QuoteStateView {
  lane(asset: Address): LaneState | undefined;
  totalPrincipalAmount(asset: Address): bigint;
  isWhitelisted(router: Address): boolean;
  blacklistFeeMultiplier(): bigint;
  partnerFeeBps(router: Address, asset: Address): bigint;
}
/** Creates a normalized accessor view over a quote state snapshot. */
export function quoteStateView(state: QuoteState): QuoteStateView {
  return {
    lane: (asset) => state.lanes.get(asset),
    totalPrincipalAmount: (asset) => state.totalPrincipalAmount.get(asset) ?? 0n,
    isWhitelisted: (router) => state.whitelist.get(router) ?? false,
    blacklistFeeMultiplier: () => state.blacklistFeeMultiplier,
    partnerFeeBps: (router, asset) => state.partnerFeeBps.get(`${router.toLowerCase()}:${asset.toLowerCase()}`) ?? 0n,
  };
}

/** Runtime context used to validate the state snapshot and block predicates. */
export interface QuoteContext {
  cash: Address;
  executionBlockNumber: bigint;
  stateVersion: bigint;
}
/** Selects whether the caller fixes input or output amount. */
export type QuoteMode = "ExactIn" | "ExactOut";
/** User quote request before route resolution and fee calculation. */
export interface QuoteRequest {
  router: Address;
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
