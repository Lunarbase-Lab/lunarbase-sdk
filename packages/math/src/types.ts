import type { Address, Word } from "./constants.js";

export interface LaneSlot0 { price: bigint; askFeeBps: bigint; bidFeeBps: bigint; pricePushThreshold: bigint; thresholdEnabled: boolean; latestUpdateBlock: bigint; reservedHighBits: bigint; }
export const EMPTY_SLOT0: LaneSlot0 = Object.freeze({ price: 0n, askFeeBps: 0n, bidFeeBps: 0n, pricePushThreshold: 0n, thresholdEnabled: false, latestUpdateBlock: 0n, reservedHighBits: 0n });

export interface LaneState { slot0: Word; exists: boolean; paused: boolean; blockDelay: bigint; slippageKBps: bigint; }
export interface QuoteState { cash: Address; lanes: ReadonlyMap<Address, LaneState>; totalPrincipalAmount: ReadonlyMap<Address, bigint>; whitelist: ReadonlyMap<Address, boolean>; blacklistFeeMultiplier: bigint; partnerFeeBps: ReadonlyMap<string, bigint>; stateVersion: bigint; }
export interface QuoteStateView { lane(asset: Address): LaneState | undefined; totalPrincipalAmount(asset: Address): bigint; isWhitelisted(router: Address): boolean; blacklistFeeMultiplier(): bigint; partnerFeeBps(router: Address, asset: Address): bigint; }
export function quoteStateView(state: QuoteState): QuoteStateView { return { lane: (asset) => state.lanes.get(asset), totalPrincipalAmount: (asset) => state.totalPrincipalAmount.get(asset) ?? 0n, isWhitelisted: (router) => state.whitelist.get(router) ?? false, blacklistFeeMultiplier: () => state.blacklistFeeMultiplier, partnerFeeBps: (router, asset) => state.partnerFeeBps.get(`${router.toLowerCase()}:${asset.toLowerCase()}`) ?? 0n }; }

export interface QuoteContext { cash: Address; executionBlockNumber: bigint; stateVersion: bigint; }
export type QuoteMode = "ExactIn" | "ExactOut";
export interface QuoteRequest { router: Address; assetIn: Address; assetOut: Address; amount: bigint; mode: QuoteMode; }
export interface QuoteResult { amountIn: bigint; amountOut: bigint; feeAsset: Address; feeAmount: bigint; partnerFee: bigint; treasuryFee: bigint; }
export type UnavailableReason = { kind: "ZeroAmount" } | { kind: "EqualAssets" } | { kind: "MissingLane"; asset: Address } | { kind: "PausedLane"; asset: Address } | { kind: "DelayedLane"; asset: Address } | { kind: "ZeroPrice"; asset: Address } | { kind: "ZeroPrincipal"; asset: Address } | { kind: "ZeroAnchor" } | { kind: "SpreadConsumesAnchor" };
export type QuoteOutcome = { kind: "Available"; result: QuoteResult } | { kind: "Unavailable"; reason: UnavailableReason };

export type { Address, Word };
