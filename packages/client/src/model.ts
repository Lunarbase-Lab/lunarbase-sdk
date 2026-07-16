import type { Address, LaneState, QuoteContext, QuoteOutcome, QuoteRequest, QuoteState, Word } from "@lunarbase/math";

export const SCHEMA_VERSION = 2n;
export const MATH_COMPATIBILITY_VERSION = "lunarbase-contracts@24db47b866e8150a0d91cffd80efe49df85179b5:math-v1";

export enum Network { Base = "Base", Monad = "Monad", Arbitrum = "Arbitrum" }
export function defaultChainId(network: Network): bigint { return network === Network.Base ? 8453n : network === Network.Monad ? 143n : 42161n; }
export enum Commitment { Realtime = "Realtime", Canonical = "Canonical", Finalized = "Finalized" }
export function commitmentRank(value: Commitment): bigint { return value === Commitment.Realtime ? 0n : value === Commitment.Canonical ? 1n : 2n; }

export interface ChainCursor { chainId: bigint; blockNumber: bigint; blockHash?: string; transactionIndex?: bigint; logIndex?: bigint; sourceSequence?: bigint; sourceSubIndex?: bigint; commitment: Commitment; }
export interface ContractLog { address: Address; topics: readonly Word[]; data: string; removed: boolean; cursor: ChainCursor; }
export interface ContractFilter { address: Address; topics: readonly Word[]; }
export interface BackfillRequest { fromBlock: bigint; toBlock: bigint; filter: ContractFilter; }
export type ChainUpdate = { kind: "Head"; cursor: ChainCursor } | { kind: "Log"; log: ContractLog } | { kind: "Reorg"; oldHead: ChainCursor; newHead: ChainCursor } | { kind: "Gap"; cursor?: ChainCursor; reason: string } | { kind: "SourceHealth"; healthy: boolean; detail: string };
export interface ChainEventSource { readonly network: Network; snapshotCursor(): Promise<ChainCursor>; backfill(request: BackfillRequest): Promise<readonly ContractLog[]>; subscribe(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate>; }

export interface RedisConfig { url: string; streamMaxLen: number; dedupTtlSeconds: bigint; checkpointIntervalUpdates: number; }
export const defaultRedisConfig = (): RedisConfig => ({ url: "redis://127.0.0.1/", streamMaxLen: 10_000, dedupTtlSeconds: 86_400n, checkpointIntervalUpdates: 100 });
export interface DeploymentConfig { network: Network; chainId: bigint; core: Address; deploymentBlock: bigint; expectedRuntimeCodeHash: string; contractCompatibilityVersion: string; httpRpcUrl: string; realtimeSource: string; redis: RedisConfig; explicitLaneAssets: readonly Address[]; eagerRouters: readonly Address[]; }
export interface BootstrapSnapshot { state: QuoteState; cursor: ChainCursor; runtimeCodeHash: string; }
export interface SnapshotProvider { snapshot(config: DeploymentConfig, laneAssets: readonly Address[], routers: readonly Address[]): Promise<BootstrapSnapshot>; }
export interface Checkpoint { schemaVersion: bigint; mathCompatibilityVersion: string; expectedRuntimeCodeHash: string; cursor: ChainCursor; state: QuoteState; }

export type QuoteEvent =
  | { kind: "LaneAdded"; asset: Address }
  | { kind: "LaneRemoved"; asset: Address }
  | { kind: "LaneUpdated"; asset: Address; slot0: Word }
  | { kind: "SlippageKSet"; asset: Address; newK: bigint }
  | { kind: "PartnerInfoSet"; router: Address; asset: Address; fee: bigint }
  | { kind: "PartnerFeeSet"; router: Address; asset: Address; fee: bigint }
  | { kind: "WhitelistSet"; router: Address; whitelisted: boolean }
  | { kind: "BlacklistFeeMultiplierSet"; multiplier: bigint }
  | { kind: "DepositExecuted"; asset: Address; principal: bigint }
  | { kind: "WithdrawalExecuted"; asset: Address; principal: bigint }
  | { kind: "SwapExecuted" };

export class LogDecodeError extends Error { constructor(readonly code: "MISSING_TOPIC0" | "INVALID_TOPIC_COUNT" | "INVALID_DATA_LENGTH" | "INVALID_ADDRESS" | "INVALID_BOOLEAN", message: string) { super(message); this.name = "LogDecodeError"; } }
export class ReducerError extends Error { constructor(readonly code: "CHAIN_ID_MISMATCH" | "CURSOR_REGRESSION" | "BLOCK_HASH_MISMATCH" | "REMOVED_LOG" | "INVALID_SLIPPAGE_K" | "INVALID_WIDTH" | "ARITHMETIC", message: string) { super(message); this.name = "ReducerError"; } }
export class IndexerError extends Error { constructor(readonly code: "NOT_READY" | "GAP" | "CODE_HASH_MISMATCH" | "FRESHNESS_UNAVAILABLE" | "NO_CURSOR" | "REDUCER" | "SOURCE", message: string) { super(message); this.name = "IndexerError"; } }
export type LogDecoder = (log: ContractLog) => QuoteEvent | undefined;
export interface ClientQuote { outcome: QuoteOutcome; cursor: ChainCursor; commitment: Commitment; observedAt: bigint; ageMilliseconds: bigint; stale: boolean; contractCodeHash: string; mathCompatibilityVersion: string; }
export interface FreshnessPolicy { minimumCommitment: Commitment; maxAgeBlocks?: bigint; }
export interface IndexerHealth { ready: boolean; commitment: Commitment; cursor?: ChainCursor; contractCodeHash: string; mathCompatibilityVersion: string; }
export interface CheckpointStore { load(): Checkpoint | undefined; commit(checkpoint: Checkpoint, updates: readonly ChainUpdate[]): void; updates(): readonly ChainUpdate[]; }
export type RedisAtomicCommand = { kind: "set"; key: string; value: Uint8Array } | { kind: "hset"; key: string; fields: Readonly<Record<string, string>> } | { kind: "xadd"; key: string; value: Uint8Array } | { kind: "xaddIfNew"; key: string; dedupKey: string; dedupTtlSeconds: bigint; value: Uint8Array } | { kind: "xtrim"; key: string; maxLen: number };

export type { Address, LaneState, QuoteContext, QuoteOutcome, QuoteRequest, QuoteState, Word };
