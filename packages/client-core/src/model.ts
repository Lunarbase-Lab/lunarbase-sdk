/** Provider-independent model shared by every TypeScript network client. */
import type { Address, LaneState, QuoteOutcome, QuoteRequest, QuoteState, Word } from "@lunarbase/math";
import type { Hex } from "ox/Hex";

/** Current JSON checkpoint schema. */
export const SCHEMA_VERSION = 3;
/** Solidity revision whose arithmetic behavior this SDK implements. */
export const MATH_COMPATIBILITY_VERSION = "lunarbase-contracts@24db47b866e8150a0d91cffd80efe49df85179b5:math-v1";

/** Supported source families. */
export enum Network {
  Base = "Base",
  Monad = "Monad",
  Arbitrum = "Arbitrum",
}

/** Returns the default mainnet chain id for one source family. */
export function defaultChainId(network: Network): bigint {
  return network === Network.Base ? 8453n : network === Network.Monad ? 143n : 42161n;
}

/** Confidence attached to a normalized source cursor. */
export enum Commitment {
  Realtime = "Realtime",
  Canonical = "Canonical",
  Finalized = "Finalized",
}

/** Returns the monotonic rank of a commitment. */
export function commitmentRank(value: Commitment): number {
  return value === Commitment.Realtime ? 0 : value === Commitment.Canonical ? 1 : 2;
}

/**
 * Provider ordering position and EVM-visible execution block.
 *
 * `sourceSequence` orders stream messages only. It never substitutes network
 * block semantics.
 */
export interface ChainCursor {
  chainId: bigint;
  blockNumber: bigint;
  executionBlockNumber: bigint;
  blockHash?: Hex;
  transactionIndex?: bigint;
  logIndex?: bigint;
  sourceSequence?: bigint;
  sourceSubIndex?: bigint;
  commitment: Commitment;
}

/** Provider-neutral Core log. */
export interface ContractLog {
  address: Address;
  topics: readonly Hex[];
  data: Hex;
  removed: boolean;
  cursor: ChainCursor;
}

/** Core address and accepted event signatures. */
export interface ContractFilter {
  address: Address;
  topics: readonly Hex[];
}

/** Inclusive canonical recovery range. */
export interface BackfillRequest {
  fromBlock: bigint;
  toBlock: bigint;
  filter: ContractFilter;
}

/** Complete update vocabulary accepted by the ordered reducer. */
export type ChainUpdate =
  | { kind: "Head"; cursor: ChainCursor }
  | { kind: "Log"; log: ContractLog }
  | { kind: "Reorg"; oldHead: ChainCursor; newHead: ChainCursor }
  | { kind: "Gap"; cursor?: ChainCursor; reason: string };

/** Identity and endpoints for one Core/router deployment. */
export interface DeploymentConfig {
  network: Network;
  chainId: bigint;
  core: Address;
  router: Address;
  expectWhitelisted: boolean;
  deploymentBlock: bigint;
  expectedRuntimeCodeHash: Hex;
  contractCompatibilityVersion: string;
  httpRpcUrl: string;
  realtimeSource: string;
  explicitLaneAssets: readonly Address[];
}

/** Complete block-tagged state returned by a data source. */
export interface BootstrapSnapshot {
  state: QuoteState;
  cursor: ChainCursor;
  runtimeCodeHash: Hex;
}

/** Versioned restart state bound to one deployment and configured router. */
export interface Checkpoint {
  schemaVersion: number;
  mathCompatibilityVersion: string;
  expectedRuntimeCodeHash: Hex;
  chainId: bigint;
  core: Address;
  router: Address;
  cursor: ChainCursor;
  state: QuoteState;
}

/** One source owns bootstrap, recovery, realtime, and checkpoint validation. */
export interface ChainDataSource {
  readonly network: Network;
  snapshot(deployment: DeploymentConfig): Promise<BootstrapSnapshot>;
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]>;
  subscribe(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate>;
  canonicalHead(): Promise<ChainCursor>;
  validateCheckpoint(checkpoint: Checkpoint): Promise<boolean>;
}

/** Decoded quote-critical Core event. `SwapExecuted` is intentionally absent. */
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
  | { kind: "WithdrawalExecuted"; asset: Address; principal: bigint };

/** Strict ABI validation error. */
export class LogDecodeError extends Error {
  constructor(
    readonly code:
      "MISSING_TOPIC0" | "INVALID_TOPIC_COUNT" | "INVALID_DATA_LENGTH" | "INVALID_ADDRESS" | "INVALID_BOOLEAN",
    message: string,
  ) {
    super(message);
    this.name = "LogDecodeError";
  }
}

/** Ordered state-transition error. */
export class ReducerError extends Error {
  constructor(
    readonly code:
      | "CHAIN_ID_MISMATCH"
      | "CURSOR_REGRESSION"
      | "BLOCK_HASH_MISMATCH"
      | "REMOVED_LOG"
      | "INVALID_SLIPPAGE_K"
      | "INVALID_WIDTH"
      | "ARITHMETIC",
    message: string,
  ) {
    super(message);
    this.name = "ReducerError";
  }
}

/** Runtime lifecycle or source-continuity error. */
export class IndexerError extends Error {
  constructor(
    readonly code: "NOT_READY" | "GAP" | "CODE_HASH_MISMATCH" | "NO_CURSOR" | "REDUCER" | "SOURCE",
    message: string,
  ) {
    super(message);
    this.name = "IndexerError";
  }
}

/** Log decoder injected at the protocol boundary. */
export type LogDecoder = (log: ContractLog) => QuoteEvent | undefined;

/** One quote bound to the exact cursor used for calculation. */
export interface ClientQuote {
  outcome: QuoteOutcome;
  cursor: ChainCursor;
  executionBlockNumber: bigint;
}

/** Batch results calculated synchronously from one state cursor. */
export interface ClientBatchQuote {
  cursor: ChainCursor;
  executionBlockNumber: bigint;
  results: readonly QuoteOutcome[];
}

/** Observable runtime state. */
export interface IndexerHealth {
  ready: boolean;
  cursor?: ChainCursor;
  commitment: Commitment;
  contractCodeHash: string;
  mathCompatibilityVersion: string;
}

export type { Address, LaneState, QuoteOutcome, QuoteRequest, QuoteState, Word };
