/** Provider-independent model shared by every TypeScript network client. */
import type {
  Address,
  FeeClass,
  LaneState,
  QuoteOutcome,
  QuoteRequest,
  QuoteState,
  Word,
} from "@lunarbase-lab/pmm-v2-math";
import type { Hex } from "ox/Hex";

const TEXT_ENCODER = new TextEncoder();

/** Current checkpoint schema. */
export const SCHEMA_VERSION = 6;
/** Quote-math compatibility profile implemented by this SDK. */
export const MATH_COMPATIBILITY_VERSION = "lunarbase-pmm-v2";

/** Supported source families. */
export enum Network {
  Evm = "Evm",
  Base = "Base",
  Monad = "Monad",
  Arbitrum = "Arbitrum",
}

/** Returns the default mainnet chain id for a chain-specific source family. */
export function defaultChainId(network: Network): bigint | undefined {
  if (network === Network.Evm) return undefined;
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
  /** EIP-155 chain identifier used to reject cross-network updates. */
  chainId: bigint;
  /** Monotonic provider block position used for ordering and recovery. */
  blockNumber: bigint;
  /** EVM-visible block number supplied to block-dependent quote math. */
  executionBlockNumber: bigint;
  /** Canonical or provisional hash for `blockNumber`, when supplied. */
  blockHash?: Hex;
  /** Transaction position for a log-level cursor. */
  transactionIndex?: bigint;
  /** Log position within the block for deterministic event ordering. */
  logIndex?: bigint;
  /** Transport-local order used only when transaction coordinates are absent. */
  sourceSequence?: bigint;
  /** Position of an update within one transport sequence. */
  sourceSubIndex?: bigint;
  /** Confidence level attached to this observed chain position. */
  commitment: Commitment;
}

/** Block identity and parent linkage carried by head-level updates. */
export interface BlockRef {
  /** Ordered source position and execution height for this block. */
  readonly cursor: ChainCursor;
  /** Hash of the parent block or proposal, when supplied by the source. */
  readonly parentHash?: Hex;
}

/** Provider-neutral Core log. */
export interface ContractLog {
  /** Contract that emitted the log. */
  address: Address;
  /** Indexed ABI topics, including the event signature at index zero. */
  topics: readonly Hex[];
  /** Unindexed ABI-encoded event payload. */
  data: Hex;
  /** Whether the provider retracted the log during a reorganization. */
  removed: boolean;
  /** Fully normalized chain and event position. */
  cursor: ChainCursor;
}

/** Core address and accepted event signatures. */
export interface ContractFilter {
  /** Contract address accepted by the source. */
  readonly address: Address;
  /** Allowed event signatures; an empty list accepts every topic zero. */
  topics: readonly Hex[];
}

/** Inclusive canonical recovery range. */
export interface BackfillRequest {
  /** First canonical block included in recovery. */
  readonly fromBlock: bigint;
  /** Last canonical block included in recovery. */
  readonly toBlock: bigint;
  /** Contract and topic filter applied by the source. */
  readonly filter: ContractFilter;
}

/** Complete update vocabulary accepted by the ordered reducer. */
export type ChainUpdate =
  | { kind: "Head"; head: BlockRef }
  | { kind: "Log"; log: ContractLog }
  | { kind: "Reorg"; oldHead: BlockRef; newHead: BlockRef }
  | { kind: "Gap"; cursor?: ChainCursor; reason: string };

/** Conservatively charges one normalized log to bounded runtime queues. */
export function contractLogRetainedBytes(log: ContractLog): number {
  const topicBytes = log.topics.reduce((total, topic) => total + topic.length, 0);
  return 256 + topicBytes + log.data.length;
}

/** Conservatively charges one normalized update to bounded runtime queues. */
export function chainUpdateRetainedBytes(update: ChainUpdate): number {
  switch (update.kind) {
    case "Log":
      return 64 + contractLogRetainedBytes(update.log);
    case "Gap":
      return 192 + TEXT_ENCODER.encode(update.reason).byteLength;
    case "Head":
      return 384;
    case "Reorg":
      return 704;
  }
}

/** Identity and runtime policy for one Core deployment. */
export interface DeploymentConfig {
  /** Network source family required by this deployment. */
  readonly network: Network;
  /** EIP-155 chain identifier expected from every source cursor. */
  readonly chainId: bigint;
  /** LunarBase Core contract whose quote-critical state is indexed. */
  readonly core: Address;
  /** Mandatory economic fee class used by every quote. */
  readonly feeClass: FeeClass;
  /** Optional execution caller whose fee allocation is chain-verified. */
  readonly verifiedRouter: Address | undefined;
  /** First block that can contain deployment lane events. */
  readonly deploymentBlock: bigint;
  /** Pinned ERC-1967 implementation behind the Core proxy. */
  readonly expectedImplementation: Address;
  /** Pinned runtime bytecode hash of `expectedImplementation`. */
  readonly expectedImplementationCodeHash: Hex;
  /** Quote-math compatibility profile expected by the client. */
  readonly contractCompatibilityVersion: string;
  /** Optional fixed lane assets that avoid a discovery replay. */
  readonly explicitLaneAssets: readonly Address[];
}

/** Complete block-tagged state returned by a data source. */
export interface BootstrapSnapshot {
  /** Fully materialized quote state read at one coherent block tag. */
  state: QuoteState;
  /** Canonical cursor identifying the block used by every state read. */
  cursor: ChainCursor;
  /** ERC-1967 implementation active at the snapshot block. */
  implementation: Address;
  /** Keccak-256 runtime bytecode hash of `implementation`. */
  implementationCodeHash: Hex;
  /** Optional router-specific accounting state verified at the same block. */
  verifiedRouter?: VerifiedRouterSnapshot;
}

/** Router-specific accounting state kept outside quote-critical chain state. */
export interface VerifiedRouterSnapshot {
  /** Execution caller whose whitelist class and partner shares were verified. */
  router: Address;
  /** Partner share keyed by fee asset. */
  partnerFeeBps: ReadonlyMap<Address, number>;
}

/** Versioned restart state bound only to chain-wide deployment state. */
export interface Checkpoint {
  /** Persistence schema version; incompatible versions are discarded. */
  schemaVersion: number;
  /** Exact pure-math compatibility identifier used to create the state. */
  mathCompatibilityVersion: string;
  /** ERC-1967 implementation verified for the serialized state. */
  expectedImplementation: Address;
  /** Runtime bytecode hash of the verified implementation. */
  expectedImplementationCodeHash: Hex;
  /** EIP-155 chain identifier that owns the checkpoint. */
  chainId: bigint;
  /** Network source family used to create the checkpoint. */
  network: Network;
  /** Core contract whose state is serialized. */
  core: Address;
  /** First deployment block used for lane discovery and recovery. */
  deploymentBlock: bigint;
  /** Fixed lane policy used when the state was bootstrapped. */
  explicitLaneAssets: readonly Address[];
  /** Last fully applied and verified source position. */
  cursor: ChainCursor;
  /** Complete quote-critical state at `cursor`. */
  state: QuoteState;
}

/** One source owns bootstrap, recovery, realtime, and checkpoint validation. */
export interface ChainDataSource {
  /** Network family implemented by the source. */
  readonly network: Network;
  /** Reconstructs coherent quote state from a canonical block tag. */
  snapshot(deployment: DeploymentConfig): Promise<BootstrapSnapshot>;
  /** Reads canonical logs over an inclusive recovery range. */
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]>;
  /**
   * Opens and acknowledges a realtime normalized update stream.
   *
   * The promise resolves only after the transport has completed its protocol
   * handshake, so a runtime cannot report readiness for a merely connecting
   * socket.
   */
  subscribe(filter: ContractFilter, signal?: AbortSignal): Promise<AsyncIterable<ChainUpdate>>;
  /** Returns the latest canonical chain position known to the source. */
  canonicalHead(): Promise<ChainCursor>;
  /** Confirms that a checkpoint cursor still belongs to the canonical chain. */
  validateCheckpoint(checkpoint: Checkpoint): Promise<boolean>;
}

/** Decoded quote-critical Core event. */
export type QuoteEvent =
  | { kind: "LaneAdded"; asset: Address }
  | { kind: "LaneRemoved"; asset: Address }
  | { kind: "LaneUpdated"; asset: Address; slot0: Word }
  | { kind: "SlippageKSet"; asset: Address; newK: number }
  | { kind: "LanePausedSet"; asset: Address; paused: boolean }
  | { kind: "PricePushThresholdSet"; asset: Address; pricePushThreshold: number; enabled: boolean }
  | { kind: "BlockDelaySet"; asset: Address; blockDelay: number }
  | { kind: "PartnerInfoSet"; router: Address; asset: Address; fee: number }
  | { kind: "PartnerFeeSet"; router: Address; asset: Address; fee: number }
  | { kind: "WhitelistSet"; router: Address; whitelisted: boolean }
  | { kind: "BlacklistFeeMultiplierSet"; multiplier: bigint }
  | { kind: "DepositExecuted"; asset: Address; principal: bigint }
  | { kind: "WithdrawalExecuted"; asset: Address; principal: bigint }
  | { kind: "Sync"; asset: Address; assetReserve: bigint; cashReserve: bigint }
  | { kind: "ImplementationUpgraded"; implementation: Address };

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
      | "UNKNOWN_LANE"
      | "FEE_CLASS_MISMATCH"
      | "VERIFIED_ROUTER_REFRESH_REQUIRED"
      | "ARITHMETIC"
      | "IMPLEMENTATION_UPGRADED",
    message: string,
  ) {
    super(message);
    this.name = "ReducerError";
  }
}

/** Runtime lifecycle or source-continuity error. */
export class IndexerError extends Error {
  constructor(
    readonly code: "NOT_READY" | "GAP" | "CODE_HASH_MISMATCH" | "NO_CURSOR" | "INVALID_REQUEST" | "REDUCER" | "SOURCE",
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
  /** Bit-exact quote result or deterministic unavailability reason. */
  outcome: QuoteOutcome;
  /** Exact normalized state position used for evaluation. */
  cursor: ChainCursor;
  /** EVM-visible block supplied to time-dependent quote math. */
  executionBlockNumber: bigint;
  /** Core implementation bytecode hash associated with the state snapshot. */
  implementationCodeHash: Hex;
  /** Quote-math compatibility profile used by this result. */
  mathCompatibilityVersion: string;
  /** Economic fee class used for this quote. */
  feeClass: FeeClass;
  /** Optional router whose accounting split was verified. */
  verifiedRouter: Address | undefined;
}

/** Batch results calculated synchronously from one state cursor. */
export interface ClientBatchQuote {
  /** Single normalized state position shared by every result. */
  cursor: ChainCursor;
  /** Single EVM-visible block shared by every result. */
  executionBlockNumber: bigint;
  /** Results evaluated synchronously without yielding between items. */
  results: readonly QuoteOutcome[];
  /** Core implementation bytecode hash associated with the shared snapshot. */
  implementationCodeHash: Hex;
  /** Quote-math compatibility profile used by this batch. */
  mathCompatibilityVersion: string;
  /** Economic fee class shared by every result. */
  feeClass: FeeClass;
  /** Optional router whose accounting splits were verified. */
  verifiedRouter: Address | undefined;
}

/** Observable runtime state. */
export interface IndexerHealth {
  /** Whether the runtime currently permits quotes. */
  ready: boolean;
  /** Latest accepted normalized position, absent before bootstrap. */
  cursor?: ChainCursor;
  /** Confidence level of the latest accepted state. */
  commitment: Commitment;
  /** Expected Core implementation bytecode hash for this deployment. */
  implementationCodeHash: string;
  /** Quote-math compatibility profile used by this runtime. */
  mathCompatibilityVersion: string;
  /** Economic fee class selected for every quote. */
  feeClass: FeeClass;
  /** Optional router whose accounting allocation is verified. */
  verifiedRouter: Address | undefined;
}

export type { Address, FeeClass, LaneState, QuoteOutcome, QuoteRequest, QuoteState, Word };
