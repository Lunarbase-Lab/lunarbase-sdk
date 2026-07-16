import type {
  Address,
  ChainCursor,
  ChainEventSource,
  ChainUpdate,
  Commitment,
  ContractFilter,
  ContractLog,
  Network,
} from "../model.js";
import { Commitment as CommitmentValue, Network as NetworkValue } from "../model.js";
import type { Word } from "@lunarbase/math";

/** Provider adapter that exposes snapshots, canonical backfill, and normalized realtime updates. */
export interface NormalizedBackend {
  snapshotCursor(network: Network): Promise<ChainCursor>;
  backfill(request: import("../model.js").BackfillRequest): Promise<readonly ContractLog[]>;
  subscribe(network: Network, filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate>;
}
export class NetworkSource implements ChainEventSource {
  /** Creates a source facade with explicit network routing. */
  constructor(
    readonly network: Network,
    private readonly backend: NormalizedBackend,
  ) {}
  /** Returns the backend's authoritative snapshot cursor. */
  snapshotCursor(): Promise<ChainCursor> {
    return this.backend.snapshotCursor(this.network);
  }
  /** Reads canonical logs through the backend. */
  backfill(request: import("../model.js").BackfillRequest): Promise<readonly ContractLog[]> {
    return this.backend.backfill(request);
  }
  /** Opens normalized realtime updates for this network. */
  subscribe(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    return this.backend.subscribe(this.network, filter, signal);
  }
}

/** Tracks monotonic Base Flashblocks payload indices and block context. */
export class BaseFlashblocksTracker {
  private payloadId?: string;
  private blockNumber?: bigint;
  private latestIndex?: bigint;
  /** Accepts a new payload index and reports whether it is novel. */
  observe(payloadId: string, blockNumber: bigint, index: bigint): boolean {
    if (this.payloadId === payloadId) {
      if (this.blockNumber !== blockNumber) throw new Error("Flashblocks payload changed block context");
      if (this.latestIndex !== undefined && index < this.latestIndex) throw new Error("Flashblocks index regression");
      if (this.latestIndex === index) return false;
      this.latestIndex = index;
      return true;
    }
    this.payloadId = payloadId;
    this.blockNumber = blockNumber;
    this.latestIndex = index;
    return true;
  }
  /** Clears provisional payload tracking after canonical recovery. */
  reset(): void {
    this.payloadId = undefined;
    this.blockNumber = undefined;
    this.latestIndex = undefined;
  }
}
export interface FlashblockHeader {
  payloadId: string;
  blockNumber: bigint;
  blockHash?: string;
  index: bigint;
}
export interface FlashblockLog {
  header: FlashblockHeader;
  transactionIndex: bigint;
  logIndex: bigint;
  address: Address;
  topics: readonly Word[];
  data: string;
  removed: boolean;
}
export class BaseFlashblocksNormalizer {
  private readonly tracker = new BaseFlashblocksTracker();
  /** Creates a normalizer for one chain id. */
  constructor(readonly chainId: bigint) {}
  /** Converts a Flashblock header into a realtime head, deduplicating indices. */
  normalizeHeader(header: FlashblockHeader): ChainUpdate | undefined {
    if (!this.tracker.observe(header.payloadId, header.blockNumber, header.index)) return undefined;
    return {
      kind: "Head",
      cursor: {
        chainId: this.chainId,
        blockNumber: header.blockNumber,
        blockHash: header.blockHash,
        sourceSequence: header.index,
        commitment: CommitmentValue.Realtime,
      },
    };
  }
  /** Emits the head-before-log sequence expected by the reducer. */
  normalizeLog(log: FlashblockLog): ChainUpdate[] {
    const head = this.normalizeHeader(log.header);
    const updates: ChainUpdate[] = head ? [head] : [];
    updates.push({
      kind: "Log",
      log: {
        address: log.address,
        topics: log.topics,
        data: log.data,
        removed: log.removed,
        cursor: {
          chainId: this.chainId,
          blockNumber: log.header.blockNumber,
          blockHash: log.header.blockHash,
          transactionIndex: log.transactionIndex,
          logIndex: log.logIndex,
          sourceSequence: log.header.index,
          sourceSubIndex: log.logIndex,
          commitment: CommitmentValue.Realtime,
        },
      },
    });
    return updates;
  }
  /** Resets provisional Flashblocks state. */
  reset(): void {
    this.tracker.reset();
  }
}

/** Tracks contiguous Monad execution-event sequence/sub-index pairs. */
export class MonadRingTracker {
  private lastSequence?: bigint;
  private lastSubIndex = -1n;
  /** Accepts contiguous sequence values and rejects gaps/regressions. */
  observe(sequence: bigint, subIndex = 0n): boolean {
    if (this.lastSequence === undefined) {
      this.lastSequence = sequence;
      this.lastSubIndex = subIndex;
      return true;
    }
    if (sequence === this.lastSequence) {
      if (subIndex <= this.lastSubIndex) return false;
      this.lastSubIndex = subIndex;
      return true;
    }
    if (sequence === this.lastSequence + 1n) {
      this.lastSequence = sequence;
      this.lastSubIndex = subIndex;
      return true;
    }
    throw new Error("Monad execution-event sequence gap");
  }
  /** Accepts sparse sequence values for transports that report gaps separately. */
  observeSparse(sequence: bigint, subIndex = 0n): boolean {
    if (this.lastSequence === undefined) {
      this.lastSequence = sequence;
      this.lastSubIndex = subIndex;
      return true;
    }
    if (sequence < this.lastSequence) throw new Error("Monad execution-event sequence regression");
    if (sequence === this.lastSequence && subIndex <= this.lastSubIndex) return false;
    this.lastSequence = sequence;
    this.lastSubIndex = subIndex;
    return true;
  }
  /** Rewinds sequence tracking after a parser gap. */
  rewind(): void {
    this.lastSequence = undefined;
    this.lastSubIndex = -1n;
  }
}
export interface MonadExecutionLog {
  sequence: bigint;
  sourceSubIndex: bigint;
  blockNumber: bigint;
  blockHash?: string;
  transactionIndex: bigint;
  logIndex: bigint;
  address: Address;
  topics: readonly Word[];
  data: string;
  commitment: Commitment;
}
export class MonadExecutionNormalizer {
  private readonly tracker = new MonadRingTracker();
  /** Creates a normalizer for one Monad chain id. */
  constructor(readonly chainId: bigint) {}
  /** Converts one parser execution log into a normalized contract log. */
  normalizeTxnLog(log: MonadExecutionLog): ChainUpdate | undefined {
    if (!this.tracker.observeSparse(log.sequence, log.sourceSubIndex)) return undefined;
    return {
      kind: "Log",
      log: {
        address: log.address,
        topics: log.topics,
        data: log.data,
        removed: false,
        cursor: {
          chainId: this.chainId,
          blockNumber: log.blockNumber,
          blockHash: log.blockHash,
          transactionIndex: log.transactionIndex,
          logIndex: log.logIndex,
          sourceSequence: log.sequence,
          sourceSubIndex: log.sourceSubIndex,
          commitment: log.commitment,
        },
      },
    };
  }
  /** Converts a parser head into a normalized source head. */
  normalizeHead(head: MonadHead): ChainUpdate {
    return {
      kind: "Head",
      cursor: {
        chainId: this.chainId,
        blockNumber: head.blockNumber,
        blockHash: head.blockHash,
        sourceSequence: head.sequence,
        commitment: head.commitment,
      },
    };
  }
  /** Emits a gap and rewinds sequence tracking for canonical recovery. */
  normalizeGap(reason: string): ChainUpdate {
    this.tracker.rewind();
    return { kind: "Gap", reason };
  }
}
export interface MonadHead {
  sequence: bigint;
  blockNumber: bigint;
  blockHash?: string;
  commitment: Commitment;
}

export interface ArbitrumExecutionContext {
  l2BlockNumber: bigint;
  evmParentBlockNumber: bigint;
}
export interface ArbitrumHead {
  context: ArbitrumExecutionContext;
  blockHash?: string;
  commitment: Commitment;
}
export class ArbitrumNitroNormalizer {
  /** Creates a normalizer for one Arbitrum chain id. */
  constructor(readonly chainId: bigint) {}
  /** Maps Nitro's EVM-visible parent context into the cursor sequence. */
  normalizeHead(head: ArbitrumHead): ChainUpdate {
    return {
      kind: "Head",
      cursor: {
        chainId: this.chainId,
        blockNumber: head.context.l2BlockNumber,
        blockHash: head.blockHash,
        sourceSequence: head.context.evmParentBlockNumber,
        commitment: head.commitment,
      },
    };
  }
  /** Validates and passes through an executed Nitro log. */
  normalizeLog(log: ContractLog): ChainUpdate {
    if (log.cursor.chainId !== this.chainId) throw new Error("Arbitrum chain id mismatch");
    return { kind: "Log", log };
  }
}

/** Holds provisional decoded events until canonical logs can be compared. */
export class ProvisionalOverlay {
  private baseCursor?: ChainCursor;
  private values: Array<readonly [ChainCursor, import("../model.js").QuoteEvent]> = [];
  /** Starts an overlay at a canonical base cursor. */
  begin(baseCursor: ChainCursor): void {
    this.baseCursor = baseCursor;
    this.values = [];
  }
  /** Appends one provisional decoded event. */
  push(cursor: ChainCursor, event: import("../model.js").QuoteEvent): void {
    this.values.push([cursor, event]);
  }
  /** Returns provisional events in application order. */
  updates(): readonly (readonly [ChainCursor, import("../model.js").QuoteEvent])[] {
    return this.values;
  }
  /** Fails if canonical replay differs from the provisional overlay. */
  verifyCanonical(canonical: readonly (readonly [ChainCursor, import("../model.js").QuoteEvent])[]): void {
    const stable = (value: unknown) =>
      JSON.stringify(value, (_key, item) => (typeof item === "bigint" ? `${item}n` : item));
    if (stable(this.values) !== stable(canonical))
      throw new Error("Flashblocks provisional overlay diverged from canonical logs");
  }
  /** Verifies, clears, and returns the canonical cursor after commit. */
  commitCanonical(
    canonical: readonly (readonly [ChainCursor, import("../model.js").QuoteEvent])[],
  ): ChainCursor | undefined {
    this.verifyCanonical(canonical);
    const cursor = canonical.length > 0 ? canonical[canonical.length - 1]?.[0] : this.baseCursor;
    this.clear();
    return cursor;
  }
  /** Discards the provisional overlay. */
  discard(): void {
    this.clear();
  }
  /** Clears base cursor and provisional events. */
  clear(): void {
    this.baseCursor = undefined;
    this.values = [];
  }
}

export class BaseFlashblocksSource extends NetworkSource {
  constructor(backend: NormalizedBackend) {
    super(NetworkValue.Base, backend);
  }
}
export class MonadExecutionEventsSource extends NetworkSource {
  constructor(backend: NormalizedBackend) {
    super(NetworkValue.Monad, backend);
  }
}
export class ArbitrumNitroSource extends NetworkSource {
  constructor(backend: NormalizedBackend) {
    super(NetworkValue.Arbitrum, backend);
  }
}

/** Select the common source facade while preserving the specialized backend. */
export function makeNetworkSource(network: Network, backend: NormalizedBackend): NetworkSource {
  if (network === NetworkValue.Base) return new BaseFlashblocksSource(backend);
  if (network === NetworkValue.Monad) return new MonadExecutionEventsSource(backend);
  return new ArbitrumNitroSource(backend);
}
