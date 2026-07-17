import type {
  BackfillRequest,
  ChainCursor,
  ChainEventSource,
  ChainUpdate,
  ContractFilter,
  ContractLog,
} from "../model.js";
import { Network } from "../model.js";
import type { NormalizedBackend } from "../source.js";
import type { ExecutionEvent, ExecutionEventReader, ExecutionHead, ExecutionLog } from "./reader.js";

/** Tracks contiguous or sparse Monad execution-event sequence positions. */
export class MonadRingTracker {
  private lastSequence?: bigint;
  private lastSubIndex = -1n;

  /** Accepts a complete contiguous raw-ring sequence. */
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

  /** Accepts sparse parser subscriptions that report gaps explicitly. */
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

  /** Rewinds sequence tracking after an explicit parser or ring gap. */
  rewind(): void {
    this.lastSequence = undefined;
    this.lastSubIndex = -1n;
  }
}

/** Converts Monad execution events into normalized runtime updates. */
export class MonadExecutionNormalizer {
  private readonly tracker = new MonadRingTracker();

  /** Creates a normalizer for one Monad chain id. */
  constructor(readonly chainId: bigint) {}

  /** Converts a raw execution event, suppressing duplicate log positions. */
  normalize(event: ExecutionEvent): ChainUpdate | undefined {
    if (event.kind === "Head") return this.normalizeHead(event.head);
    if (event.kind === "Log") return this.normalizeTxnLog(event.log);
    this.tracker.rewind();
    return { kind: "Gap", cursor: event.cursor, reason: event.reason };
  }

  /** Converts one execution log into a normalized contract log. */
  normalizeTxnLog(log: ExecutionLog): ChainUpdate | undefined {
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

  /** Converts a Monad block lifecycle event into a normalized head. */
  normalizeHead(head: ExecutionHead): ChainUpdate {
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

  /** Emits a fail-closed gap and rewinds sequence tracking. */
  normalizeGap(reason: string): ChainUpdate {
    this.tracker.rewind();
    return { kind: "Gap", reason };
  }
}

/** Universal Monad runtime parameterized by execution reader and recovery backend. */
export class MonadExecutionEngine implements ChainEventSource {
  readonly network = Network.Monad;

  constructor(
    readonly reader: ExecutionEventReader,
    readonly canonical: NormalizedBackend,
    readonly chainId: bigint,
  ) {}

  /** Returns the canonical cursor used by snapshot handoff and recovery. */
  snapshotCursor(): Promise<ChainCursor> {
    return this.canonical.snapshotCursor(Network.Monad);
  }

  /** Reads canonical Monad logs for an inclusive range. */
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]> {
    return this.canonical.backfill(request);
  }

  /** Opens execution events and converts them into common runtime updates. */
  subscribe(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    return this.normalize(this.reader.subscribeExecution(filter, signal));
  }

  private async *normalize(events: AsyncIterable<ExecutionEvent>): AsyncIterable<ChainUpdate> {
    const normalizer = new MonadExecutionNormalizer(this.chainId);
    try {
      for await (const event of events) {
        const update = normalizer.normalize(event);
        if (update) yield update;
        if (update?.kind === "Gap") return;
      }
    } catch (error) {
      yield normalizer.normalizeGap(
        `Monad execution reader failed; canonical recovery required: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
}
