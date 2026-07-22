/** Monad parser execution-event model and normalization. */
import type { Address } from "@lunarbase/math";
import type { ChainCursor, ChainUpdate, Commitment, ContractFilter } from "@lunarbase/client-core";
import type { Hex } from "ox/Hex";

/** Block lifecycle record emitted by the parser. */
export interface ExecutionHead {
  /** Monotonic parser or native execution-event sequence. */
  readonly sequence: bigint;
  /** EVM-visible Monad block height. */
  readonly blockNumber: bigint;
  /** Block identifier supplied by the execution source, when available. */
  readonly blockHash?: Hex;
  /** Lifecycle confidence represented by the notification. */
  readonly commitment: Commitment;
}

/** EVM log emitted before normalization into the common model. */
export interface ExecutionLog {
  /** Monotonic parser or native execution-event sequence. */
  readonly sequence: bigint;
  /** Deterministic position within one source sequence. */
  readonly sourceSubIndex: bigint;
  /** EVM-visible block that executed the log. */
  readonly blockNumber: bigint;
  /** Hash of the executing block, when supplied by the source. */
  readonly blockHash?: Hex;
  /** Transaction position within the executing block. */
  readonly transactionIndex: bigint;
  /** Log position within the executing block. */
  readonly logIndex: bigint;
  /** EVM contract that emitted the log. */
  readonly address: Address;
  /** Indexed event topics, including signature topic zero. */
  readonly topics: readonly Hex[];
  /** Unindexed ABI-encoded event payload. */
  readonly data: Hex;
  /** Lifecycle confidence inherited from the nearest block notification. */
  readonly commitment: Commitment;
}

/** Raw parser lifecycle event. */
export type ExecutionEvent =
  | { readonly kind: "Head"; readonly head: ExecutionHead }
  | { readonly kind: "Log"; readonly log: ExecutionLog }
  | { readonly kind: "Gap"; readonly cursor?: ChainCursor; readonly reason: string };

/** Suppresses duplicate sparse parser positions and rejects regression. */
export class MonadSequenceTracker {
  /** Latest source sequence accepted from the filtered parser stream. */
  private lastSequence?: bigint;
  /** Latest event position accepted within `lastSequence`. */
  private lastSubIndex = -1n;

  /** Records one parser sequence/sub-index pair. */
  observe(sequence: bigint, subIndex: bigint): boolean {
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

  /** Clears ordering after an explicit parser gap. */
  rewind(): void {
    this.lastSequence = undefined;
    this.lastSubIndex = -1n;
  }
}

/** Converts parser events to provider-neutral runtime updates. */
export class MonadExecutionNormalizer {
  /** Duplicate and regression guard for sparse parser messages. */
  private readonly tracker = new MonadSequenceTracker();

  constructor(
    /** EIP-155 chain identifier attached to every normalized cursor. */
    readonly chainId: bigint,
  ) {}

  /** Converts one event, returning `undefined` for an exact duplicate. */
  normalize(event: ExecutionEvent): ChainUpdate | undefined {
    if (event.kind === "Gap") {
      this.tracker.rewind();
      return { kind: "Gap", cursor: event.cursor, reason: event.reason };
    }
    if (event.kind === "Head")
      return {
        kind: "Head",
        cursor: {
          chainId: this.chainId,
          blockNumber: event.head.blockNumber,
          executionBlockNumber: event.head.blockNumber,
          blockHash: event.head.blockHash,
          sourceSequence: event.head.sequence,
          commitment: event.head.commitment,
        },
      };
    const log = event.log;
    if (!this.tracker.observe(log.sequence, log.sourceSubIndex)) return undefined;
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
          executionBlockNumber: log.blockNumber,
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
}

/** Parser transport boundary useful to alternative portable implementations. */
export interface ExecutionEventReader {
  /** Opens and acknowledges a raw stream filtered to one Core deployment. */
  subscribeExecution(filter: ContractFilter, signal?: AbortSignal): Promise<AsyncIterable<ExecutionEvent>>;
}
