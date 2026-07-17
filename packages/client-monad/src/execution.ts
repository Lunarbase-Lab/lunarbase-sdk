/** Monad parser execution-event model and normalization. */
import type { Address, Word } from "@lunarbase/math";
import type { ChainCursor, ChainUpdate, Commitment, ContractFilter } from "@lunarbase/client-core";

/** Block lifecycle record emitted by the parser. */
export interface ExecutionHead {
  readonly sequence: bigint;
  readonly blockNumber: bigint;
  readonly blockHash?: string;
  readonly commitment: Commitment;
}

/** EVM log emitted before normalization into the common model. */
export interface ExecutionLog {
  readonly sequence: bigint;
  readonly sourceSubIndex: bigint;
  readonly blockNumber: bigint;
  readonly blockHash?: string;
  readonly transactionIndex: bigint;
  readonly logIndex: bigint;
  readonly address: Address;
  readonly topics: readonly Word[];
  readonly data: string;
  readonly commitment: Commitment;
}

/** Raw parser lifecycle event. */
export type ExecutionEvent =
  | { readonly kind: "Head"; readonly head: ExecutionHead }
  | { readonly kind: "Log"; readonly log: ExecutionLog }
  | { readonly kind: "Gap"; readonly cursor?: ChainCursor; readonly reason: string };

/** Suppresses duplicate sparse parser positions and rejects regression. */
export class MonadSequenceTracker {
  private lastSequence?: bigint;
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
  private readonly tracker = new MonadSequenceTracker();

  constructor(readonly chainId: bigint) {}

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
  subscribeExecution(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ExecutionEvent>;
}
