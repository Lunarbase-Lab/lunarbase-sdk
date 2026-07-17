import type { Address, Word } from "@lunarbase/math";
import { Commitment, type ChainUpdate } from "@lunarbase/client-core";

/** Tracks monotonic Base Flashblocks payload indices and block context. */
export class BaseFlashblocksTracker {
  private payloadId?: string;
  private blockNumber?: bigint;
  private latestIndex?: bigint;

  /** Accepts a payload index and reports whether it starts a new payload. */
  observe(payloadId: string, blockNumber: bigint, index: bigint): boolean {
    if (this.payloadId === payloadId) {
      if (this.blockNumber !== blockNumber) throw new Error("Flashblocks payload changed block context");
      if (this.latestIndex !== undefined && index < this.latestIndex) throw new Error("Flashblocks index regression");
      if (this.latestIndex === index) return false;
      this.latestIndex = index;
      return false;
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

/** Provider-independent Flashblocks header. */
export interface FlashblockHeader {
  readonly payloadId: string;
  readonly blockNumber: bigint;
  readonly blockHash?: string;
  readonly index: bigint;
}

/** Pending log associated with one Flashblocks header. */
export interface FlashblockLog {
  readonly header: FlashblockHeader;
  readonly transactionIndex: bigint;
  readonly logIndex: bigint;
  readonly address: Address;
  readonly topics: readonly Word[];
  readonly data: string;
  readonly removed: boolean;
}

/** Converts Base Flashblocks payloads into normalized runtime updates. */
export class BaseFlashblocksNormalizer {
  private readonly tracker = new BaseFlashblocksTracker();

  /** Creates a normalizer for one Base chain id. */
  constructor(readonly chainId: bigint) {}

  /** Converts a Flashblocks header into a realtime head. */
  normalizeHeader(header: FlashblockHeader): ChainUpdate | undefined {
    if (!this.tracker.observe(header.payloadId, header.blockNumber, header.index)) return undefined;
    return {
      kind: "Head",
      cursor: {
        chainId: this.chainId,
        blockNumber: header.blockNumber,
        blockHash: header.blockHash,
        sourceSequence: header.index,
        commitment: Commitment.Realtime,
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
          commitment: Commitment.Realtime,
        },
      },
    });
    return updates;
  }

  /** Clears provisional Flashblocks state. */
  reset(): void {
    this.tracker.reset();
  }
}
