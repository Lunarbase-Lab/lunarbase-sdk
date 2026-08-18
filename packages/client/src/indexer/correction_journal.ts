/** Bounded compact undo journal for optimistic quote-state corrections. */
import { IndexerError, type BlockRef, type ChainCursor } from "../model.js";
import { QuoteReducer } from "../state/reducer.js";
import { reducerUndoRetainedBytes, type ReducerUndo } from "../state/reducer_undo.js";
import { BoundedRingBuffer } from "./ring_buffer.js";

const ENTRY_OVERHEAD_BYTES = 192;

/** Hard protocol-compatible rollback horizon. */
export const MAX_CORRECTION_HISTORY_BLOCKS = 128;
/** Hard compact before-image memory budget. */
export const MAX_CORRECTION_HISTORY_BYTES = 16 * 1024 * 1024;
/** Default rollback horizon for direct in-memory QuoteIndexer users. */
export const DEFAULT_CORRECTION_HISTORY_BLOCKS = MAX_CORRECTION_HISTORY_BLOCKS;
/** Default compact journal memory budget. */
export const DEFAULT_CORRECTION_HISTORY_BYTES = MAX_CORRECTION_HISTORY_BYTES;

/** Hard count and byte limits for compact optimistic before-images. */
export interface CorrectionJournalLimits {
  readonly blockCapacity: number;
  readonly byteCapacity: number;
}

interface UndoBlock {
  readonly block: BlockRef;
  readonly undos: ReducerUndo[];
  bytes: number;
}

type CursorIdentity = Pick<ChainCursor, "chainId" | "blockNumber" | "executionBlockNumber" | "blockHash">;

interface BlockIdentity extends CursorIdentity {
  blockHash: NonNullable<ChainCursor["blockHash"]>;
}

/** Candidate state and retained shared prefix produced without mutating live state. */
export interface CorrectionCandidate {
  readonly reducer: QuoteReducer;
  readonly journal: CorrectionJournal;
}

/** Stores only touched lane/router before-images, never complete state clones. */
export class CorrectionJournal {
  private readonly blocks: BoundedRingBuffer<UndoBlock>;
  private readonly observedBlocks: BoundedRingBuffer<BlockIdentity>;
  /** Constant-time identity lookup for the bounded observed header window. */
  private readonly observedByNumber = new Map<bigint, BlockIdentity>();
  private observedEvictionFloor?: bigint;
  readonly limits: CorrectionJournalLimits;
  private retainedBytesValue = 0;
  /** Cumulative eventful-block evictions since this journal lineage was created. */
  private evictionsValue = 0;

  constructor(
    private rollbackFloor: BlockRef,
    limits: CorrectionJournalLimits,
  ) {
    validateLimits(limits);
    this.limits = Object.freeze({ blockCapacity: limits.blockCapacity, byteCapacity: limits.byteCapacity });
    this.rollbackFloor = cloneBlock(rollbackFloor);
    this.blocks = new BoundedRingBuffer(this.limits.blockCapacity);
    this.observedBlocks = new BoundedRingBuffer(this.limits.blockCapacity);
  }

  /** Retains a bounded block identity even when the block changed no quote state. */
  observe(cursor: ChainCursor): void {
    if (!cursor.blockHash) return;
    const retained = this.observedByNumber.get(cursor.blockNumber);
    if (retained) {
      retained.chainId = cursor.chainId;
      retained.executionBlockNumber = cursor.executionBlockNumber;
      retained.blockHash = cursor.blockHash;
      return;
    }
    if (this.observedBlocks.length === this.observedBlocks.capacity) this.evictObserved();
    const observed = identity(cursor);
    this.observedBlocks.push(observed);
    this.observedByNumber.set(observed.blockNumber, observed);
  }

  /** Rejects state mutation after its prior block identity left the bounded window. */
  validateMutation(cursor: ChainCursor): void {
    if (!cursor.blockHash) throw new IndexerError("GAP", "optimistic state mutation has no block hash");
    if (this.observedEvictionFloor !== undefined && cursor.blockNumber <= this.observedEvictionFloor)
      throw new IndexerError("GAP", "optimistic mutation is outside retained block identity history");
    const observed = this.observedByNumber.get(cursor.blockNumber);
    if (observed) assertHash(observed, cursor, "optimistic mutation conflicts with observed block identity");
  }

  /** Records one state mutation after successful ordered reduction. */
  record(cursor: ChainCursor, undo: ReducerUndo): void {
    if (!cursor.blockHash) throw new IndexerError("GAP", "optimistic state mutation has no block hash");
    const bytes = reducerUndoRetainedBytes(undo);
    const last = this.blocks.peek(this.blocks.length - 1);
    if (last?.block.cursor.blockNumber === cursor.blockNumber) {
      assertHash(last.block.cursor, cursor, "multiple hashes observed for one journal block");
      this.makeRoom(bytes, false, last);
      last.undos.push(undo);
      last.bytes += bytes;
      this.retainedBytesValue += bytes;
      return;
    }
    if (last && cursor.blockNumber < last.block.cursor.blockNumber)
      throw new IndexerError("GAP", "optimistic journal block regression");
    const entryBytes = ENTRY_OVERHEAD_BYTES + bytes;
    if (entryBytes > this.limits.byteCapacity)
      throw new IndexerError("GAP", "optimistic journal entry exceeds its byte budget");
    this.makeRoom(entryBytes, true);
    const entry: UndoBlock = {
      block: { cursor: { ...cursor } },
      undos: [undo],
      bytes: entryBytes,
    };
    if (!this.blocks.push(entry)) throw new IndexerError("GAP", "optimistic journal count budget exceeded");
    this.retainedBytesValue += entryBytes;
    this.observe(cursor);
  }

  /** Builds an isolated rollback candidate while live quotes retain old state. */
  candidate(current: QuoteReducer, commonAncestor: BlockRef, oldBranch: readonly BlockRef[]): CorrectionCandidate {
    this.validateAncestor(commonAncestor);
    this.validateOldBranch(commonAncestor.cursor.blockNumber, oldBranch);
    const reducer = current.fork();
    const prefix = new CorrectionJournal(this.rollbackFloor, this.limits);
    prefix.evictionsValue = this.evictionsValue;
    prefix.observedEvictionFloor = this.observedEvictionFloor;
    for (let offset = this.blocks.length - 1; offset >= 0; offset -= 1) {
      const entry = this.blocks.peek(offset)!;
      if (entry.block.cursor.blockNumber <= commonAncestor.cursor.blockNumber) continue;
      for (let index = entry.undos.length - 1; index >= 0; index -= 1) reducer.revert(entry.undos[index]!);
    }
    reducer.prepareCorrection(commonAncestor.cursor);
    for (let offset = 0; offset < this.blocks.length; offset += 1) {
      const entry = this.blocks.peek(offset)!;
      if (entry.block.cursor.blockNumber > commonAncestor.cursor.blockNumber) break;
      prefix.pushExisting(entry);
    }
    for (let offset = 0; offset < this.observedBlocks.length; offset += 1) {
      const observed = this.observedBlocks.peek(offset)!;
      if (observed.blockNumber <= commonAncestor.cursor.blockNumber) prefix.pushObserved(observed);
    }
    return { reducer, journal: prefix };
  }

  /** Number of eventful blocks retained for correction. */
  get blockCount(): number {
    return this.blocks.length;
  }

  /** Conservative bytes retained by compact before-images. */
  get retainedBytes(): number {
    return this.retainedBytesValue;
  }

  /** Cumulative number of history blocks evicted by count or byte pressure. */
  get evictionCount(): number {
    return this.evictionsValue;
  }

  private makeRoom(additionalBytes: number, addsBlock: boolean, protectedEntry?: UndoBlock): void {
    while (
      (addsBlock && this.blocks.length >= this.limits.blockCapacity) ||
      additionalBytes > this.limits.byteCapacity - this.retainedBytesValue
    ) {
      const oldest = this.blocks.peek();
      if (!oldest || oldest === protectedEntry)
        throw new IndexerError("GAP", "optimistic journal byte budget exceeded within one block");
      this.blocks.shift();
      this.retainedBytesValue -= oldest.bytes;
      this.evictionsValue = Math.min(Number.MAX_SAFE_INTEGER, this.evictionsValue + 1);
      this.rollbackFloor = cloneBlock(oldest.block);
    }
  }

  private validateAncestor(ancestor: BlockRef): void {
    if (ancestor.cursor.chainId !== this.rollbackFloor.cursor.chainId)
      throw new IndexerError("GAP", "correction ancestor chain id mismatch");
    if (ancestor.cursor.blockNumber < this.rollbackFloor.cursor.blockNumber)
      throw new IndexerError("GAP", "correction exceeds retained optimistic history");
    if (ancestor.cursor.blockNumber === this.rollbackFloor.cursor.blockNumber) {
      assertHash(this.rollbackFloor.cursor, ancestor.cursor, "correction ancestor conflicts with rollback floor");
      return;
    }
    for (let offset = 0; offset < this.blocks.length; offset += 1) {
      const block = this.blocks.peek(offset)!.block;
      if (block.cursor.blockNumber === ancestor.cursor.blockNumber) {
        assertHash(block.cursor, ancestor.cursor, "correction ancestor conflicts with journal history");
        return;
      }
      if (block.cursor.blockNumber > ancestor.cursor.blockNumber) break;
    }
    for (let offset = 0; offset < this.observedBlocks.length; offset += 1) {
      const observed = this.observedBlocks.peek(offset)!;
      if (observed.blockNumber !== ancestor.cursor.blockNumber) continue;
      assertHash(observed, ancestor.cursor, "correction ancestor conflicts with observed block history");
      return;
    }
    throw new IndexerError("GAP", "correction ancestor identity is not retained");
  }

  private validateOldBranch(ancestorNumber: bigint, oldBranch: readonly BlockRef[]): void {
    let branchOffset = 0;
    for (let offset = 0; offset < this.blocks.length; offset += 1) {
      const retained = this.blocks.peek(offset)!.block.cursor;
      if (retained.blockNumber <= ancestorNumber) continue;
      while (branchOffset < oldBranch.length && oldBranch[branchOffset]!.cursor.blockNumber < retained.blockNumber)
        branchOffset += 1;
      const branch = oldBranch[branchOffset]?.cursor;
      if (!branch || branch.blockNumber !== retained.blockNumber)
        throw new IndexerError("GAP", "old correction branch does not cover retained optimistic state");
      assertHash(retained, branch, "old correction branch conflicts with retained optimistic state");
    }
    for (let offset = 0; offset < this.observedBlocks.length; offset += 1) {
      const retained = this.observedBlocks.peek(offset)!;
      if (retained.blockNumber <= ancestorNumber) continue;
      const branch = oldBranch.find((block) => block.cursor.blockNumber === retained.blockNumber)?.cursor;
      if (!branch) throw new IndexerError("GAP", "old correction branch does not cover observed block history");
      assertHash(retained, branch, "old correction branch conflicts with observed block history");
    }
  }

  private pushExisting(entry: UndoBlock): void {
    if (!this.blocks.push(entry)) throw new IndexerError("GAP", "correction prefix exceeds journal count budget");
    this.retainedBytesValue += entry.bytes;
  }

  private evictObserved(): void {
    const removed = this.observedBlocks.shift();
    if (!removed) return;
    this.observedByNumber.delete(removed.blockNumber);
    if (this.observedEvictionFloor === undefined || removed.blockNumber > this.observedEvictionFloor)
      this.observedEvictionFloor = removed.blockNumber;
  }

  private pushObserved(observed: BlockIdentity): void {
    const retained = this.observedByNumber.get(observed.blockNumber);
    if (retained) {
      retained.chainId = observed.chainId;
      retained.executionBlockNumber = observed.executionBlockNumber;
      retained.blockHash = observed.blockHash;
      return;
    }
    if (this.observedBlocks.length === this.observedBlocks.capacity) this.evictObserved();
    const cloned = { ...observed };
    this.observedBlocks.push(cloned);
    this.observedByNumber.set(cloned.blockNumber, cloned);
  }
}

function validateLimits(limits: CorrectionJournalLimits): void {
  if (!Number.isSafeInteger(limits.blockCapacity) || limits.blockCapacity <= 0)
    throw new IndexerError("INVALID_REQUEST", "correction history block capacity must be positive");
  if (limits.blockCapacity > MAX_CORRECTION_HISTORY_BLOCKS)
    throw new IndexerError("INVALID_REQUEST", "correction history block capacity must be at most 128");
  if (!Number.isSafeInteger(limits.byteCapacity) || limits.byteCapacity < 1024)
    throw new IndexerError("INVALID_REQUEST", "correction history byte capacity must be at least 1024");
  if (limits.byteCapacity > MAX_CORRECTION_HISTORY_BYTES)
    throw new IndexerError("INVALID_REQUEST", "correction history byte capacity must be at most 16 MiB");
}

function assertHash(left: CursorIdentity, right: CursorIdentity, message: string): void {
  if (
    left.chainId !== right.chainId ||
    left.blockNumber !== right.blockNumber ||
    left.executionBlockNumber !== right.executionBlockNumber ||
    !left.blockHash ||
    !right.blockHash ||
    left.blockHash.toLowerCase() !== right.blockHash.toLowerCase()
  )
    throw new IndexerError("GAP", message);
}

function cloneBlock(block: BlockRef): BlockRef {
  return { cursor: { ...block.cursor }, parentHash: block.parentHash };
}

function identity(cursor: ChainCursor): BlockIdentity {
  return {
    chainId: cursor.chainId,
    blockNumber: cursor.blockNumber,
    executionBlockNumber: cursor.executionBlockNumber,
    blockHash: cursor.blockHash!,
  };
}
