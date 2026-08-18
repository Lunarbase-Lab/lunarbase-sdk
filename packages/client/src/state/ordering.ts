/** Bounded deterministic ordering for concurrently decoded source messages. */
import { chainUpdateRetainedBytes, type ChainCursor, type ChainUpdate } from "../model.js";

type CursorKey = readonly [bigint, bigint, bigint, bigint, bigint, number];

/** Reorder buffer that fails closed on overflow or conflicting cursor payloads. */
export class CursorReorderBuffer {
  /** Updates indexed by their normalized deterministic source position. */
  private readonly pending = new Map<string, { key: CursorKey; update: ChainUpdate }>();
  /** Sticky continuity-failure flag cleared only by creating a new buffer. */
  private poisoned = false;
  /** Conservative bytes retained by pending updates. */
  private pendingBytes = 0;

  /** Creates a buffer with a hard memory bound. */
  constructor(
    /** Maximum retained updates before continuity fails closed. */
    readonly capacity: number,
    /** Maximum retained bytes before continuity fails closed. */
    readonly byteCapacity: number = capacity * 64 * 1024,
  ) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) throw new Error("reorder buffer capacity must be positive");
    if (!Number.isSafeInteger(byteCapacity) || byteCapacity <= 0)
      throw new Error("reorder buffer byte capacity must be positive");
  }

  /** Returns the number of messages awaiting a head watermark. */
  len(): number {
    return this.pending.size;
  }

  /** Returns whether no message is buffered. */
  isEmpty(): boolean {
    return this.pending.size === 0;
  }

  /** Returns whether recovery is required. */
  isPoisoned(): boolean {
    return this.poisoned;
  }

  /** Inserts one message; a repeated cursor requires canonical recovery. */
  push(update: ChainUpdate): void {
    if (this.poisoned) throw new Error("reorder buffer is poisoned; resnapshot required");
    const key = updateKey(update);
    const encoded = encodeKey(key);
    if (this.pending.has(encoded)) {
      this.poisoned = true;
      throw new Error("multiple updates share one cursor");
    }
    const bytes = chainUpdateRetainedBytes(update);
    if (this.pending.size >= this.capacity || bytes > this.byteCapacity - this.pendingBytes) {
      this.poisoned = true;
      throw new Error("reorder buffer count or byte budget exceeded; resnapshot required");
    }
    this.pending.set(encoded, { key, update });
    this.pendingBytes += bytes;
  }

  /** Releases all messages through one block or log watermark. */
  drainThrough(watermark: ChainCursor): ChainUpdate[] {
    const limit = watermarkKey(watermark);
    return this.drain(({ key }) => compareKey(key, limit) <= 0);
  }

  /** Releases every pending message in cursor order. */
  drainAll(): ChainUpdate[] {
    return this.drain(() => true);
  }

  private drain(predicate: (entry: { key: CursorKey; update: ChainUpdate }) => boolean): ChainUpdate[] {
    const entries = [...this.pending.values()].filter(predicate).sort((left, right) => compareKey(left.key, right.key));
    for (const entry of entries) {
      this.pending.delete(encodeKey(entry.key));
      this.pendingBytes -= chainUpdateRetainedBytes(entry.update);
    }
    return entries.map(({ update }) => update);
  }
}

function cursorKey(cursor: ChainCursor, rank: number): CursorKey {
  const positioned = cursor.transactionIndex !== undefined && cursor.logIndex !== undefined;
  const transportOrder = positioned ? 0n : (cursor.sourceSequence ?? 0n);
  const transportSubIndex = positioned ? 0n : (cursor.sourceSubIndex ?? 0n);
  return [
    cursor.blockNumber,
    cursor.transactionIndex ?? 0n,
    cursor.logIndex ?? 0n,
    transportOrder,
    transportSubIndex,
    rank,
  ];
}

function updateKey(update: ChainUpdate): CursorKey {
  switch (update.kind) {
    case "Head":
      return cursorKey(update.head.cursor, 0);
    case "Log":
      return cursorKey(update.log.cursor, 1);
    case "Correction":
      return cursorKey(update.correction.newTip.cursor, 2);
    case "Reorg":
      return cursorKey(update.newHead.cursor, 3);
    case "Gap":
      return update.cursor ? cursorKey(update.cursor, 4) : [(1n << 256n) - 1n, 0n, 0n, 0n, 0n, 4];
  }
}

function watermarkKey(cursor: ChainCursor): CursorKey {
  if (cursor.transactionIndex === undefined && cursor.logIndex === undefined)
    return [cursor.blockNumber, (1n << 32n) - 1n, (1n << 32n) - 1n, (1n << 64n) - 1n, (1n << 32n) - 1n, 255];
  const transportOrder = 0n;
  return [cursor.blockNumber, cursor.transactionIndex ?? 0n, cursor.logIndex ?? 0n, transportOrder, 0n, 255];
}

function encodeKey(key: CursorKey): string {
  return key.map(String).join(":");
}

function compareKey(left: CursorKey, right: CursorKey): number {
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] < right[index]) return -1;
    if (left[index] > right[index]) return 1;
  }
  return 0;
}
