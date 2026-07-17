/** Bounded deterministic ordering for concurrently decoded source messages. */
import type { ChainCursor, ChainUpdate } from "../model.js";

type CursorKey = readonly [bigint, bigint, bigint, bigint, bigint, number];

/** Reorder buffer that fails closed on overflow or conflicting cursor payloads. */
export class CursorReorderBuffer {
  private readonly pending = new Map<string, { key: CursorKey; update: ChainUpdate }>();
  private poisoned = false;

  /** Creates a buffer with a hard memory bound. */
  constructor(readonly capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) throw new Error("reorder buffer capacity must be positive");
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
    if (this.pending.size >= this.capacity) {
      this.poisoned = true;
      throw new Error("reorder buffer overflow; resnapshot required");
    }
    this.pending.set(encoded, { key, update });
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
    for (const entry of entries) this.pending.delete(encodeKey(entry.key));
    return entries.map(({ update }) => update);
  }
}

function cursorKey(cursor: ChainCursor, rank: number): CursorKey {
  return [
    cursor.blockNumber,
    cursor.transactionIndex ?? 0n,
    cursor.logIndex ?? 0n,
    cursor.sourceSequence ?? 0n,
    cursor.sourceSubIndex ?? 0n,
    rank,
  ];
}

function updateKey(update: ChainUpdate): CursorKey {
  switch (update.kind) {
    case "Head":
      return cursorKey(update.cursor, 0);
    case "Log":
      return cursorKey(update.log.cursor, 1);
    case "Reorg":
      return cursorKey(update.newHead, 2);
    case "Gap":
      return update.cursor ? cursorKey(update.cursor, 3) : [(1n << 256n) - 1n, 0n, 0n, 0n, 0n, 3];
  }
}

function watermarkKey(cursor: ChainCursor): CursorKey {
  if (cursor.transactionIndex === undefined && cursor.logIndex === undefined)
    return [cursor.blockNumber, (1n << 32n) - 1n, (1n << 32n) - 1n, (1n << 64n) - 1n, (1n << 32n) - 1n, 255];
  return [
    cursor.blockNumber,
    cursor.transactionIndex ?? 0n,
    cursor.logIndex ?? 0n,
    cursor.sourceSequence ?? 0n,
    cursor.sourceSubIndex ?? 0n,
    255,
  ];
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
