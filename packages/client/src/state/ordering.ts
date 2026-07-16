import type { ChainCursor, ChainUpdate } from "../model.js";

type CursorKey = readonly [bigint, bigint, bigint, number];

/** Bounded deterministic handoff buffer for parallel transport/decode work. */
export class CursorReorderBuffer {
  private readonly pending = new Map<string, { key: CursorKey; update: ChainUpdate }>();
  private poisoned = false;

  /** Creates a bounded reorder buffer. */
  constructor(readonly capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) throw new Error("reorder buffer capacity must be positive");
  }

  /** Returns the number of pending unique cursor entries. */
  len(): number {
    return this.pending.size;
  }
  /** Returns whether no updates are pending. */
  isEmpty(): boolean {
    return this.pending.size === 0;
  }
  /** Returns whether a conflict or overflow poisoned the buffer. */
  isPoisoned(): boolean {
    return this.poisoned;
  }

  /** Inserts an update, deduplicating identical cursor payloads. */
  push(update: ChainUpdate): boolean {
    if (this.poisoned) throw new Error("reorder buffer is poisoned; resnapshot required");
    const key = updateKey(update);
    const encoded = key.map((value) => value.toString()).join(":");
    const existing = this.pending.get(encoded);
    if (existing) {
      if (stable(existing.update) === stable(update)) return false;
      this.poisoned = true;
      throw new Error("conflicting updates share one cursor");
    }
    if (this.pending.size >= this.capacity) {
      this.poisoned = true;
      throw new Error("reorder buffer overflow; resnapshot required");
    }
    this.pending.set(encoded, { key, update });
    return true;
  }

  /** Releases updates at or before the supplied cursor watermark. */
  drainThrough(watermark: ChainCursor): ChainUpdate[] {
    const limit = watermarkKey(watermark);
    return this.drain((entry) => compareKey(entry.key, limit) <= 0);
  }

  /** Releases every pending update in deterministic cursor order. */
  drainAll(): ChainUpdate[] {
    return this.drain(() => true);
  }

  private drain(predicate: (entry: { key: CursorKey; update: ChainUpdate }) => boolean): ChainUpdate[] {
    const entries = [...this.pending.values()].filter(predicate).sort((left, right) => compareKey(left.key, right.key));
    for (const entry of entries) this.pending.delete(entry.key.map((value) => value.toString()).join(":"));
    return entries.map(({ update }) => update);
  }
}

function cursorKey(cursor: ChainCursor, rank: number): CursorKey {
  return [cursor.blockNumber, cursor.transactionIndex ?? 0n, cursor.logIndex ?? 0n, rank];
}
function updateKey(update: ChainUpdate): CursorKey {
  return update.kind === "Head"
    ? cursorKey(update.cursor, 0)
    : update.kind === "Log"
      ? cursorKey(update.log.cursor, 1)
      : update.kind === "Reorg"
        ? cursorKey(update.newHead, 2)
        : update.kind === "Gap"
          ? update.cursor
            ? cursorKey(update.cursor, 3)
            : [2n ** 64n - 1n, 0n, 0n, 3]
          : [0n, 0n, 0n, 4];
}
function watermarkKey(cursor: ChainCursor): CursorKey {
  return cursor.transactionIndex === undefined && cursor.logIndex === undefined
    ? [cursor.blockNumber, 2n ** 32n - 1n, 2n ** 32n - 1n, 255]
    : [cursor.blockNumber, cursor.transactionIndex ?? 0n, cursor.logIndex ?? 0n, 255];
}
function compareKey(left: CursorKey, right: CursorKey): number {
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] < right[index]) return -1;
    if (left[index] > right[index]) return 1;
  }
  return 0;
}
function stable(value: unknown): string {
  return JSON.stringify(value, (_key, item) => (typeof item === "bigint" ? `${item}n` : item));
}
