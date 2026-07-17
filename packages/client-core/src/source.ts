/** Utilities shared by concrete `ChainDataSource` implementations. */
import type { ChainCursor, ChainUpdate } from "./model.js";

/** Returns the ordering cursor carried by a normalized update. */
export function updateCursor(update: ChainUpdate): ChainCursor | undefined {
  switch (update.kind) {
    case "Head":
      return update.cursor;
    case "Log":
      return update.log.cursor;
    case "Reorg":
      return update.newHead;
    case "Gap":
      return update.cursor;
  }
}

/** Compares stream positions without interpreting source sequence as a block. */
export function compareCursor(left: ChainCursor, right: ChainCursor): number {
  const leftTransportOrder =
    left.transactionIndex === undefined && left.logIndex === undefined ? (left.sourceSequence ?? 0n) : 0n;
  const rightTransportOrder =
    right.transactionIndex === undefined && right.logIndex === undefined ? (right.sourceSequence ?? 0n) : 0n;
  const fields = [
    [left.blockNumber, right.blockNumber],
    [left.transactionIndex ?? 0n, right.transactionIndex ?? 0n],
    [left.logIndex ?? 0n, right.logIndex ?? 0n],
    [leftTransportOrder, rightTransportOrder],
    [left.sourceSubIndex ?? 0n, right.sourceSubIndex ?? 0n],
  ] as const;
  for (const [a, b] of fields) {
    if (a < b) return -1;
    if (a > b) return 1;
  }
  return 0;
}
