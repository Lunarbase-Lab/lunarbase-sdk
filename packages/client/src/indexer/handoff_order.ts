/** Receive-order-safe bootstrap handoff ordering. */
import type { ChainUpdate } from "../model.js";
import { compareCursor, updateCursor } from "../source.js";

/** Sorts ordinary head/log bursts without moving updates across lifecycle barriers. */
export function orderHandoffUpdates(buffered: readonly ChainUpdate[]): ChainUpdate[] {
  const ordered: ChainUpdate[] = [];
  let segment: ChainUpdate[] = [];
  const flush = () => {
    segment.sort((left, right) => {
      const a = updateCursor(left)!;
      const b = updateCursor(right)!;
      return compareCursor(a, b);
    });
    ordered.push(...segment);
    segment = [];
  };
  for (const update of buffered) {
    if (isBarrier(update)) {
      flush();
      ordered.push(update);
    } else {
      segment.push(update);
    }
  }
  flush();
  return ordered;
}

function isBarrier(update: ChainUpdate): boolean {
  return update.kind === "Correction" || update.kind === "Reorg" || update.kind === "Gap";
}
