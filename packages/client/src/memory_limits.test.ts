import { strict as assert } from "node:assert";
import { test } from "node:test";
import { BoundedUpdateQueue } from "./indexer/update_queue.js";
import { chainUpdateRetainedBytes, Commitment, type ChainCursor, type ChainUpdate } from "./model.js";
import { CursorReorderBuffer } from "./state/ordering.js";

function head(blockNumber: bigint): ChainUpdate {
  const cursor: ChainCursor = {
    chainId: 143n,
    blockNumber,
    executionBlockNumber: blockNumber,
    commitment: Commitment.Realtime,
  };
  return { kind: "Head", cursor };
}

test("runtime queue replaces byte overflow with one explicit recovery gap", () => {
  const queue = new BoundedUpdateQueue(10, 1024);
  queue.push({ kind: "Gap", reason: "x".repeat(2048) });
  const updates = queue.drainAll();
  assert.equal(updates.length, 1);
  assert.equal(updates[0]?.kind, "Gap");
  assert.ok((updates[0]?.kind === "Gap" ? updates[0].reason : "").includes("byte budget exceeded"));
});

test("reorder byte charge is released and overflow poisons continuity", () => {
  const first = head(1n);
  const buffer = new CursorReorderBuffer(10, chainUpdateRetainedBytes(first));
  buffer.push(first);
  assert.deepEqual(buffer.drainAll(), [first]);
  buffer.push(head(2n));
  assert.throws(() => buffer.push(head(3n)), /byte budget exceeded/);
  assert.equal(buffer.isPoisoned(), true);
});
