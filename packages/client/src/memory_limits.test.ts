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
  return { kind: "Head", head: { cursor } };
}

test("runtime queue replaces byte overflow with one explicit recovery gap", () => {
  const queue = new BoundedUpdateQueue(10, 1024);
  queue.push({ kind: "Gap", reason: "x".repeat(2048) });
  const updates = queue.drainAll();
  assert.equal(updates.length, 1);
  assert.equal(updates[0]?.kind, "Gap");
  assert.ok((updates[0]?.kind === "Gap" ? updates[0].reason : "").includes("byte budget exceeded"));
});

test("runtime queue releases the immutable admission charge after caller mutation", async () => {
  const queue = new BoundedUpdateQueue(4, 1024);
  const aliased: ChainUpdate = { kind: "Gap", reason: "x" };
  queue.push(aliased);
  aliased.reason = "y".repeat(2048);

  assert.equal(await queue.next(new AbortController().signal), aliased);
  queue.push({ kind: "Gap", reason: "z".repeat(1200) });

  const overflow = await queue.next(new AbortController().signal);
  assert.equal(overflow?.kind, "Gap");
  assert.ok((overflow?.kind === "Gap" ? overflow.reason : "").includes("byte budget exceeded"));
});

test("runtime queue overflow retains the highest discarded cursor watermark", async () => {
  const queue = new BoundedUpdateQueue(1, 1024);
  queue.push(head(1n));
  queue.push(head(2n));
  queue.push(head(3n));

  const overflow = await queue.next(new AbortController().signal);
  assert.equal(overflow?.kind, "Gap");
  assert.equal(overflow?.kind === "Gap" ? overflow.cursor?.blockNumber : undefined, 3n);
});

test("runtime queue keeps mixed-chain overflow coverage unprovable", async () => {
  const queue = new BoundedUpdateQueue(1, 1024);
  queue.push(head(1n));
  queue.push({
    kind: "Head",
    head: {
      cursor: {
        chainId: 1n,
        blockNumber: 2n,
        executionBlockNumber: 2n,
        commitment: Commitment.Realtime,
      },
    },
  });
  queue.push(head(3n));

  const overflow = await queue.next(new AbortController().signal);
  assert.equal(overflow?.kind, "Gap");
  assert.equal(overflow?.kind === "Gap" ? overflow.cursor : undefined, undefined);
});

test("cursorless gap dominates every cursor during overflow collapse", async () => {
  const queue = new BoundedUpdateQueue(2, 1024);
  queue.push(head(1n));
  queue.push(head(2n));
  queue.push({ kind: "Gap", reason: "unknown source horizon" });

  const overflow = await queue.next(new AbortController().signal);
  assert.equal(overflow?.kind, "Gap");
  assert.equal(overflow?.kind === "Gap" ? overflow.cursor : undefined, undefined);
});

test("cursorless gap erases an existing overflow watermark", async () => {
  const queue = new BoundedUpdateQueue(1, 1024);
  queue.push(head(1n));
  queue.push(head(2n));
  queue.push({ kind: "Gap", reason: "unknown source horizon" });

  const overflow = await queue.next(new AbortController().signal);
  assert.equal(overflow?.kind, "Gap");
  assert.equal(overflow?.kind === "Gap" ? overflow.cursor : undefined, undefined);
});

test("oversized borrowed source updates never invoke the ownership clone", async () => {
  const queue = new BoundedUpdateQueue(4, 1024);
  let ownershipCalls = 0;
  queue.pushBorrowed({ kind: "Gap", reason: "x".repeat(2_048) }, (update) => {
    ownershipCalls += 1;
    return update;
  });

  assert.equal(ownershipCalls, 0);
  assert.equal((await queue.next(new AbortController().signal))?.kind, "Gap");
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
