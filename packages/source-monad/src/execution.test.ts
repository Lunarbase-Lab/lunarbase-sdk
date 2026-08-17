import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Commitment } from "@lunarbase-lab/pmm-v2-client";
import { MonadExecutionNormalizer, MonadSequenceTracker } from "./execution.js";

test("Monad parser ordering accepts sparse positions and rejects regression", () => {
  const tracker = new MonadSequenceTracker();
  assert.equal(tracker.observe(10n, 0n), true);
  assert.equal(tracker.observe(10n, 0n), false);
  assert.equal(tracker.observe(15n, 0n), true);
  assert.throws(() => tracker.observe(14n, 0n));
});

test("Monad heads preserve optional proposal parent linkage", () => {
  const normalizer = new MonadExecutionNormalizer(143n);
  const update = normalizer.normalize({
    kind: "Head",
    head: {
      sequence: 10n,
      blockNumber: 42n,
      blockHash: `0x${"11".repeat(32)}`,
      parentHash: `0x${"22".repeat(32)}`,
      commitment: Commitment.Realtime,
    },
  });

  assert.equal(update?.kind, "Head");
  if (update?.kind === "Head") {
    assert.equal(update.head.cursor.blockHash, `0x${"11".repeat(32)}`);
    assert.equal(update.head.parentHash, `0x${"22".repeat(32)}`);
  }
});
