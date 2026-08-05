import { strict as assert } from "node:assert";
import { test } from "node:test";
import { MonadSequenceTracker } from "./execution.js";

test("Monad parser ordering accepts sparse positions and rejects regression", () => {
  const tracker = new MonadSequenceTracker();
  assert.equal(tracker.observe(10n, 0n), true);
  assert.equal(tracker.observe(10n, 0n), false);
  assert.equal(tracker.observe(15n, 0n), true);
  assert.throws(() => tracker.observe(14n, 0n));
});
