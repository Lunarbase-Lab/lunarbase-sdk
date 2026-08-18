import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { IndexerLifecycleEvent } from "../model.js";
import { LifecyclePublisher } from "./lifecycle_publisher.js";

test("lifecycle observer queue is count bounded and never invokes listeners inline", async () => {
  const publisher = new LifecyclePublisher();
  const received: IndexerLifecycleEvent[] = [];
  publisher.subscribe((event) => received.push(event));

  for (let index = 0; index < 65; index += 1) publisher.publish({ kind: "Gap", reason: `count-${index}` });

  assert.equal(received.length, 0);
  await Promise.resolve();
  assert.deepEqual(
    received.map(({ kind }) => kind),
    ["ObserverGap"],
  );
});

test("lifecycle observer queue has a byte bound and truncates diagnostics", async () => {
  const publisher = new LifecyclePublisher();
  const received: IndexerLifecycleEvent[] = [];
  publisher.subscribe((event) => received.push(event));

  for (let index = 0; index < 60; index += 1)
    publisher.publish({ kind: "Gap", reason: `${index}-${"x".repeat(2_000)}` });

  for (let turn = 0; turn < 8; turn += 1) await Promise.resolve();
  assert.ok(received.some(({ kind }) => kind === "ObserverGap"));
  assert.ok(
    received.every((event) => event.kind === "CorrectionApplied" || event.reason.length <= 1_024),
    "retained diagnostics must be truncated before enqueue",
  );
});

test("lifecycle delivery yields after a bounded microtask batch", async () => {
  const publisher = new LifecyclePublisher();
  const received: IndexerLifecycleEvent[] = [];
  publisher.subscribe((event) => received.push(event));
  for (let index = 0; index < 32; index += 1) publisher.publish({ kind: "Gap", reason: `batch-${index}` });

  await Promise.resolve();
  assert.equal(received.length, 16);
  await Promise.resolve();
  assert.equal(received.length, 16, "delivery must yield beyond the microtask queue");
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  assert.equal(received.length, 32);
});
