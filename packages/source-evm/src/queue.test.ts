import { strict as assert } from "node:assert";
import { test } from "node:test";
import { BoundedFrameQueue } from "./index.js";

test("frame queue preserves FIFO order across ring wrap-around", async () => {
  const queue = new BoundedFrameQueue(3, 1024);
  queue.push("one");
  queue.push("two");
  queue.push("three");
  assert.equal(await queue.next(), "one");
  queue.push("four");
  assert.equal(await queue.next(), "two");
  assert.equal(await queue.next(), "three");
  assert.equal(await queue.next(), "four");
});

test("frame queue removes abort listeners after delivery and cancellation", async () => {
  const deliveredSignal = new TrackedAbortSignal();
  const queue = new BoundedFrameQueue(2, 1024);
  const delivered = queue.next(deliveredSignal.signal);
  queue.push("frame");
  assert.equal(await delivered, "frame");
  assert.equal(deliveredSignal.listeners, 0);

  const cancelledSignal = new TrackedAbortSignal();
  const cancelled = queue.next(cancelledSignal.signal);
  cancelledSignal.abort();
  assert.equal(await cancelled, undefined);
  assert.equal(cancelledSignal.listeners, 0);
});

class TrackedAbortSignal {
  private readonly callbacks = new Set<EventListenerOrEventListenerObject>();
  readonly signal = this as unknown as AbortSignal;
  aborted = false;

  get listeners(): number {
    return this.callbacks.size;
  }

  addEventListener(type: string, callback: EventListenerOrEventListenerObject | null): void {
    if (type === "abort" && callback) this.callbacks.add(callback);
  }

  removeEventListener(type: string, callback: EventListenerOrEventListenerObject | null): void {
    if (type === "abort" && callback) this.callbacks.delete(callback);
  }

  abort(): void {
    if (this.aborted) return;
    this.aborted = true;
    for (const callback of [...this.callbacks]) {
      if (typeof callback === "function") callback.call(this.signal, new Event("abort"));
      else callback.handleEvent(new Event("abort"));
    }
  }
}
