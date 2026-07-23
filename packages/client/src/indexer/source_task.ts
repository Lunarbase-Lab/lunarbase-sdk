/** Realtime subscription lifecycle, liveness watchdog, and reconnect loop. */
import type { ChainDataSource, ContractFilter } from "../model.js";
import type { BoundedUpdateQueue } from "./update_queue.js";

/** Observable transport activity used to gate bootstrap and recovery. */
export class SourceActivity {
  /** Waiters interested in the next established subscription. */
  private readonly waiters = new Set<() => void>();
  /** Whether the current transport completed its protocol handshake. */
  private active = false;

  /** Publishes a transport state transition. */
  setActive(active: boolean): void {
    this.active = active;
    if (active) {
      for (const waiter of this.waiters) waiter();
      this.waiters.clear();
    }
  }

  /** Waits for an acknowledged subscription or cooperative cancellation. */
  async waitUntilActive(signal: AbortSignal): Promise<boolean> {
    if (this.active) return true;
    if (signal.aborted) return false;
    return new Promise((resolve) => {
      const ready = () => {
        signal.removeEventListener("abort", aborted);
        this.waiters.delete(ready);
        resolve(true);
      };
      const aborted = () => {
        this.waiters.delete(ready);
        resolve(false);
      };
      this.waiters.add(ready);
      signal.addEventListener("abort", aborted, { once: true });
    });
  }
}

/** Pumps one acknowledged stream at a time into the bounded reducer queue. */
export async function pumpSource(
  source: ChainDataSource,
  filter: ContractFilter,
  queue: BoundedUpdateQueue,
  activity: SourceActivity,
  signal: AbortSignal,
  reconnectDelayMilliseconds: number,
  stallTimeoutMilliseconds: number,
): Promise<void> {
  let everActive = false;
  while (!signal.aborted && !queue.closed) {
    const attempt = linkedController(signal);
    activity.setActive(false);
    try {
      const stream = await source.subscribe(filter, attempt.signal);
      everActive = true;
      activity.setActive(true);
      await consumeStream(stream, queue, attempt, signal, stallTimeoutMilliseconds);
    } catch (error) {
      if (signal.aborted || queue.closed) return;
      if (everActive)
        queue.push({
          kind: "Gap",
          reason: `source failed: ${message(error)}`,
        });
    } finally {
      activity.setActive(false);
      attempt.abort();
    }
    await delay(reconnectDelayMilliseconds, signal);
  }
}

async function consumeStream(
  stream: AsyncIterable<import("../model.js").ChainUpdate>,
  queue: BoundedUpdateQueue,
  attempt: AbortController,
  signal: AbortSignal,
  stallTimeoutMilliseconds: number,
): Promise<void> {
  const iterator = stream[Symbol.asyncIterator]();
  try {
    while (!signal.aborted && !queue.closed) {
      const result = await nextWithTimeout(iterator, stallTimeoutMilliseconds, attempt);
      if (result === "timeout") {
        queue.push({ kind: "Gap", reason: "realtime source stalled; canonical recovery required" });
        return;
      }
      if (result.done) {
        queue.push({ kind: "Gap", reason: "source stream ended; canonical recovery required" });
        return;
      }
      queue.push(result.value);
      if (result.value.kind === "Gap") return;
    }
  } finally {
    attempt.abort();
    await iterator.return?.();
  }
}

async function nextWithTimeout(
  iterator: AsyncIterator<import("../model.js").ChainUpdate>,
  milliseconds: number,
  attempt: AbortController,
): Promise<IteratorResult<import("../model.js").ChainUpdate> | "timeout"> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => {
      attempt.abort();
      resolve("timeout");
    }, milliseconds);
  });
  try {
    return await Promise.race([iterator.next(), timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

export function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(done, milliseconds);
    function done() {
      signal.removeEventListener("abort", done);
      clearTimeout(timer);
      resolve();
    }
    signal.addEventListener("abort", done, { once: true });
  });
}

function linkedController(parent: AbortSignal): AbortController {
  const controller = new AbortController();
  if (parent.aborted) controller.abort();
  else {
    const abort = () => controller.abort();
    parent.addEventListener("abort", abort, { once: true });
    controller.signal.addEventListener("abort", () => parent.removeEventListener("abort", abort), {
      once: true,
    });
  }
  return controller;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
