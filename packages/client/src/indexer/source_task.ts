/** Realtime subscription lifecycle, liveness watchdog, and reconnect loop. */
import type {
  BlockRef,
  ChainCorrection,
  ChainCursor,
  ChainDataSource,
  ChainUpdate,
  ContractFilter,
  ContractLog,
} from "../model.js";
import type { BoundedUpdateQueue } from "./update_queue.js";
import { withDeadline } from "./lifecycle.js";

/** Observable transport activity used to gate bootstrap and recovery. */
export class SourceActivity {
  /** Waiters interested in the next established subscription. */
  private readonly waiters = new Set<(generation: number) => void>();
  private readonly inactiveListeners = new Set<() => void>();
  /** Whether the current transport completed its protocol handshake. */
  private active = false;
  private generationValue = 0;

  /** Publishes a transport state transition. */
  setActive(active: boolean): void {
    if (this.active === active) return;
    this.active = active;
    this.generationValue += 1;
    if (active) {
      for (const waiter of this.waiters) waiter(this.generationValue);
      this.waiters.clear();
    } else {
      for (const listener of this.inactiveListeners) listener();
    }
  }

  /** Waits for an acknowledged subscription or cooperative cancellation. */
  async waitUntilActive(signal: AbortSignal): Promise<boolean> {
    return (await this.waitForLease(signal)) !== undefined;
  }

  /** Returns the generation of the acknowledged subscription that satisfied the wait. */
  waitForLease(signal: AbortSignal): Promise<number | undefined> {
    if (this.active) return Promise.resolve(this.generationValue);
    if (signal.aborted) return Promise.resolve(undefined);
    return new Promise((resolve) => {
      let settled = false;
      const finish = (generation: number | undefined) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", aborted);
        this.waiters.delete(ready);
        resolve(generation);
      };
      const ready = (generation: number) => finish(generation);
      const aborted = () => finish(undefined);
      this.waiters.add(ready);
      signal.addEventListener("abort", aborted, { once: true });
      if (signal.aborted) aborted();
    });
  }

  /** Verifies that no disconnect/reconnect ABA occurred since a snapshot attempt began. */
  isCurrent(generation: number): boolean {
    return this.active && this.generationValue === generation;
  }

  /** Registers a synchronous readiness revocation hook for true transport inactivity. */
  onInactive(listener: () => void): () => void {
    this.inactiveListeners.add(listener);
    if (!this.active) listener();
    return () => this.inactiveListeners.delete(listener);
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
  operationTimeoutMilliseconds: number,
): Promise<void> {
  let everActive = false;
  while (!signal.aborted && !queue.closed) {
    const attempt = linkedController(signal);
    activity.setActive(false);
    try {
      const stream = await withDeadline(
        "source subscription",
        operationTimeoutMilliseconds,
        signal,
        () => source.subscribe(filter, attempt.signal),
        () => attempt.abort(),
      );
      everActive = true;
      activity.setActive(true);
      await consumeStream(
        stream,
        queue,
        activity,
        attempt,
        signal,
        stallTimeoutMilliseconds,
        operationTimeoutMilliseconds,
      );
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
  activity: SourceActivity,
  attempt: AbortController,
  signal: AbortSignal,
  stallTimeoutMilliseconds: number,
  operationTimeoutMilliseconds: number,
): Promise<void> {
  const iterator = stream[Symbol.asyncIterator]();
  try {
    while (!signal.aborted && !queue.closed) {
      const result = await nextWithTimeout(iterator, stallTimeoutMilliseconds, attempt);
      if (result === "cancelled") return;
      if (result === "timeout") {
        activity.setActive(false);
        queue.push({ kind: "Gap", reason: "realtime source stalled; canonical recovery required" });
        return;
      }
      if (result.done) {
        activity.setActive(false);
        queue.push({ kind: "Gap", reason: "source stream ended; canonical recovery required" });
        return;
      }
      const admitted = queue.pushBorrowed(result.value, ownChainUpdate);
      if (!admitted || result.value.kind === "Gap") {
        activity.setActive(false);
        return;
      }
    }
  } finally {
    attempt.abort();
    if (iterator.return)
      await withDeadline("source iterator close", operationTimeoutMilliseconds, undefined, () =>
        iterator.return!(),
      ).catch(() => undefined);
  }
}

async function nextWithTimeout(
  iterator: AsyncIterator<import("../model.js").ChainUpdate>,
  milliseconds: number,
  attempt: AbortController,
): Promise<IteratorResult<import("../model.js").ChainUpdate> | "timeout" | "cancelled"> {
  return new Promise((resolve, reject) => {
    let settled = false;
    let timedOut = false;
    const finish = (value: IteratorResult<import("../model.js").ChainUpdate> | "timeout" | "cancelled") => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      attempt.signal.removeEventListener("abort", onAbort);
      resolve(value);
    };
    const fail = (error: unknown) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      attempt.signal.removeEventListener("abort", onAbort);
      reject(error);
    };
    const onAbort = () => finish(timedOut ? "timeout" : "cancelled");
    const timer = setTimeout(() => {
      timedOut = true;
      finish("timeout");
      attempt.abort();
    }, milliseconds);
    attempt.signal.addEventListener("abort", onAbort, { once: true });
    if (attempt.signal.aborted) {
      onAbort();
      return;
    }
    iterator.next().then(finish, fail);
  });
}

export function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    let settled = false;
    function done() {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", done);
      clearTimeout(timer);
      resolve();
    }
    const timer = setTimeout(done, milliseconds);
    signal.addEventListener("abort", done, { once: true });
    if (signal.aborted) done();
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

function ownChainUpdate(update: ChainUpdate): ChainUpdate {
  switch (update.kind) {
    case "Head":
      return Object.freeze({ kind: "Head", head: ownBlock(update.head) });
    case "Log":
      return Object.freeze({ kind: "Log", log: ownLog(update.log) });
    case "Correction":
      return Object.freeze({ kind: "Correction", correction: ownCorrection(update.correction) });
    case "Reorg":
      return Object.freeze({ kind: "Reorg", oldHead: ownBlock(update.oldHead), newHead: ownBlock(update.newHead) });
    case "Gap":
      return Object.freeze({ kind: "Gap", cursor: update.cursor && ownCursor(update.cursor), reason: update.reason });
  }
}

function ownCorrection(correction: ChainCorrection): ChainCorrection {
  return Object.freeze({
    commonAncestor: ownBlock(correction.commonAncestor),
    oldTip: ownBlock(correction.oldTip),
    newTip: ownBlock(correction.newTip),
    oldBranch: Object.freeze(correction.oldBranch.map(ownBlock)),
    newBranch: Object.freeze(correction.newBranch.map(ownBlock)),
    replacementLogs: Object.freeze(correction.replacementLogs.map(ownLog)),
  });
}

function ownBlock(block: BlockRef): BlockRef {
  return Object.freeze({ cursor: ownCursor(block.cursor), parentHash: block.parentHash });
}

function ownLog(log: ContractLog): ContractLog {
  return Object.freeze({
    address: log.address,
    topics: Object.freeze([...log.topics]),
    data: log.data,
    removed: log.removed,
    cursor: ownCursor(log.cursor),
  });
}

function ownCursor(cursor: ChainCursor): ChainCursor {
  return Object.freeze({ ...cursor });
}
