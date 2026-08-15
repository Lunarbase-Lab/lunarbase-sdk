/** Bounded single-consumer handoff for normalized chain updates. */
import { chainUpdateRetainedBytes, type ChainUpdate } from "../model.js";

export class BoundedUpdateQueue {
  /** Updates waiting for the ordered reducer. */
  private readonly values: ChainUpdate[] = [];
  /** Pending reducer reads. */
  private readonly waiters: Array<(value: ChainUpdate | undefined) => void> = [];
  /** Conservative bytes charged by updates currently in `values`. */
  private retainedBytes = 0;
  /** Whether shutdown has permanently closed the queue. */
  private ended = false;
  /** Whether one overflow gap already replaced the buffered updates. */
  private overflowed = false;

  /** Creates a queue with a strict in-memory update bound. */
  constructor(
    /** Maximum number of buffered normalized updates. */
    private readonly capacity: number,
    /** Maximum retained bytes across buffered normalized updates. */
    private readonly byteCapacity: number,
  ) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) throw new Error("queue capacity must be positive");
    if (!Number.isSafeInteger(byteCapacity) || byteCapacity < 1024)
      throw new Error("queue byte capacity must be at least 1024");
  }

  /** Reports whether producers must stop. */
  get closed(): boolean {
    return this.ended;
  }

  /** Enqueues one update or replaces an overflow with one fail-closed gap. */
  push(update: ChainUpdate): void {
    if (this.ended || this.overflowed) return;
    const bytes = chainUpdateRetainedBytes(update);
    if (this.values.length >= this.capacity || bytes > this.byteCapacity - this.retainedBytes) {
      this.values.length = 0;
      this.retainedBytes = 0;
      const gap: ChainUpdate = {
        kind: "Gap",
        reason: "runtime queue count or byte budget exceeded; canonical recovery required",
      };
      this.values.push(gap);
      this.retainedBytes = chainUpdateRetainedBytes(gap);
      this.overflowed = true;
    } else {
      this.values.push(update);
      this.retainedBytes += bytes;
    }
    this.resolveWaiters();
  }

  /** Drains the bootstrap/recovery handoff without cloning updates. */
  drainAll(): ChainUpdate[] {
    this.overflowed = false;
    this.retainedBytes = 0;
    return this.values.splice(0);
  }

  /** Closes the queue and releases a waiting reducer. */
  close(): void {
    this.ended = true;
    this.resolveWaiters();
  }

  /** Reads one update until data, cancellation, or close. */
  async next(signal: AbortSignal): Promise<ChainUpdate | undefined> {
    const value = this.values.shift();
    if (value) {
      this.retainedBytes -= chainUpdateRetainedBytes(value);
      if (this.values.length === 0) this.overflowed = false;
      return value;
    }
    if (this.ended || signal.aborted) return undefined;
    return new Promise((resolve) => {
      const waiter = (next: ChainUpdate | undefined) => {
        signal.removeEventListener("abort", onAbort);
        resolve(next);
      };
      const removeWaiter = () => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
      };
      const onAbort = () => {
        removeWaiter();
        resolve(undefined);
      };
      signal.addEventListener("abort", onAbort, { once: true });
      this.waiters.push(waiter);
    });
  }

  private resolveWaiters(): void {
    while (this.waiters.length > 0 && (this.values.length > 0 || this.ended)) {
      const value = this.values.shift();
      if (value) this.retainedBytes -= chainUpdateRetainedBytes(value);
      this.waiters.shift()?.(value);
    }
  }
}
