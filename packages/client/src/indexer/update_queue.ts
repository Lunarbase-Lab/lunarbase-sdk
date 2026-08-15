/** Bounded single-consumer handoff for normalized chain updates. */
import { chainUpdateRetainedBytes, type ChainUpdate } from "../model.js";
import { BoundedRingBuffer } from "./ring_buffer.js";

type UpdateWaiter = (value: ChainUpdate | undefined) => void;

export class BoundedUpdateQueue {
  /** Updates waiting for the ordered reducer. */
  private readonly values: BoundedRingBuffer<ChainUpdate>;
  /** Pending reducer reads. */
  private readonly waiters = new Set<UpdateWaiter>();
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
    this.values = new BoundedRingBuffer(capacity);
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
      this.values.clear();
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
    return this.values.drainAll();
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
      let settled = false;
      const waiter = (next: ChainUpdate | undefined) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", onAbort);
        this.waiters.delete(waiter);
        resolve(next);
      };
      const onAbort = () => {
        waiter(undefined);
      };
      this.waiters.add(waiter);
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) onAbort();
    });
  }

  private resolveWaiters(): void {
    while (this.waiters.size > 0 && (this.values.length > 0 || this.ended)) {
      const value = this.values.shift();
      if (value) this.retainedBytes -= chainUpdateRetainedBytes(value);
      const waiter = this.waiters.values().next().value;
      if (waiter) waiter(value);
    }
  }
}
