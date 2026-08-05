/** Bounded single-consumer handoff for normalized chain updates. */
import type { ChainUpdate } from "../model.js";

export class BoundedUpdateQueue {
  /** Updates waiting for the ordered reducer. */
  private readonly values: ChainUpdate[] = [];
  /** Pending reducer reads. */
  private readonly waiters: Array<(value: ChainUpdate | undefined) => void> = [];
  /** Whether shutdown has permanently closed the queue. */
  private ended = false;
  /** Whether one overflow gap already replaced the buffered updates. */
  private overflowed = false;

  /** Creates a queue with a strict in-memory update bound. */
  constructor(
    /** Maximum number of buffered normalized updates. */
    private readonly capacity: number,
  ) {}

  /** Reports whether producers must stop. */
  get closed(): boolean {
    return this.ended;
  }

  /** Enqueues one update or replaces an overflow with one fail-closed gap. */
  push(update: ChainUpdate): void {
    if (this.ended || this.overflowed) return;
    if (this.values.length >= this.capacity) {
      this.values.length = 0;
      this.values.push({
        kind: "Gap",
        reason: "runtime queue overflow; canonical recovery required",
      });
      this.overflowed = true;
    } else {
      this.values.push(update);
    }
    this.resolveWaiters();
  }

  /** Drains the bootstrap/recovery handoff without cloning updates. */
  drainAll(): ChainUpdate[] {
    this.overflowed = false;
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
    while (this.waiters.length > 0 && (this.values.length > 0 || this.ended))
      this.waiters.shift()?.(this.values.shift());
  }
}
