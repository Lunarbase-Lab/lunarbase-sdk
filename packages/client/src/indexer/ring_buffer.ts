/** Fixed-capacity FIFO with constant-time enqueue and dequeue. */
export class BoundedRingBuffer<T> {
  private readonly values: Array<T | undefined>;
  private head = 0;
  private size = 0;

  constructor(readonly capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0)
      throw new Error("ring buffer capacity must be a positive safe integer");
    this.values = new Array<T | undefined>(capacity);
  }

  /** Number of retained entries. */
  get length(): number {
    return this.size;
  }

  /** Adds one entry, returning false instead of growing beyond capacity. */
  push(value: T): boolean {
    if (this.size === this.capacity) return false;
    this.values[(this.head + this.size) % this.capacity] = value;
    this.size += 1;
    return true;
  }

  /** Reads one retained entry without removing it. */
  peek(offset = 0): T | undefined {
    if (!Number.isSafeInteger(offset) || offset < 0 || offset >= this.size) return undefined;
    return this.values[(this.head + offset) % this.capacity];
  }

  /** Removes the oldest entry without shifting the backing array. */
  shift(): T | undefined {
    if (this.size === 0) return undefined;
    const value = this.values[this.head];
    this.values[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this.size -= 1;
    if (this.size === 0) this.head = 0;
    return value;
  }

  /** Releases every retained entry and its backing references. */
  clear(): void {
    if (this.size > 0) this.values.fill(undefined);
    this.head = 0;
    this.size = 0;
  }

  /** Moves all entries into one ordered array. */
  drainAll(): T[] {
    const drained = new Array<T>(this.size);
    for (let index = 0; index < drained.length; index += 1) drained[index] = this.shift()!;
    return drained;
  }
}
