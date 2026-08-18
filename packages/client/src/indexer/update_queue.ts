/** Bounded single-consumer handoff for normalized chain updates. */
import { chainUpdateRetainedBytes, type ChainCursor, type ChainUpdate } from "../model.js";
import { compareCursor, updateCursor } from "../source.js";
import { BoundedRingBuffer } from "./ring_buffer.js";

type UpdateWaiter = (value: DequeuedUpdate | undefined) => void;

/** One admitted update with immutable resource and reducer-order metadata. */
export interface DequeuedUpdate {
  readonly update: ChainUpdate;
  readonly sequence: number;
  readonly chargedBytes: number;
}

type QueuedUpdate = DequeuedUpdate;

export class BoundedUpdateQueue {
  private readonly values: BoundedRingBuffer<QueuedUpdate>;
  private readonly recoveryValues: BoundedRingBuffer<QueuedUpdate>;
  private readonly waiters = new Set<UpdateWaiter>();
  /** Bytes retained by both ordinary and recovery-staged entries. */
  private retainedBytes = 0;
  private acceptedSequenceValue = 0;
  private drainedThroughSequenceValue = 0;
  private ended = false;
  private overflowed = false;
  private recovering = false;

  constructor(
    private readonly capacity: number,
    private readonly byteCapacity: number,
  ) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) throw new Error("queue capacity must be positive");
    if (!Number.isSafeInteger(byteCapacity) || byteCapacity < 1024)
      throw new Error("queue byte capacity must be at least 1024");
    this.values = new BoundedRingBuffer(capacity);
    this.recoveryValues = new BoundedRingBuffer(capacity);
  }

  get closed(): boolean {
    return this.ended;
  }

  get acceptedSequence(): number {
    return this.acceptedSequenceValue;
  }

  get drainedThroughSequence(): number {
    return this.drainedThroughSequenceValue;
  }

  /** Enqueues one update or collapses every retained entry into one watermark gap. */
  push(update: ChainUpdate): void {
    this.pushCharged(update, chainUpdateRetainedBytes(update));
  }

  /** Measures borrowed source data before allocating an owned immutable copy. */
  pushBorrowed(update: ChainUpdate, own: (value: ChainUpdate) => ChainUpdate): boolean {
    return this.pushCharged(update, chainUpdateRetainedBytes(update), own);
  }

  private pushCharged(update: ChainUpdate, chargedBytes: number, own?: (value: ChainUpdate) => ChainUpdate): boolean {
    if (this.ended) return false;
    if (this.overflowed) {
      this.extendOverflowWatermark(update);
      return false;
    }
    let admitted = true;
    if (this.retainedCount >= this.capacity || chargedBytes > this.byteCapacity - this.retainedBytes) {
      this.collapseOverflow(update);
      admitted = false;
    } else {
      this.values.push({ update: own ? own(update) : update, chargedBytes, sequence: this.nextSequence() });
      this.retainedBytes += chargedBytes;
    }
    this.resolveWaiters();
    return admitted;
  }

  /** Transfers a failed in-flight update into queue-owned recovery staging. */
  beginRecovery(failed?: DequeuedUpdate): void {
    this.recovering = true;
    if (failed) {
      if (this.retainedCount >= this.capacity || failed.chargedBytes > this.byteCapacity - this.retainedBytes)
        this.collapseOverflow(failed.update);
      else {
        this.recoveryValues.push(failed);
        this.retainedBytes += failed.chargedBytes;
      }
    }
    this.overflowed = false;
  }

  /** Moves the current handoff into staging without releasing count or bytes. */
  stageQueuedForRecovery(): number {
    this.recovering = true;
    let entry = this.values.shift();
    while (entry) {
      if (!this.recoveryValues.push(entry)) throw new Error("recovery staging count invariant violated");
      entry = this.values.shift();
    }
    this.overflowed = false;
    return this.acceptedSequenceValue;
  }

  /** Returns bounded staged entries without transferring ownership. */
  recoveryEntries(): DequeuedUpdate[] {
    const entries = new Array<DequeuedUpdate>(this.recoveryValues.length);
    for (let index = 0; index < entries.length; index += 1) entries[index] = this.recoveryValues.peek(index)!;
    return entries;
  }

  /** Releases staged resources only after the candidate was installed. */
  completeRecovery(): number {
    let entry = this.recoveryValues.shift();
    while (entry) {
      this.retainedBytes -= entry.chargedBytes;
      entry = this.recoveryValues.shift();
    }
    this.recovering = false;
    this.drainedThroughSequenceValue = this.acceptedSequenceValue;
    return this.drainedThroughSequenceValue;
  }

  /** Releases recovery-only ownership after terminal shutdown. */
  abortRecovery(): void {
    this.recoveryValues.clear();
    this.values.clear();
    this.retainedBytes = 0;
    this.recovering = false;
    this.overflowed = false;
  }

  /** Drains a bootstrap handoff, releasing its queue budget. */
  drainAll(): ChainUpdate[] {
    return this.drainAllWithSequence().updates;
  }

  drainAllWithSequence(): { readonly updates: ChainUpdate[]; readonly throughSequence: number } {
    const queued = this.values.drainAll();
    for (const entry of queued) this.retainedBytes -= entry.chargedBytes;
    const throughSequence = this.acceptedSequenceValue;
    this.drainedThroughSequenceValue = throughSequence;
    this.overflowed = false;
    return { updates: queued.map(({ update }) => update), throughSequence };
  }

  close(): void {
    this.ended = true;
    this.resolveWaiters();
  }

  async next(signal: AbortSignal): Promise<ChainUpdate | undefined> {
    return (await this.nextWithSequence(signal))?.update;
  }

  async nextWithSequence(signal: AbortSignal): Promise<DequeuedUpdate | undefined> {
    const queued = this.take();
    if (queued) return queued;
    if (this.ended || signal.aborted) return undefined;
    return new Promise((resolve) => {
      let settled = false;
      const waiter = (next: DequeuedUpdate | undefined) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", onAbort);
        this.waiters.delete(waiter);
        resolve(next);
      };
      const onAbort = () => waiter(undefined);
      this.waiters.add(waiter);
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) onAbort();
    });
  }

  wakeConsumer(): void {
    this.waiters.values().next().value?.(undefined);
  }

  private get retainedCount(): number {
    return this.values.length + this.recoveryValues.length;
  }

  private take(): DequeuedUpdate | undefined {
    const queued = this.values.shift();
    if (!queued) return undefined;
    this.retainedBytes -= queued.chargedBytes;
    if (this.values.length === 0) this.overflowed = false;
    return queued;
  }

  private resolveWaiters(): void {
    while (this.waiters.size > 0 && (this.values.length > 0 || this.ended)) {
      const waiter = this.waiters.values().next().value;
      waiter?.(this.take());
    }
  }

  private collapseOverflow(incoming: ChainUpdate): void {
    const cursor = this.highestRetainedCursor(incoming);
    this.values.clear();
    this.recoveryValues.clear();
    this.retainedBytes = 0;
    const gap: ChainUpdate = {
      kind: "Gap",
      cursor,
      reason: "runtime queue count or byte budget exceeded; canonical recovery required",
    };
    const entry = {
      update: gap,
      chargedBytes: chainUpdateRetainedBytes(gap),
      sequence: this.nextSequence(),
    };
    (this.recovering ? this.recoveryValues : this.values).push(entry);
    this.retainedBytes = entry.chargedBytes;
    this.overflowed = !this.recovering;
  }

  private extendOverflowWatermark(incoming: ChainUpdate): void {
    const entry = this.values.peek();
    if (entry?.update.kind !== "Gap") return;
    const update: ChainUpdate = {
      ...entry.update,
      cursor: isUnknownGap(incoming)
        ? undefined
        : entry.update.cursor && highestCursor(entry.update.cursor, updateCursor(incoming)),
    };
    const chargedBytes = chainUpdateRetainedBytes(update);
    this.values.shift();
    this.values.push({ update, chargedBytes, sequence: entry.sequence });
    this.retainedBytes = chargedBytes;
  }

  private highestRetainedCursor(incoming: ChainUpdate): ChainCursor | undefined {
    if (isUnknownGap(incoming)) return undefined;
    let highest = updateCursor(incoming);
    for (const ring of [this.recoveryValues, this.values]) {
      for (let index = 0; index < ring.length; index += 1) {
        const update = ring.peek(index)!.update;
        if (isUnknownGap(update)) return undefined;
        highest = highestCursor(highest, updateCursor(update));
      }
    }
    return highest && { ...highest };
  }

  private nextSequence(): number {
    if (this.acceptedSequenceValue === Number.MAX_SAFE_INTEGER) throw new Error("queue admission sequence exhausted");
    this.acceptedSequenceValue += 1;
    return this.acceptedSequenceValue;
  }
}

function isUnknownGap(update: ChainUpdate): boolean {
  return update.kind === "Gap" && update.cursor === undefined;
}

function highestCursor(left: ChainCursor | undefined, right: ChainCursor | undefined): ChainCursor | undefined {
  if (!left) return right && { ...right };
  if (!right) return { ...left };
  if (left.chainId !== right.chainId) return undefined;
  return { ...(compareCursor(left, right) >= 0 ? left : right) };
}
