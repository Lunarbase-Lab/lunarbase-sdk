/** Bounded asynchronous lifecycle observer fan-out. */
import type { IndexerLifecycleEvent, IndexerLifecycleListener } from "../model.js";
import { BoundedRingBuffer } from "./ring_buffer.js";

const NOTICE_CAPACITY = 64;
const NOTICE_BYTE_CAPACITY = 64 * 1024;
const MAX_NOTICES_PER_MICROTASK = 16;
const MAX_REASON_CHARACTERS = 1_024;
const TEXT_ENCODER = new TextEncoder();

/** Keeps user callbacks outside reducer/correction execution. */
export class LifecyclePublisher {
  private readonly listeners = new Set<IndexerLifecycleListener>();
  private readonly notices = new BoundedRingBuffer<IndexerLifecycleEvent>(NOTICE_CAPACITY);
  private retainedBytesValue = 0;
  private scheduled = false;

  /** Registers an observer and returns an idempotent unsubscribe function. */
  subscribe(listener: IndexerLifecycleListener): () => void {
    this.listeners.add(listener);
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.listeners.delete(listener);
    };
  }

  /** Publishes one compact notice without invoking untrusted code inline. */
  publish(event: IndexerLifecycleEvent): void {
    if (this.listeners.size === 0) return;
    this.enqueue(event);
    if (this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => this.drain());
  }

  /** Retains one transactional notice without scheduling observer callbacks. */
  stage(event: IndexerLifecycleEvent): void {
    this.enqueue(event);
  }

  /** Moves a bounded successful transaction into the live async publisher. */
  flushInto(target: LifecyclePublisher): void {
    let event = this.notices.shift();
    while (event) {
      this.retainedBytesValue -= retainedBytes(event);
      target.publish(event);
      event = this.notices.shift();
    }
  }

  private enqueue(event: IndexerLifecycleEvent): void {
    const notice = normalize(event);
    const bytes = retainedBytes(notice);
    if (this.notices.length >= NOTICE_CAPACITY || bytes > NOTICE_BYTE_CAPACITY - this.retainedBytesValue) {
      this.notices.clear();
      this.retainedBytesValue = 0;
      const overflow: IndexerLifecycleEvent = {
        kind: "ObserverGap",
        reason: "lifecycle observer queue overflowed; quote continuity is unaffected",
      };
      this.notices.push(overflow);
      this.retainedBytesValue = retainedBytes(overflow);
    } else {
      this.notices.push(notice);
      this.retainedBytesValue += bytes;
    }
  }

  private drain(): void {
    let processed = 0;
    while (processed < MAX_NOTICES_PER_MICROTASK) {
      const event = this.notices.shift();
      if (!event) {
        this.scheduled = false;
        return;
      }
      this.retainedBytesValue -= retainedBytes(event);
      processed += 1;
      for (const listener of this.listeners) {
        try {
          listener(event);
        } catch {
          // Observer failures never affect source continuity or quote readiness.
        }
      }
    }
    // Yield to timers and I/O before delivering the next bounded observer batch.
    setTimeout(() => this.drain(), 0);
  }
}

function normalize(event: IndexerLifecycleEvent): IndexerLifecycleEvent {
  if (event.kind === "CorrectionApplied") return event;
  return { ...event, reason: event.reason.slice(0, MAX_REASON_CHARACTERS) };
}

function retainedBytes(event: IndexerLifecycleEvent): number {
  return event.kind === "CorrectionApplied" ? 1_536 : 192 + TEXT_ENCODER.encode(event.reason).byteLength;
}
