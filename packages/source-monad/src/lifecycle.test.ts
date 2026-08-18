import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { ContractFilter } from "@lunarbase-lab/pmm-v2-client";
import type { JsonRpcHttpClient, SocketEvent, WebSocketLike } from "@lunarbase-lab/pmm-v2-source-evm";
import { MonadParserSource } from "./index.js";

const FILTER = {
  address: "0x0000000000000000000000000000000000000001",
  topics: [],
} as const satisfies ContractFilter;

test("Monad handshake timeout releases socket and AbortSignal listeners", async () => {
  const socket = new FakeSocket();
  const tracked = new TrackedAbortSignal();
  const source = new MonadParserSource(
    {} as JsonRpcHttpClient,
    "ws://unused",
    143n,
    "latest",
    { handshakeTimeoutMilliseconds: 5 },
    () => socket,
  );

  let failure: unknown;
  try {
    await source.subscribeExecution(FILTER, tracked.signal);
  } catch (error) {
    failure = error;
  }
  assert.ok(failure instanceof Error && failure.message.includes("timed out"));
  assert.equal(tracked.listeners, 0);
  assert.equal(socket.listenerCount, 0);
  assert.equal(socket.closeCalls, 1);
});

class FakeSocket implements WebSocketLike {
  readonly readyState = 0;
  closeCalls = 0;
  private readonly listeners = new Map<string, Set<(event: SocketEvent) => void>>();

  get listenerCount(): number {
    return [...this.listeners.values()].reduce((total, listeners) => total + listeners.size, 0);
  }

  send(_data: string): void {}

  close(): void {
    this.closeCalls += 1;
  }

  addEventListener(type: "open" | "message" | "error" | "close", listener: (event: SocketEvent) => void): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: "open" | "message" | "error" | "close", listener: (event: SocketEvent) => void): void {
    this.listeners.get(type)?.delete(listener);
  }
}

class TrackedAbortSignal {
  private readonly callbacks = new Set<EventListenerOrEventListenerObject>();
  readonly signal = this as unknown as AbortSignal;
  readonly aborted = false;

  get listeners(): number {
    return this.callbacks.size;
  }

  addEventListener(type: string, callback: EventListenerOrEventListenerObject | null): void {
    if (type === "abort" && callback) this.callbacks.add(callback);
  }

  removeEventListener(type: string, callback: EventListenerOrEventListenerObject | null): void {
    if (type === "abort" && callback) this.callbacks.delete(callback);
  }
}
