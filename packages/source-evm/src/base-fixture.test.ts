import { strict as assert } from "node:assert";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { createBaseFlashblocksSource, type SocketEvent, type WebSocketLike } from "./index.js";

const ZERO_BLOCK_HASH = `0x${"00".repeat(32)}`;

test("Base fixture accepts positioned pendingLogs with a zero block hash", async () => {
  const frames = (
    await readFile(new URL("../../../fixtures/event-replay/base-flashblocks/ws-frames.jsonl", import.meta.url), "utf8")
  )
    .trim()
    .split("\n");

  const socket = new FixtureSocket();
  const source = createBaseFlashblocksSource(
    {
      httpRpcUrl: "http://unused",
      realtimeUrl: "ws://unused",
      chainId: 8453n,
    },
    {
      fetcher: (() => Promise.reject(new Error("unused"))) as typeof fetch,
      webSocketFactory: () => socket,
    },
  );
  const abort = new AbortController();
  const stream = source.subscribe(
    {
      address: "0x0000000000000000000000000000000000000001",
      topics: [],
    },
    abort.signal,
  );

  socket.emit("open", {});
  for (const frame of frames) socket.emit("message", { data: frame });

  const iterator = (await stream)[Symbol.asyncIterator]();
  const updates = [];
  for (let index = 0; index < 3; index += 1) {
    const update = (await iterator.next()).value;
    assert.ok(update);
    updates.push(update);
  }

  const pending = updates.find((update) => update.kind === "Log");
  assert.equal(pending?.kind, "Log");
  if (pending?.kind === "Log") {
    assert.equal(pending.log.cursor.blockNumber, 42n);
    assert.equal(pending.log.cursor.blockHash, ZERO_BLOCK_HASH);
    assert.equal(pending.log.cursor.transactionIndex, 3n);
    assert.equal(pending.log.cursor.logIndex, 7n);
  }
  assert.equal(updates.filter((update) => update.kind === "Head").length, 2);
  assert.equal(
    updates.some((update) => update.kind === "Gap"),
    false,
  );

  abort.abort();
  await iterator.return?.();
  assert.equal(socket.closeCalls, 1);
});

class FixtureSocket implements WebSocketLike {
  readonly readyState = 0;
  closeCalls = 0;
  private readonly listeners = new Map<string, Set<(event: SocketEvent) => void>>();

  send(_data: string): void {}

  close(_code?: number, _reason?: string): void {
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

  emit(type: string, event: SocketEvent): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}
