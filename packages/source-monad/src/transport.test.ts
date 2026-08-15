import { strict as assert } from "node:assert";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { JsonRpcHttpClient, type SocketEvent, type WebSocketLike } from "@lunarbase-lab/pmm-v2-source-evm";
import { type MonadParserConfig, MonadParserSource } from "./transport.js";

const FILTER = {
  address: "0x0000000000000000000000000000000000000001",
  topics: [],
} as const;

function parserSource(socket: FakeSocket, config: Partial<MonadParserConfig> = {}): MonadParserSource {
  return new MonadParserSource(
    new JsonRpcHttpClient("http://unused", (() => Promise.reject(new Error("unused"))) as typeof fetch),
    "ws://unused",
    143n,
    "latest",
    config,
    () => socket,
  );
}

function acknowledgement(id: unknown, result: string): string {
  return JSON.stringify({ jsonrpc: "2.0", id, result });
}

async function settleHandshake(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

async function expectRejection(operation: Promise<unknown>, pattern: RegExp): Promise<void> {
  try {
    await operation;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    assert.ok(pattern.test(message));
    return;
  }
  throw new Error("expected operation to reject");
}

async function closeEstablishedStream(stream: AsyncIterable<unknown>, socket: FakeSocket): Promise<void> {
  const iterator = stream[Symbol.asyncIterator]();
  const result = iterator.next();
  socket.emit("close", { reason: "test complete" });
  await result;
  assert.equal((await iterator.next()).done, true);
}

test("Monad parser fixture preserves nested head hash and fails closed on a retracted log", async () => {
  const frames = (
    await readFile(
      new URL("../../../fixtures/event-replay/monad-exec-events/parser-messages.jsonl", import.meta.url),
      "utf8",
    )
  )
    .trim()
    .split("\n");
  const socket = new FakeSocket();
  const source = parserSource(socket);
  const subscription = source.subscribeExecution(FILTER);
  socket.emit("open", {});
  socket.emit("message", { data: frames[0] });
  socket.emit("message", { data: frames[1] });
  const iterator = (await subscription)[Symbol.asyncIterator]();

  const headResult = iterator.next();
  socket.emit("message", { data: frames[2] });
  const head = (await headResult).value;
  assert.equal(head?.kind, "Head");
  if (head?.kind === "Head") {
    assert.equal(head.head.blockHash, `0x${"aa".repeat(32)}`);
  }

  const removed = JSON.parse(frames[3] ?? "{}") as Record<string, unknown>;
  const result = removed.result as Record<string, unknown>;
  result.removed = true;
  const gapResult = iterator.next();
  socket.emit("message", { data: JSON.stringify(removed) });
  const gap = (await gapResult).value;
  assert.equal(gap?.kind, "Gap");
  if (gap?.kind === "Gap") assert.ok(gap.reason.includes("retracted"));

  assert.equal((await iterator.next()).done, true);
  assert.equal(socket.closeCalls, 1);
});

test("Monad parser rejects non-boolean removed instead of applying the log", async () => {
  const frames = (
    await readFile(
      new URL("../../../fixtures/event-replay/monad-exec-events/parser-messages.jsonl", import.meta.url),
      "utf8",
    )
  )
    .trim()
    .split("\n");
  const socket = new FakeSocket();
  const subscription = parserSource(socket).subscribeExecution(FILTER);
  socket.emit("open", {});
  socket.emit("message", { data: frames[0] });
  socket.emit("message", { data: frames[1] });
  const iterator = (await subscription)[Symbol.asyncIterator]();

  const malformed = JSON.parse(frames[3] ?? "{}") as Record<string, unknown>;
  (malformed.result as Record<string, unknown>).removed = "true";
  const gapResult = iterator.next();
  socket.emit("message", { data: JSON.stringify(malformed) });
  const gap = (await gapResult).value;

  assert.equal(gap?.kind, "Gap");
  if (gap?.kind === "Gap") assert.ok(/removed is not boolean/.test(gap.reason));
  assert.equal((await iterator.next()).done, true);
  assert.equal(socket.closeCalls, 1);
});

test("Monad parser requires exact numeric acknowledgement ids", async () => {
  for (const id of ["1", true, 1.5, 3]) {
    const socket = new FakeSocket();
    const subscription = parserSource(socket).subscribeExecution(FILTER);
    const rejection = expectRejection(subscription, /unexpected numeric id/);
    socket.emit("open", {});
    socket.emit("message", { data: acknowledgement(id, "sub_logs") });
    await rejection;
    assert.equal(socket.closeCalls, 1);
  }
});

test("Monad parser accepts a stable duplicate acknowledgement and rejects a conflict", async () => {
  const stableSocket = new FakeSocket();
  const stable = parserSource(stableSocket).subscribeExecution(FILTER);
  stableSocket.emit("open", {});
  stableSocket.emit("message", { data: acknowledgement(1, "sub_logs") });
  stableSocket.emit("message", { data: acknowledgement(1, "sub_logs") });
  stableSocket.emit("message", { data: acknowledgement(2, "sub_all") });
  await closeEstablishedStream(await stable, stableSocket);
  assert.equal(stableSocket.closeCalls, 1);

  const conflictSocket = new FakeSocket();
  const conflict = parserSource(conflictSocket).subscribeExecution(FILTER);
  const rejection = expectRejection(conflict, /changed subscription id/);
  conflictSocket.emit("open", {});
  conflictSocket.emit("message", { data: acknowledgement(1, "sub_logs") });
  conflictSocket.emit("message", { data: acknowledgement(1, "other_logs") });
  await rejection;
  assert.equal(conflictSocket.closeCalls, 1);
});

test("Monad parser bounds notifications prefetched during handshake", async () => {
  const socket = new FakeSocket();
  const subscription = parserSource(socket, { queueCapacity: 2 }).subscribeExecution(FILTER);
  const rejection = expectRejection(subscription, /prefetch count or byte budget exceeded/);
  socket.emit("open", {});
  await settleHandshake();
  for (let index = 0; index < 3; index += 1) {
    socket.emit("message", {
      data: JSON.stringify({ jsonrpc: "2.0", method: "subscription", result: { type: "health", index } }),
    });
    await settleHandshake();
  }
  await rejection;
  assert.equal(socket.closeCalls, 1);
});

test("Monad parser uses one absolute opening and acknowledgement deadline", async () => {
  const socket = new FakeSocket();
  const subscription = parserSource(socket, {
    handshakeTimeoutMilliseconds: 40,
    queueCapacity: 128,
  }).subscribeExecution(FILTER);
  const rejection = expectRejection(subscription, /handshake timed out/);
  socket.emit("open", {});
  const interval = setInterval(() => {
    socket.emit("message", {
      data: JSON.stringify({ jsonrpc: "2.0", method: "subscription", result: { type: "health" } }),
    });
  }, 5);
  try {
    await rejection;
  } finally {
    clearInterval(interval);
  }
  assert.equal(socket.closeCalls, 1);
});

class FakeSocket implements WebSocketLike {
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
