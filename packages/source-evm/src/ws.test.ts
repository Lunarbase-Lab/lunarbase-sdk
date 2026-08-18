import { strict as assert } from "node:assert";
import { test } from "node:test";
import { CORE_EVENT_TOPICS, Network } from "@lunarbase-lab/pmm-v2-client";
import { EvmRpcSource, JsonRpcHttpClient, type SocketEvent, type WebSocketLike } from "./index.js";
import { parseHead } from "./ws/protocol.js";

const FILTER = {
  address: "0x0000000000000000000000000000000000000001",
  topics: [],
} as const;

test("Arbitrum heads require explicit execution context", () => {
  const head = {
    number: "0x2a",
    hash: `0x${"11".repeat(32)}`,
    parentHash: `0x${"22".repeat(32)}`,
  };
  assert.throws(() => parseHead(head, 42161n, true), /l1BlockNumber/);
  assert.throws(() => parseHead({ ...head, l1BlockNumber: "0x01" }, 42161n, true), /canonical hex quantity/);
  assert.equal(parseHead({ ...head, l1BlockNumber: "0x2a" }, 42161n, true).cursor.executionBlockNumber, 42n);
});

test("Arbitrum stream fails closed when a head has no execution context", async () => {
  const socket = new FakeSocket();
  const iterator = await arbitrumIterator(socket);
  const first = iterator.next();

  socket.emit("message", { data: JSON.stringify(headNotification(42n, "11", "22")) });

  const update = (await first).value;
  assert.equal(update?.kind, "Gap");
  if (update?.kind === "Gap") assert.ok(update.reason.includes("l1BlockNumber"));
  assert.equal((await iterator.next()).done, true);
});

test("standard logs wait from the successor and publish at the deadline without another head", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  try {
    await withMonotonicClock(async (setNow) => {
      const socket = new FakeSocket();
      const abort = new AbortController();
      const iterator = await standardIterator(socket, abort.signal);
      const first = iterator.next();
      let firstSettled = false;
      void first.then(() => {
        firstSettled = true;
      });

      setNow(0);
      socket.emit("message", { data: JSON.stringify(headNotification(42n, "11", "22", 7n)) });
      await flushMicrotasks();
      assert.equal(firstSettled, false);

      setNow(12_000);
      socket.emit("message", { data: JSON.stringify(headNotification(43n, "33", "11")) });
      await flushMicrotasks();
      assert.equal(firstSettled, false);

      setNow(13_000);
      socket.emit("message", { data: JSON.stringify(logNotification(42n, "11", 5n, 8n)) });
      await flushMicrotasks();
      assert.equal(firstSettled, false);

      setNow(13_999);
      context.mock.timers.tick(999);
      await flushMicrotasks();
      assert.equal(firstSettled, false);

      setNow(14_000);
      context.mock.timers.tick(1);

      const logUpdate = (await first).value;
      assert.equal(logUpdate?.kind, "Log");
      if (logUpdate?.kind === "Log") {
        assert.equal(logUpdate.log.cursor.blockNumber, 42n);
        assert.equal(logUpdate.log.cursor.executionBlockNumber, 7n);
        assert.equal(logUpdate.log.cursor.transactionIndex, 5n);
        assert.equal(logUpdate.log.cursor.logIndex, 8n);
      }

      const headUpdate = (await iterator.next()).value;
      assert.equal(headUpdate?.kind, "Head");
      if (headUpdate?.kind === "Head") {
        assert.equal(headUpdate.head.cursor.blockNumber, 42n);
        assert.equal(headUpdate.head.parentHash, `0x${"22".repeat(32)}`);
      }

      abort.abort();
      await iterator.return?.();
      assert.equal(socket.closeCalls, 1);
    });
  } finally {
    context.mock.timers.reset();
  }
});

test("standard exact duplicate head is idempotent", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  try {
    await withMonotonicClock(async (setNow) => {
      const socket = new FakeSocket();
      const abort = new AbortController();
      const iterator = await standardIterator(socket, abort.signal);
      const first = iterator.next();

      setNow(0);
      const head42 = headNotification(42n, "11", "22");
      socket.emit("message", { data: JSON.stringify(head42) });
      setNow(1);
      socket.emit("message", { data: JSON.stringify(head42) });
      await flushMicrotasks();

      setNow(12_000);
      socket.emit("message", { data: JSON.stringify(headNotification(43n, "33", "11")) });
      await flushMicrotasks();
      setNow(14_000);
      context.mock.timers.tick(2_000);

      const firstUpdate = (await first).value;
      assert.equal(firstUpdate?.kind, "Head");
      if (firstUpdate?.kind === "Head") {
        assert.equal(firstUpdate.head.cursor.blockNumber, 42n);
        assert.equal(firstUpdate.head.cursor.sourceSequence, 1n);
        assert.equal(firstUpdate.head.parentHash, `0x${"22".repeat(32)}`);
      }

      const second = iterator.next();
      setNow(24_000);
      socket.emit("message", { data: JSON.stringify(headNotification(44n, "44", "33")) });
      await flushMicrotasks();
      setNow(26_000);
      context.mock.timers.tick(2_000);

      const secondUpdate = (await second).value;
      assert.equal(secondUpdate?.kind, "Head");
      if (secondUpdate?.kind === "Head") {
        assert.equal(secondUpdate.head.cursor.blockNumber, 43n);
        assert.equal(secondUpdate.head.cursor.sourceSequence, 2n);
      }

      abort.abort();
      await iterator.return?.();
    });
  } finally {
    context.mock.timers.reset();
  }
});

test("competing heads preserve both block references in a reorg", async () => {
  const socket = new FakeSocket();
  const abort = new AbortController();
  const iterator = await standardIterator(socket, abort.signal);
  const next = iterator.next();

  socket.emit("message", { data: JSON.stringify(headNotification(42n, "11", "22")) });
  await flushMicrotasks();
  socket.emit("message", { data: JSON.stringify(headNotification(42n, "33", "44")) });

  const update = (await next).value;
  assert.equal(update?.kind, "Reorg");
  if (update?.kind === "Reorg") {
    assert.equal(update.oldHead.cursor.blockHash, `0x${"11".repeat(32)}`);
    assert.equal(update.oldHead.parentHash, `0x${"22".repeat(32)}`);
    assert.equal(update.newHead.cursor.blockHash, `0x${"33".repeat(32)}`);
    assert.equal(update.newHead.parentHash, `0x${"44".repeat(32)}`);
  }

  abort.abort();
  await iterator.return?.();
});

test("Base exact duplicate head is not published twice", async () => {
  const socket = new FakeSocket();
  const abort = new AbortController();
  const iterator = await pendingLogsIterator(socket, abort.signal);
  const head42 = headNotification(42n, "11", "22");

  const first = iterator.next();
  socket.emit("message", { data: JSON.stringify(head42) });
  const firstUpdate = (await first).value;
  assert.equal(firstUpdate?.kind, "Head");
  if (firstUpdate?.kind === "Head") assert.equal(firstUpdate.head.cursor.sourceSequence, 1n);

  const second = iterator.next();
  let secondSettled = false;
  void second.then(() => {
    secondSettled = true;
  });
  socket.emit("message", { data: JSON.stringify(head42) });
  await flushMicrotasks();
  assert.equal(secondSettled, false);

  socket.emit("message", { data: JSON.stringify(headNotification(43n, "33", "11")) });
  const secondUpdate = (await second).value;
  assert.equal(secondUpdate?.kind, "Head");
  if (secondUpdate?.kind === "Head") {
    assert.equal(secondUpdate.head.cursor.blockNumber, 43n);
    assert.equal(secondUpdate.head.cursor.sourceSequence, 2n);
  }

  abort.abort();
  await iterator.return?.();
});

test("standard logs fail closed when a log arrives at an emitted watermark", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  try {
    await withMonotonicClock(async (setNow) => {
      const socket = new FakeSocket();
      const iterator = await standardIterator(socket);
      const first = iterator.next();

      setNow(0);
      socket.emit("message", { data: JSON.stringify(headNotification(42n, "11", "22")) });
      await flushMicrotasks();
      setNow(12_000);
      socket.emit("message", { data: JSON.stringify(headNotification(43n, "33", "11")) });
      await flushMicrotasks();
      setNow(14_000);
      context.mock.timers.tick(2_000);

      const headUpdate = (await first).value;
      assert.equal(headUpdate?.kind, "Head");
      if (headUpdate?.kind === "Head") assert.equal(headUpdate.head.cursor.blockNumber, 42n);

      const late = iterator.next();
      socket.emit("message", { data: JSON.stringify(logNotification(42n, "11", 0n, 0n)) });
      const update = (await late).value;
      assert.equal(update?.kind, "Gap");
      if (update?.kind === "Gap") {
        assert.equal(update.cursor?.blockNumber, 42n);
        assert.ok(update.reason.includes("after its block watermark"));
      }

      const done = await iterator.next();
      assert.equal(done.done, true);
      assert.equal(socket.closeCalls, 1);
    });
  } finally {
    context.mock.timers.reset();
  }
});

test("standard logs discard an incomplete block when the socket disconnects", async () => {
  await withMonotonicClock(async (setNow) => {
    const socket = new FakeSocket();
    const iterator = await standardIterator(socket);
    const first = iterator.next();

    setNow(0);
    socket.emit("message", { data: JSON.stringify(headNotification(42n, "11", "22")) });
    await flushMicrotasks();
    socket.emit("message", { data: JSON.stringify(logNotification(42n, "11", 0n, 0n)) });
    await flushMicrotasks();
    socket.emit("close", {});

    const update = (await first).value;
    assert.equal(update?.kind, "Gap");
    if (update?.kind === "Gap") {
      assert.equal(update.cursor?.blockNumber, 42n);
      assert.ok(update.reason.includes("closed"));
    }

    const done = await iterator.next();
    assert.equal(done.done, true);
    assert.equal(socket.closeCalls, 1);
  });
});

test("live logs fail closed for a valid Core event from another contract", async () => {
  const socket = new FakeSocket();
  const iterator = await pendingLogsIterator(socket);
  const first = iterator.next();

  socket.emit("message", {
    data: JSON.stringify(
      logNotification(42n, "11", 0n, 0n, {
        address: "0x0000000000000000000000000000000000000002",
        topics: [CORE_EVENT_TOPICS.LaneAdded, "0x0000000000000000000000001111111111111111111111111111111111111111"],
      }),
    ),
  });

  const update = (await first).value;
  assert.equal(update?.kind, "Gap");
  if (update?.kind === "Gap") {
    assert.equal(update.cursor?.blockNumber, 42n);
    assert.ok(update.reason.includes("address mismatch"));
  }
  assert.equal((await iterator.next()).done, true);
  assert.equal(socket.closeCalls, 1);
});

test("handshake deadline is not extended by an unrelated frame", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  try {
    await withMonotonicClock(async (setNow) => {
      setNow(0);
      const socket = new FakeSocket();
      const handshake = pendingHandshake(socket);
      const rejected = assert.rejects(handshake, /handshake timed out/);
      await flushMicrotasks();

      setNow(9_999);
      context.mock.timers.tick(9_999);
      socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", method: "notice" }) });
      await flushMicrotasks();

      setNow(10_000);
      context.mock.timers.tick(1);
      await rejected;
      assert.equal(socket.closeCalls, 1);
    });
  } finally {
    context.mock.timers.reset();
  }
});

test("handshake prefetch fails closed at its configured bound", async () => {
  const socket = new FakeSocket();
  const handshake = pendingHandshake(socket, 2);
  const rejected = assert.rejects(handshake, /prefetch count or byte budget exceeded/);
  await flushMicrotasks();

  for (let index = 0; index < 3; index += 1) {
    socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", method: "notice", index }) });
    await flushMicrotasks();
  }

  await rejected;
  assert.equal(socket.closeCalls, 1);
});

test("handshake requires numeric request ids and stable subscription acknowledgements", async () => {
  const invalidIdSocket = new FakeSocket();
  const invalidId = pendingHandshake(invalidIdSocket);
  const invalidIdRejected = assert.rejects(invalidId, /response id must be an integer/);
  invalidIdSocket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: "1", result: "logs" }) });
  await invalidIdRejected;
  assert.equal(invalidIdSocket.closeCalls, 1);

  const conflictSocket = new FakeSocket();
  const conflict = pendingHandshake(conflictSocket);
  const conflictRejected = assert.rejects(conflict, /acknowledgement changed/);
  conflictSocket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs-a" }) });
  await flushMicrotasks();
  conflictSocket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs-b" }) });
  await conflictRejected;
  assert.equal(conflictSocket.closeCalls, 1);
});

test("every WebSocket subscription validates the configured chain id", async () => {
  const firstSocket = new FakeSocket();
  const reconnectSocket = new FakeSocket();
  const sockets = [firstSocket, reconnectSocket];
  let connection = 0;
  const source = new EvmRpcSource(
    chainRpc(97n),
    "ws://unused",
    Network.Evm,
    97n,
    "latest",
    {},
    () => sockets[connection++]!,
  );

  const firstAbort = new AbortController();
  const first = source.subscribe(FILTER, firstAbort.signal);
  firstSocket.emit("open", {});
  emitHandshake(firstSocket, "0x61");
  const firstIterator = (await first)[Symbol.asyncIterator]();
  const stopped = firstIterator.next();
  firstAbort.abort();
  assert.equal((await stopped).done, true);
  assert.equal(firstSocket.closeCalls, 1);

  const reconnect = source.subscribe(FILTER);
  const rejected = assert.rejects(reconnect, /chain id mismatch: expected 97, got 98/);
  reconnectSocket.emit("open", {});
  emitHandshake(reconnectSocket, "0x62");
  await rejected;
  assert.equal(reconnectSocket.closeCalls, 1);
  assert.equal(connection, 2);
});

function chainRpc(chainId: bigint | (() => bigint)): JsonRpcHttpClient {
  const readChainId = typeof chainId === "bigint" ? () => chainId : chainId;
  const fetcher = (async (_input: string | URL | Request, init?: RequestInit) => {
    const request = JSON.parse(String(init?.body)) as { readonly id: number; readonly method: string };
    assert.equal(request.method, "eth_chainId");
    return new Response(JSON.stringify({ jsonrpc: "2.0", id: request.id, result: hex(readChainId()) }), {
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
  return new JsonRpcHttpClient("http://unused", fetcher);
}

function emitHandshake(socket: FakeSocket, chainId: string): void {
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 2, result: "heads" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 3, result: chainId }) });
}

function pendingHandshake(
  socket: FakeSocket,
  queueCapacity = 4,
): Promise<AsyncIterable<import("@lunarbase-lab/pmm-v2-client").ChainUpdate>> {
  const source = new EvmRpcSource(
    chainRpc(97n),
    "ws://unused",
    Network.Evm,
    97n,
    "latest",
    { queueCapacity },
    () => socket,
  );
  const handshake = source.subscribe(FILTER);
  socket.emit("open", {});
  return handshake;
}

async function pendingLogsIterator(
  socket: FakeSocket,
  signal?: AbortSignal,
): Promise<AsyncIterator<import("@lunarbase-lab/pmm-v2-client").ChainUpdate>> {
  const source = new EvmRpcSource(
    chainRpc(8453n),
    "ws://unused",
    Network.Base,
    8453n,
    "latest",
    { logsSubscription: "pendingLogs", progressiveHeads: true },
    () => socket,
  );
  const stream = source.subscribe(FILTER, signal);
  socket.emit("open", {});
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 2, result: "heads" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 3, result: "0x2105" }) });
  return (await stream)[Symbol.asyncIterator]();
}

async function arbitrumIterator(
  socket: FakeSocket,
): Promise<AsyncIterator<import("@lunarbase-lab/pmm-v2-client").ChainUpdate>> {
  const source = new EvmRpcSource(
    chainRpc(42161n),
    "ws://unused",
    Network.Arbitrum,
    42161n,
    "latest",
    {},
    () => socket,
  );
  const stream = source.subscribe(FILTER);
  socket.emit("open", {});
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 2, result: "heads" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 3, result: "0xa4b1" }) });
  return (await stream)[Symbol.asyncIterator]();
}

async function standardIterator(
  socket: FakeSocket,
  signal?: AbortSignal,
): Promise<AsyncIterator<import("@lunarbase-lab/pmm-v2-client").ChainUpdate>> {
  const source = new EvmRpcSource(chainRpc(97n), "ws://unused", Network.Evm, 97n, "latest", {}, () => socket);
  const stream = source.subscribe(FILTER, signal);
  socket.emit("open", {});
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 2, result: "heads" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 3, result: "0x61" }) });
  return (await stream)[Symbol.asyncIterator]();
}

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

function headNotification(blockNumber: bigint, hashByte: string, parentByte: string, executionBlockNumber?: bigint) {
  return {
    jsonrpc: "2.0",
    method: "eth_subscription",
    params: {
      subscription: "heads",
      result: {
        number: hex(blockNumber),
        hash: `0x${hashByte.repeat(32)}`,
        parentHash: `0x${parentByte.repeat(32)}`,
        ...(executionBlockNumber === undefined ? {} : { l1BlockNumber: hex(executionBlockNumber) }),
      },
    },
  };
}

function logNotification(
  blockNumber: bigint,
  hashByte: string,
  transactionIndex: bigint,
  logIndex: bigint,
  overrides: Readonly<Record<string, unknown>> = {},
) {
  return {
    jsonrpc: "2.0",
    method: "eth_subscription",
    params: {
      subscription: "logs",
      result: {
        address: FILTER.address,
        topics: [],
        data: "0x",
        blockHash: `0x${hashByte.repeat(32)}`,
        blockNumber: hex(blockNumber),
        transactionHash: `0x${"aa".repeat(32)}`,
        transactionIndex: hex(transactionIndex),
        logIndex: hex(logIndex),
        removed: false,
        ...overrides,
      },
    },
  };
}

function hex(value: bigint): `0x${string}` {
  return `0x${value.toString(16)}`;
}

async function withMonotonicClock(callback: (setNow: (value: number) => void) => Promise<void>): Promise<void> {
  let now = 0;
  const performanceObject = globalThis.performance;
  const ownDescriptor = Object.getOwnPropertyDescriptor(performanceObject, "now");
  Object.defineProperty(performanceObject, "now", {
    configurable: true,
    value: () => now,
  });
  try {
    await callback((value) => {
      now = value;
    });
  } finally {
    if (ownDescriptor) Object.defineProperty(performanceObject, "now", ownDescriptor);
    else Reflect.deleteProperty(performanceObject, "now");
  }
}

async function flushMicrotasks(): Promise<void> {
  for (let count = 0; count < 8; count += 1) await Promise.resolve();
}
