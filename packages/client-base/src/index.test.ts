import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Network, type ClientConnectConfig, type SocketEvent, type WebSocketLike } from "@lunarbase/client-core";
import { BaseDataSource } from "./index.js";

test("Base uses official pendingLogs plus newHeads transport", () => {
  const source = new BaseDataSource(config(), {
    fetcher: (() => Promise.reject(new Error("unused"))) as typeof fetch,
    webSocketFactory: () => {
      throw new Error("unused");
    },
  });
  assert.equal(source.config.logsSubscription, "pendingLogs");
  assert.equal(source.config.progressiveHeads, true);
});

test("Base treats changing same-height Flashblock heads as progress", async () => {
  const socket = new FakeSocket();
  const source = new BaseDataSource(config(), {
    fetcher: (() => Promise.reject(new Error("unused"))) as typeof fetch,
    webSocketFactory: () => socket,
  });
  const abort = new AbortController();
  const iterator = source.subscribe(config().filter, abort.signal)[Symbol.asyncIterator]();
  const first = iterator.next();
  socket.emit("open", {});
  socket.emit("message", {
    data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs" }),
  });
  socket.emit("message", {
    data: JSON.stringify({ jsonrpc: "2.0", id: 2, result: "heads" }),
  });
  socket.emit("message", {
    data: JSON.stringify(notification("0x2a", "11", "22")),
  });
  const firstUpdate = (await first).value;
  assert.equal(firstUpdate?.kind, "Head", firstUpdate?.kind === "Gap" ? firstUpdate.reason : undefined);

  const second = iterator.next();
  socket.emit("message", {
    data: JSON.stringify(notification("0x2a", "33", "22")),
  });
  const update = (await second).value;
  assert.equal(update?.kind, "Head");
  if (update?.kind === "Head") assert.equal(update.cursor.sourceSequence, 2n);
  abort.abort();
  await iterator.return?.();
});

class FakeSocket implements WebSocketLike {
  readonly readyState = 0;
  private readonly listeners = new Map<string, Set<(event: SocketEvent) => void>>();

  send(_data: string): void {}
  close(_code?: number, _reason?: string): void {}

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

function notification(number: string, hashByte: string, parentByte: string) {
  return {
    jsonrpc: "2.0",
    method: "eth_subscription",
    params: {
      subscription: "heads",
      result: {
        number,
        hash: `0x${hashByte.repeat(32)}`,
        parentHash: `0x${parentByte.repeat(32)}`,
      },
    },
  };
}

function config(): ClientConnectConfig {
  return {
    deployment: {
      network: Network.Base,
      chainId: 8453n,
      core: "0x0000000000000000000000000000000000000001",
      router: "0x0000000000000000000000000000000000000002",
      expectWhitelisted: true,
      deploymentBlock: 1n,
      expectedRuntimeCodeHash: `0x${"00".repeat(32)}`,
      contractCompatibilityVersion: "test",
      httpRpcUrl: "http://unused",
      realtimeSource: "ws://unused",
      explicitLaneAssets: [],
    },
    filter: {
      address: "0x0000000000000000000000000000000000000001",
      topics: [],
    },
    queueBound: 16,
    reconnectDelayMilliseconds: 10,
  };
}
