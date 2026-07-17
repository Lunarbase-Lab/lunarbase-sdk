import { strict as assert } from "node:assert";
import { test } from "node:test";
import {
  BaseFlashblocksBackend,
  JsonRpcHttpClient,
  MonadExecutionEventsSource,
  MonadSidecarBackend,
  Network,
  type SocketEvent,
  type WebSocketLike,
} from "../index.js";

const address = (last: string) => `0x${last.padStart(40, "0")}`;

test("Base Flashblocks backend consumes documented subscriptions", async () => {
  const core = address("1");
  const blockHash = `0x${"22".repeat(32)}`;
  class ScriptedSocket implements WebSocketLike {
    readonly readyState = 1;
    private readonly listeners = new Map<string, Set<(event: SocketEvent) => void>>();
    send(data: string): void {
      const request = JSON.parse(data) as { id?: number; method?: string };
      if (request.method !== "eth_subscribe") return;
      if (request.id === 1) this.emit({ data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs-sub" }) });
      if (request.id === 2) {
        this.emit({ data: JSON.stringify({ jsonrpc: "2.0", id: 2, result: "flash-sub" }) });
        this.emit({
          data: JSON.stringify({
            jsonrpc: "2.0",
            method: "eth_subscription",
            params: {
              subscription: "flash-sub",
              result: {
                payload_id: "0x11",
                index: "0x0",
                base: { block_number: "0x2a" },
                diff: { block_hash: blockHash },
              },
            },
          }),
        });
        this.emit({
          data: JSON.stringify({
            jsonrpc: "2.0",
            method: "eth_subscription",
            params: {
              subscription: "logs-sub",
              result: {
                address: core,
                topics: ["0x1"],
                data: "0x",
                blockNumber: "0x2a",
                blockHash,
                transactionIndex: "0x0",
                logIndex: "0x0",
                removed: false,
              },
            },
          }),
        });
      }
    }
    close(): void {
      this.emit({ reason: "closed" });
    }
    addEventListener(type: string, listener: (event: SocketEvent) => void): void {
      const listeners = this.listeners.get(type) ?? new Set();
      listeners.add(listener);
      this.listeners.set(type, listeners);
    }
    removeEventListener(type: string, listener: (event: SocketEvent) => void): void {
      this.listeners.get(type)?.delete(listener);
    }
    private emit(event: SocketEvent): void {
      for (const listener of this.listeners.get(event.reason ? "close" : "message") ?? []) listener(event);
    }
  }
  const socket = new ScriptedSocket();
  const backend = new BaseFlashblocksBackend(
    new JsonRpcHttpClient("http://unused"),
    "ws://unused",
    Network.Base,
    8453n,
    "finalized",
    {},
    () => socket,
  );
  const iterator = backend.subscribe(Network.Base, { address: core, topics: [] })[Symbol.asyncIterator]();
  const head = await iterator.next();
  assert.equal(head.value?.kind, "Head");
  const log = await iterator.next();
  assert.equal(log.value?.kind, "Log");
  if (log.value?.kind === "Log") assert.equal(log.value.log.cursor.blockNumber, 42n);
  await iterator.return?.();
});

test("Monad sidecar backend preserves sparse seqnos and turns parser gaps into Gap", async () => {
  const core = address("1");
  const blockHash = `0x${"aa".repeat(32)}`;
  class ScriptedSocket implements WebSocketLike {
    readonly readyState = 1;
    private readonly listeners = new Map<string, Set<(event: SocketEvent) => void>>();
    send(data: string): void {
      const request = JSON.parse(data) as { id?: number; method?: string };
      if (request.method !== "subscribe") return;
      if (request.id === 1) this.emit({ data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs-sub" }) });
      if (request.id === 2) {
        this.emit({ data: JSON.stringify({ jsonrpc: "2.0", id: 2, result: "all-sub" }) });
        this.emit({
          data: JSON.stringify({
            method: "subscription",
            result: {
              type: "newHead",
              commitment: "verified",
              seqno: 1000,
              blockNumber: 700,
              header: { blockTag: { id: blockHash } },
            },
            params: { subscription: "all-sub" },
          }),
        });
        this.emit({
          data: JSON.stringify({
            method: "subscription",
            result: {
              type: "log",
              kind: "event",
              seqno: 1004,
              blockNumber: 700,
              transactionIndex: 0,
              logIndex: 0,
              address: core,
              topics: ["0x1"],
              data: "0x",
              blockHash,
            },
            params: { subscription: "logs-sub" },
          }),
        });
        this.emit({ data: JSON.stringify({ method: "subscriptionGap", params: { skipped: 3 } }) });
      }
    }
    close(): void {
      this.emit({ reason: "closed" });
    }
    addEventListener(type: string, listener: (event: SocketEvent) => void): void {
      const listeners = this.listeners.get(type) ?? new Set();
      listeners.add(listener);
      this.listeners.set(type, listeners);
    }
    removeEventListener(type: string, listener: (event: SocketEvent) => void): void {
      this.listeners.get(type)?.delete(listener);
    }
    private emit(event: SocketEvent): void {
      for (const listener of this.listeners.get(event.reason ? "close" : "message") ?? []) listener(event);
    }
  }
  const backend = new MonadSidecarBackend(
    new JsonRpcHttpClient("http://unused"),
    "ws://unused",
    Network.Monad,
    143n,
    "finalized",
    {},
    () => new ScriptedSocket(),
  );
  const iterator = backend.subscribe(Network.Monad, { address: core, topics: [] })[Symbol.asyncIterator]();
  const head = await iterator.next();
  assert.equal(head.value?.kind, "Head");
  const log = await iterator.next();
  assert.equal(log.value?.kind, "Log");
  if (log.value?.kind === "Log") assert.equal(log.value.log.cursor.sourceSequence, 1004n);
  const gap = await iterator.next();
  assert.equal(gap.value?.kind, "Gap");
  await iterator.return?.();

  const source = new MonadExecutionEventsSource(backend);
  const sourceIterator = source.subscribe({ address: core, topics: [] })[Symbol.asyncIterator]();
  assert.equal((await sourceIterator.next()).value?.kind, "Head");
  const sourceLog = await sourceIterator.next();
  assert.equal(sourceLog.value?.kind, "Log");
  if (sourceLog.value?.kind === "Log") assert.equal(sourceLog.value.log.cursor.sourceSequence, 1004n);
  assert.equal((await sourceIterator.next()).value?.kind, "Gap");
  await sourceIterator.return?.();
});
