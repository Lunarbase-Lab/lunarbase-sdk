import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Commitment, Network, type Checkpoint } from "@lunarbase-lab/pmm-v2-client";
import { parseAddress } from "@lunarbase-lab/pmm-v2-math";
import { EvmRpcSource, JsonRpcHttpClient, RpcHttpBackend, type SocketEvent, type WebSocketLike } from "./index.js";

type RpcRequest = {
  readonly id: number;
  readonly method: string;
  readonly params: readonly unknown[];
};

const FILTER = {
  address: "0x0000000000000000000000000000000000000001",
  topics: [],
} as const;

const CORE = parseAddress(FILTER.address);
const BLOCK_HASH = `0x${"11".repeat(32)}`;

test("every reconnect revalidates the independent HTTP chain id", async () => {
  const firstSocket = new FakeSocket();
  const reconnectSocket = new FakeSocket();
  const sockets = [firstSocket, reconnectSocket];
  let connection = 0;
  let httpChecks = 0;
  const source = new EvmRpcSource(
    chainRpc(() => (httpChecks++ === 0 ? 97n : 98n)),
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

  const reconnect = source.subscribe(FILTER);
  const rejected = assert.rejects(reconnect, /HTTP RPC chain id mismatch: expected 97, got 98/);
  reconnectSocket.emit("open", {});
  emitHandshake(reconnectSocket, "0x61");
  await rejected;
  assert.equal(reconnectSocket.closeCalls, 1);
  assert.equal(httpChecks, 2);
  assert.equal(connection, 2);
});

test("backend coalesces chain verification and never checks per backfill page", async () => {
  const requests: RpcRequest[] = [];
  const client = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher((request) => {
      if (request.method === "eth_chainId") return "0x61";
      if (request.method === "eth_getBlockByNumber") return { number: "0x2a", hash: BLOCK_HASH, transactions: [] };
      if (request.method === "eth_getLogs") return [];
      throw new Error("unexpected " + request.method);
    }, requests),
  );
  const backend = new RpcHttpBackend(client, Network.Evm, 97n);

  const [head, logs] = await Promise.all([
    backend.canonicalHead(),
    backend.backfill({
      fromBlock: 10n,
      toBlock: 10_010n,
      filter: {
        address: CORE,
        topics: [],
      },
    }),
  ]);

  assert.equal(head.blockNumber, 42n);
  assert.deepEqual(logs, []);
  assert.equal(requests.filter((request) => request.method === "eth_chainId").length, 1);
  assert.equal(requests.filter((request) => request.method === "eth_getLogs").length, 11);
});

test("standalone canonical boundaries reject a foreign HTTP chain before data reads", async () => {
  const operations: readonly ((backend: RpcHttpBackend) => Promise<unknown>)[] = [
    (backend) => backend.canonicalHead(),
    (backend) =>
      backend.backfill({
        fromBlock: 42n,
        toBlock: 42n,
        filter: { address: CORE, topics: [] },
      }),
    (backend) => backend.validateCheckpoint(checkpointIdentity(97n, 97n)),
  ];

  for (const operation of operations) {
    const requests: RpcRequest[] = [];
    const backend = new RpcHttpBackend(
      new JsonRpcHttpClient(
        "https://rpc.example",
        rpcFetcher((request) => {
          if (request.method === "eth_chainId") return "0x62";
          throw new Error("unexpected " + request.method);
        }, requests),
      ),
      Network.Evm,
      97n,
    );

    await assert.rejects(operation(backend), /HTTP RPC chain id mismatch: expected 97, got 98/);
    assert.deepEqual(
      requests.map((request) => request.method),
      ["eth_chainId"],
    );
  }
});

test("checkpoint validation rejects foreign checkpoint identity without RPC", async () => {
  let calls = 0;
  const backend = new RpcHttpBackend(
    new JsonRpcHttpClient("https://rpc.example", (async () => {
      calls += 1;
      throw new Error("RPC must not be called");
    }) as typeof fetch),
    Network.Evm,
    97n,
  );

  assert.equal(await backend.validateCheckpoint(checkpointIdentity(98n, 97n)), false);
  assert.equal(await backend.validateCheckpoint(checkpointIdentity(97n, 98n)), false);
  assert.equal(calls, 0);
});

test("checkpoint validation binds both block and execution identity before contract reads", async () => {
  const implementation = parseAddress("0x00000000000000000000000000000000000000aa");
  const implementationWord = `0x${"0".repeat(24)}${implementation.slice(2)}` as `0x${string}`;
  const implementationCodeHash = `0x${"22".repeat(32)}` as `0x${string}`;
  const checkpoint = {
    ...checkpointIdentity(97n, 97n),
    core: CORE,
    expectedImplementation: implementation,
    expectedImplementationCodeHash: implementationCodeHash,
    cursor: {
      chainId: 97n,
      blockNumber: 42n,
      executionBlockNumber: 42n,
      blockHash: BLOCK_HASH,
      commitment: Commitment.Canonical,
    },
  } as Checkpoint;
  const canonicalCursor = { ...checkpoint.cursor };
  const cases = [
    { name: "exact identity", cursor: canonicalCursor, valid: true, identityReads: 2 },
    {
      name: "different block number",
      cursor: { ...canonicalCursor, blockNumber: 43n },
      valid: false,
      identityReads: 0,
    },
    {
      name: "different execution block number",
      cursor: { ...canonicalCursor, executionBlockNumber: 43n },
      valid: false,
      identityReads: 0,
    },
  ] as const;

  for (const scenario of cases) {
    let identityReads = 0;
    const rpc = {
      chainId: async () => 97n,
      blockCursor: async () => scenario.cursor,
      getStorageAtHash: async () => {
        identityReads += 1;
        return implementationWord;
      },
      runtimeCodeHashAtHash: async () => {
        identityReads += 1;
        return implementationCodeHash;
      },
    } as unknown as JsonRpcHttpClient;

    assert.equal(
      await new RpcHttpBackend(rpc, Network.Evm, 97n).validateCheckpoint(checkpoint),
      scenario.valid,
      scenario.name,
    );
    assert.equal(identityReads, scenario.identityReads, scenario.name);
  }
});

test("queued reconnect verification invalidates the backend cache synchronously", async () => {
  const requests: RpcRequest[] = [];
  let chainChecks = 0;
  const backend = new RpcHttpBackend(
    new JsonRpcHttpClient(
      "https://rpc.example",
      rpcFetcher((request) => {
        if (request.method === "eth_chainId") return chainChecks++ === 0 ? "0x61" : "0x62";
        if (request.method === "eth_getBlockByNumber") return { number: "0x2a", hash: BLOCK_HASH, transactions: [] };
        throw new Error("unexpected " + request.method);
      }, requests),
    ),
    Network.Evm,
    97n,
  );

  await backend.canonicalHead();
  const reconnect = backend.verifyChainId();
  const canonicalHead = backend.canonicalHead();

  await assert.rejects(reconnect, /HTTP RPC chain id mismatch/);
  await assert.rejects(canonicalHead, /HTTP RPC chain id mismatch/);
  assert.equal(requests.filter((request) => request.method === "eth_chainId").length, 3);
  assert.equal(requests.filter((request) => request.method === "eth_getBlockByNumber").length, 1);
});

function rpcFetcher(responder: (request: RpcRequest) => unknown, requests: RpcRequest[]): typeof fetch {
  return (async (_input: string | URL | Request, init?: RequestInit) => {
    const request = JSON.parse(String(init?.body)) as RpcRequest;
    requests.push(request);
    return new Response(JSON.stringify({ jsonrpc: "2.0", id: request.id, result: responder(request) }), {
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
}

function chainRpc(chainId: bigint | (() => bigint)): JsonRpcHttpClient {
  const readChainId = typeof chainId === "bigint" ? () => chainId : chainId;
  return new JsonRpcHttpClient(
    "http://unused",
    rpcFetcher((request) => {
      assert.equal(request.method, "eth_chainId");
      return hex(readChainId());
    }, []),
  );
}

function checkpointIdentity(chainId: bigint, cursorChainId: bigint): Checkpoint {
  return {
    chainId,
    cursor: {
      chainId: cursorChainId,
      blockNumber: 42n,
      executionBlockNumber: 42n,
      commitment: Commitment.Canonical,
    },
  } as unknown as Checkpoint;
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

function emitHandshake(socket: FakeSocket, chainId: string): void {
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 1, result: "logs" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 2, result: "heads" }) });
  socket.emit("message", { data: JSON.stringify({ jsonrpc: "2.0", id: 3, result: chainId }) });
}

function hex(value: bigint): `0x${string}` {
  return `0x${value.toString(16)}`;
}
