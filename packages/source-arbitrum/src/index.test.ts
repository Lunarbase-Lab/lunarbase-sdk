import { strict as assert } from "node:assert";
import { test } from "node:test";
import { RpcError } from "@lunarbase-lab/pmm-v2-source-evm";
import { ArbitrumNitroSource } from "./index.js";

type RpcRequest = {
  readonly id: number | string;
  readonly method: string;
  readonly params: readonly unknown[];
};

const ADDRESS = "0x0000000000000000000000000000000000000001";
const BACKFILL_REQUEST = {
  fromBlock: 42n,
  toBlock: 43n,
  filter: { address: ADDRESS, topics: [] },
} as const;

function rpcFetcher(
  responder: (request: RpcRequest) => unknown,
  requests: RpcRequest[],
  responseId: (request: RpcRequest) => unknown = (request) => request.id,
): typeof fetch {
  return (async (_input: string | URL | Request, init?: RequestInit) => {
    const request = JSON.parse(String(init?.body)) as RpcRequest;
    requests.push(request);
    const result = request.method === "eth_chainId" ? "0xa4b1" : responder(request);
    return new Response(JSON.stringify({ jsonrpc: "2.0", id: responseId(request), result }), {
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
}

function source(fetcher: typeof fetch): ArbitrumNitroSource {
  return new ArbitrumNitroSource(
    {
      httpRpcUrl: "https://rpc.example",
      realtimeUrl: "wss://rpc.example",
      chainId: 42161n,
    },
    {
      fetcher,
      webSocketFactory: () => {
        throw new Error("unused");
      },
    },
  );
}

function rawLog(blockNumber: bigint, logIndex: bigint): Record<string, unknown> {
  const byte = blockNumber === 42n ? "11" : "22";
  return {
    address: ADDRESS,
    topics: [],
    data: "0x",
    blockNumber: `0x${blockNumber.toString(16)}`,
    blockHash: `0x${byte.repeat(32)}`,
    transactionHash: `0x${"aa".repeat(32)}`,
    transactionIndex: "0x0",
    logIndex: `0x${logIndex.toString(16)}`,
    removed: false,
  };
}

test("Arbitrum uses standard logs subscriptions", () => {
  assert.equal(source((() => Promise.reject(new Error("unused"))) as typeof fetch).config.logsSubscription, "logs");
});

test("backfill maps Nitro l1BlockNumber once per distinct L2 block", async () => {
  const requests: RpcRequest[] = [];
  const logs = [rawLog(42n, 0n), rawLog(42n, 1n), rawLog(43n, 0n)];
  const result = await source(
    rpcFetcher((request) => {
      if (request.method === "eth_getLogs") return logs;
      if (request.method === "eth_getBlockByNumber") {
        const blockNumber = request.params[0];
        if (blockNumber === "0x2a")
          return { number: blockNumber, hash: `0x${"11".repeat(32)}`, l1BlockNumber: "0x7", transactions: [] };
        if (blockNumber === "0x2b")
          return { number: blockNumber, hash: `0x${"22".repeat(32)}`, l1BlockNumber: "0x8", transactions: [] };
      }
      throw new Error(`unexpected ${request.method}`);
    }, requests),
  ).backfill(BACKFILL_REQUEST);

  assert.deepEqual(
    result.map((log) => log.cursor.executionBlockNumber),
    [7n, 7n, 8n],
  );
  assert.deepEqual(
    requests
      .filter((request) => request.method === "eth_getBlockByNumber")
      .map((request) => request.params[0])
      .sort(),
    ["0x2a", "0x2b"],
  );
});

test("canonical head is pinned to explicit Nitro execution context", async () => {
  const requests: RpcRequest[] = [];
  const cursor = await source(
    rpcFetcher((request) => {
      if (request.method !== "eth_getBlockByNumber") throw new Error(`unexpected ${request.method}`);
      const blockTag = request.params[0];
      if (blockTag === "latest") return { number: "0x2a", hash: `0x${"11".repeat(32)}`, transactions: [] };
      if (blockTag === "0x2a")
        return {
          number: "0x2a",
          hash: `0x${"11".repeat(32)}`,
          l1BlockNumber: "0x7",
          transactions: [],
        };
      throw new Error(`unexpected block tag ${String(blockTag)}`);
    }, requests),
  ).canonicalHead();

  assert.equal(cursor.executionBlockNumber, 7n);
  assert.deepEqual(
    requests.filter((request) => request.method === "eth_getBlockByNumber").map((request) => request.params[0]),
    ["latest", "0x2a"],
  );
});

test("backfill rejects mismatched execution-context response id", async () => {
  const requests: RpcRequest[] = [];
  const operation = source(
    rpcFetcher(
      (request) => {
        if (request.method === "eth_getLogs") return [rawLog(42n, 0n)];
        if (request.method === "eth_getBlockByNumber")
          return {
            number: "0x2a",
            hash: `0x${"11".repeat(32)}`,
            l1BlockNumber: "0x7",
            transactions: [],
          };
        throw new Error(`unexpected ${request.method}`);
      },
      requests,
      (request) => (request.method === "eth_getBlockByNumber" ? "another-request" : request.id),
    ),
  ).backfill(BACKFILL_REQUEST);

  await assert.rejects(operation, (error: unknown) => error instanceof RpcError && error.code === "INVALID");
});

test("backfill rejects execution context from a different branch", async () => {
  const requests: RpcRequest[] = [];
  const operation = source(
    rpcFetcher((request) => {
      if (request.method === "eth_getLogs") return [rawLog(42n, 0n)];
      if (request.method === "eth_getBlockByNumber") {
        return {
          number: "0x2a",
          hash: `0x${"22".repeat(32)}`,
          l1BlockNumber: "0x7",
          transactions: [],
        };
      }
      throw new Error(`unexpected ${request.method}`);
    }, requests),
  ).backfill(BACKFILL_REQUEST);

  await assert.rejects(operation, (error: unknown) => error instanceof RpcError && error.code === "INVALID");
  assert.deepEqual(
    requests.map((request) => request.method),
    ["eth_chainId", "eth_getLogs", "eth_getBlockByNumber"],
  );
});

test("backfill rejects missing or conflicting log block hashes", async () => {
  const missingHash = rawLog(42n, 0n);
  delete missingHash.blockHash;
  const conflictingHash = { ...rawLog(42n, 1n), blockHash: `0x${"22".repeat(32)}` };
  for (const logs of [[missingHash], [rawLog(42n, 0n), conflictingHash]]) {
    const requests: RpcRequest[] = [];
    const operation = source(
      rpcFetcher((request) => {
        if (request.method === "eth_getLogs") return logs;
        throw new Error(`unexpected ${request.method}`);
      }, requests),
    ).backfill(BACKFILL_REQUEST);

    await assert.rejects(operation, (error: unknown) => error instanceof RpcError && error.code === "INVALID");
    assert.deepEqual(
      requests.map((request) => request.method),
      ["eth_chainId", "eth_getLogs"],
    );
  }
});

test("backfill rejects absent or malformed Nitro execution context", async () => {
  for (const l1BlockNumber of [undefined, "0x01"] as const) {
    const requests: RpcRequest[] = [];
    const operation = source(
      rpcFetcher((request) => {
        if (request.method === "eth_getLogs") return [rawLog(42n, 0n)];
        if (request.method === "eth_getBlockByNumber") {
          return {
            number: "0x2a",
            hash: `0x${"11".repeat(32)}`,
            ...(l1BlockNumber === undefined ? {} : { l1BlockNumber }),
            transactions: [],
          };
        }
        throw new Error(`unexpected ${request.method}`);
      }, requests),
    ).backfill(BACKFILL_REQUEST);

    await assert.rejects(operation, (error: unknown) => error instanceof RpcError && error.code === "INVALID");
    assert.deepEqual(
      requests.map((request) => request.method),
      ["eth_chainId", "eth_getLogs", "eth_getBlockByNumber"],
    );
  }
});
