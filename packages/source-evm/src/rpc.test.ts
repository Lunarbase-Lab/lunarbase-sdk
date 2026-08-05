import { strict as assert } from "node:assert";
import { test } from "node:test";
import {
  Commitment,
  MATH_COMPATIBILITY_VERSION,
  Network,
  QuoteReducer,
  quoteCriticalTopics,
  type ChainCursor,
  type DeploymentConfig,
} from "@lunarbase-lab/pmm-v2-client";
import { parseAddress, type Address } from "@lunarbase-lab/pmm-v2-math";
import { JsonRpcHttpClient, RpcError, RpcSnapshotProvider, keccak256Hex, parseRpcLog } from "./rpc.js";
import { EvmRpcSource } from "./ws.js";

type RpcRequest = {
  readonly id: number;
  readonly method: string;
  readonly params: readonly unknown[];
};

function rpcFetcher(responder: (request: RpcRequest) => unknown, requests: RpcRequest[]): typeof fetch {
  return (async (_input: string | URL | Request, init?: RequestInit) => {
    const request = JSON.parse(String(init?.body)) as RpcRequest;
    requests.push(request);
    return new Response(JSON.stringify({ jsonrpc: "2.0", id: request.id, result: responder(request) }), {
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
}

test("read-only viem client performs no implicit chain or retry calls", async () => {
  const requests: RpcRequest[] = [];
  const client = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher((request) => {
      if (request.method === "eth_chainId") return "0x2105";
      if (request.method === "eth_getCode") return "0x6000";
      throw new Error(`unexpected ${request.method}`);
    }, requests),
  );

  assert.equal(requests.length, 0, "construction must not touch the network");
  assert.equal(await client.chainId(), 8453n);
  assert.deepEqual(
    requests.map((request) => request.method),
    ["eth_chainId"],
  );

  await client.getCode(parseAddress("0x0000000000000000000000000000000000000001"), "0x2a");
  assert.deepEqual(
    requests.map((request) => request.method),
    ["eth_chainId", "eth_getCode"],
  );
  assert.equal((requests[1]?.params[1] as string | undefined) ?? "", "0x2a");
});

test("backfill sends one topic0 OR filter and no auxiliary requests", async () => {
  const requests: RpcRequest[] = [];
  const client = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher((request) => {
      assert.equal(request.method, "eth_getLogs");
      return [];
    }, requests),
  );
  const topics = quoteCriticalTopics();

  const logs = await client.getLogs(
    {
      fromBlock: 10n,
      toBlock: 20n,
      filter: {
        address: parseAddress("0x0000000000000000000000000000000000000001"),
        topics,
      },
    },
    8453n,
    Commitment.Canonical,
  );

  assert.deepEqual(logs, []);
  assert.equal(requests.length, 1);
  const filter = requests[0]?.params[0] as { topics?: readonly (readonly string[])[] };
  assert.deepEqual(filter.topics, [topics]);
});

test("backfill with empty topics requests and returns every Core log", async () => {
  const requests: RpcRequest[] = [];
  const address = parseAddress("0x0000000000000000000000000000000000000001");
  const blockHash = `0x${"11".repeat(32)}`;
  const client = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher(() => {
      return [
        {
          address,
          topics: [],
          data: "0x",
          blockNumber: "0x2a",
          blockHash,
          transactionHash: `0x${"22".repeat(32)}`,
          transactionIndex: "0x0",
          logIndex: "0x0",
          removed: false,
        },
      ];
    }, requests),
  );

  const logs = await client.getLogs(
    {
      fromBlock: 42n,
      toBlock: 42n,
      filter: { address, topics: [] },
    },
    8453n,
    Commitment.Canonical,
  );

  assert.equal(logs.length, 1);
  assert.equal(logs[0]?.cursor.blockNumber, 42n);
  assert.deepEqual((requests[0]?.params[0] as { topics?: unknown }).topics, []);
});

test("raw RPC log parsing validates and canonicalizes every field", () => {
  const log = parseRpcLog(
    rawRpcLog({
      address: "0x00000000000000000000000000000000000000AA",
      topics: ["0x" + "AB".repeat(32)],
      data: "0xABCD",
    }),
    8453n,
    Commitment.Realtime,
  );

  assert.equal(log.address, "0x00000000000000000000000000000000000000aa");
  assert.deepEqual(log.topics, ["0x" + "ab".repeat(32)]);
  assert.equal(log.data, "0xabcd");
  assert.equal(log.cursor.blockNumber, 42n);
  assert.equal(log.cursor.transactionIndex, 2n);
  assert.equal(log.cursor.logIndex, 3n);
});

test("raw RPC log parsing rejects malformed fields and unsafe indices", () => {
  const malformed: readonly [string, Record<string, unknown>][] = [
    ["address", { address: "0x01" }],
    ["topics", { topics: "0x" }],
    ["topic", { topics: ["0x" + "gg".repeat(32)] }],
    ["too many topics", { topics: Array.from({ length: 5 }, () => "0x" + "11".repeat(32)) }],
    ["data", { data: "0x0" }],
    ["block hash", { blockHash: "0x01" }],
    ["transaction hash", { transactionHash: "0x" + "gg".repeat(32) }],
    ["removed", { removed: "false" }],
    ["transaction index", { transactionIndex: "0x20000000000001" }],
    ["log index", { logIndex: "0x20000000000001" }],
  ];

  for (const [field, override] of malformed)
    assert.throws(
      () => parseRpcLog(rawRpcLog(override), 8453n, Commitment.Realtime),
      (error: unknown) => error instanceof RpcError && error.code === "INVALID",
      field,
    );
});

function rawRpcLog(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    address: "0x0000000000000000000000000000000000000001",
    topics: [],
    data: "0x",
    blockNumber: "0x2a",
    blockHash: "0x" + "11".repeat(32),
    transactionHash: "0x" + "22".repeat(32),
    transactionIndex: "0x2",
    logIndex: "0x3",
    removed: false,
    ...overrides,
  };
}

test("transport failure is fail-fast when retries are disabled", async () => {
  let calls = 0;
  const failingFetch = (async () => {
    calls += 1;
    return new Response("upstream unavailable", { status: 503 });
  }) as typeof fetch;
  const client = new JsonRpcHttpClient("https://rpc.example", failingFetch);

  await assert.rejects(client.getCode(parseAddress("0x0000000000000000000000000000000000000001"), "latest"));
  assert.equal(calls, 1);
});

test("EIP-1898 code reads are pinned by block hash", async () => {
  const requests: RpcRequest[] = [];
  const client = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher(() => "0x6000", requests),
  );
  const hash = `0x${"11".repeat(32)}` as `0x${string}`;

  await client.getCodeAtHash(parseAddress("0x0000000000000000000000000000000000000001"), hash);

  assert.deepEqual(requests[0]?.params[1], { blockHash: hash });
});

test("backfill splits ranges larger than ten thousand blocks", async () => {
  const requests: RpcRequest[] = [];
  const client = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher(() => [], requests),
  );

  await client.getLogs(
    {
      fromBlock: 10n,
      toBlock: 10_010n,
      filter: {
        address: parseAddress("0x0000000000000000000000000000000000000001"),
        topics: quoteCriticalTopics(),
      },
    },
    8453n,
    Commitment.Canonical,
  );

  assert.equal(requests.length, 2);
  assert.deepEqual(
    requests.map((request) => {
      const filter = request.params[0] as { fromBlock: string; toBlock: string };
      return [filter.fromBlock, filter.toBlock];
    }),
    [
      ["0xa", "0x2719"],
      ["0x271a", "0x271a"],
    ],
  );
});

test("source rejects a deployment chainId mismatch before snapshot RPC", async () => {
  const requests: RpcRequest[] = [];
  const rpc = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher(() => {
      throw new Error("snapshot RPC must not be called");
    }, requests),
  );
  const source = new EvmRpcSource(rpc, "wss://rpc.example", Network.Evm, 97n);
  const config: DeploymentConfig = {
    network: Network.Evm,
    chainId: 98n,
    core: parseAddress("0x0000000000000000000000000000000000000001"),
    router: parseAddress("0x0000000000000000000000000000000000000002"),
    expectWhitelisted: true,
    deploymentBlock: 1n,
    expectedImplementation: parseAddress("0x0000000000000000000000000000000000000003"),
    expectedImplementationCodeHash: `0x${"11".repeat(32)}`,
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    explicitLaneAssets: [],
  };

  await assert.rejects(source.snapshot(config), (error: unknown) => {
    return (error as { code?: string }).code === "INVALID";
  });
  assert.equal(requests.length, 0);
});

test("snapshot canonicalizes lane and partner address keys", async () => {
  const blockHash = `0x${"11".repeat(32)}` as `0x${string}`;
  const implementation = parseAddress("0x00000000000000000000000000000000000000aa");
  const implementationWord = `0x${"0".repeat(24)}${implementation.slice(2)}` as `0x${string}`;
  const runtimeCode = "0x6000" as const;
  const core = parseAddress("0x0000000000000000000000000000000000000001");
  const router = parseAddress("0x0000000000000000000000000000000000000002");
  const laneAsset = "0x0000000000000000000000000000000000000003" as Address;
  const cash = "0x0000000000000000000000000000000000000002" as Address;
  const cursor: ChainCursor = {
    chainId: 97n,
    blockNumber: 42n,
    executionBlockNumber: 42n,
    blockHash,
    commitment: Commitment.Canonical,
  };
  const rpc = {
    chainId: async () => 97n,
    blockCursor: async () => cursor,
    getStorageAtHash: async () => implementationWord,
    getCodeAtHash: async () => runtimeCode,
    client: {
      readContract: async (request: { functionName: string; args?: readonly unknown[] }) => {
        if (request.functionName === "cash") return cash;
        if (request.functionName === "whitelist") return false;
        if (request.functionName === "blacklistFeeMultiplier") return 1n;
        if (request.functionName === "lane") return `0x${(1n << 200n).toString(16).padStart(64, "0")}` as `0x${string}`;
        if (request.functionName === "reserves") {
          const asset = String(request.args?.[0]).toLowerCase();
          return asset === cash.toLowerCase() ? [2000n, 0n, 0n, 0n, 0n] : [1000n, 0n, 0n, 0n, 1000n];
        }
        if (request.functionName === "partners") return [0n, 0, 0, router];
        throw new Error(`unexpected ${request.functionName}`);
      },
    },
  } as unknown as JsonRpcHttpClient;
  const config: DeploymentConfig = {
    network: Network.Evm,
    chainId: 97n,
    core,
    router,
    expectWhitelisted: false,
    deploymentBlock: 0n,
    expectedImplementation: implementation,
    expectedImplementationCodeHash: keccak256Hex(runtimeCode),
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    explicitLaneAssets: [laneAsset],
  };

  const snapshot = await new RpcSnapshotProvider(rpc).snapshot(config);

  assert.deepEqual([...snapshot.state.lanes.keys()], [laneAsset.toLowerCase()]);
  assert.deepEqual(
    [...snapshot.state.feeProfile.partnerFeeBps.keys()].sort(),
    [laneAsset.toLowerCase(), cash.toLowerCase()].sort(),
  );
});

test("snapshot preserves the global blacklist multiplier after whitelist removal", async () => {
  const blockHash = `0x${"11".repeat(32)}` as `0x${string}`;
  const implementation = parseAddress("0x00000000000000000000000000000000000000aa");
  const implementationWord = `0x${"0".repeat(24)}${implementation.slice(2)}` as `0x${string}`;
  const runtimeCode = "0x6000" as const;
  const core = parseAddress("0x0000000000000000000000000000000000000001");
  const router = parseAddress("0x0000000000000000000000000000000000000002");
  const laneAsset = parseAddress("0x0000000000000000000000000000000000000003");
  const cash = parseAddress("0x0000000000000000000000000000000000000004");
  const cursor: ChainCursor = {
    chainId: 97n,
    blockNumber: 42n,
    executionBlockNumber: 42n,
    blockHash,
    commitment: Commitment.Canonical,
  };
  const rpc = {
    chainId: async () => 97n,
    blockCursor: async () => cursor,
    getStorageAtHash: async () => implementationWord,
    getCodeAtHash: async () => runtimeCode,
    client: {
      readContract: async (request: { functionName: string; args?: readonly unknown[] }) => {
        if (request.functionName === "cash") return cash;
        if (request.functionName === "whitelist") return true;
        if (request.functionName === "blacklistFeeMultiplier") return 9n;
        if (request.functionName === "lane") return `0x${(1n << 200n).toString(16).padStart(64, "0")}` as `0x${string}`;
        if (request.functionName === "reserves") {
          const asset = String(request.args?.[0]).toLowerCase();
          return asset === cash.toLowerCase() ? [2000n, 0n, 0n, 0n, 0n] : [1000n, 0n, 0n, 0n, 1000n];
        }
        if (request.functionName === "partners") return [0n, 0, 0, router];
        throw new Error(`unexpected ${request.functionName}`);
      },
    },
  } as unknown as JsonRpcHttpClient;
  const config: DeploymentConfig = {
    network: Network.Evm,
    chainId: 97n,
    core,
    router,
    expectWhitelisted: true,
    deploymentBlock: 0n,
    expectedImplementation: implementation,
    expectedImplementationCodeHash: keccak256Hex(runtimeCode),
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    explicitLaneAssets: [laneAsset],
  };

  const snapshot = await new RpcSnapshotProvider(rpc).snapshot(config);
  assert.equal(snapshot.state.feeProfile.blacklistFeeMultiplier, 9n);

  const reducer = new QuoteReducer(snapshot.state, router);
  reducer.bootstrap(snapshot.cursor);
  reducer.apply(
    {
      ...snapshot.cursor,
      blockNumber: 43n,
      executionBlockNumber: 43n,
      blockHash: `0x${"22".repeat(32)}`,
      transactionIndex: 0n,
      logIndex: 0n,
    },
    { kind: "WhitelistSet", router, whitelisted: false },
  );

  const checkpoint = reducer.checkpoint(config);
  assert.equal(checkpoint?.state.feeProfile.whitelisted, false);
  assert.equal(checkpoint?.state.feeProfile.blacklistFeeMultiplier, 9n);
});
