import { strict as assert } from "node:assert";
import { test } from "node:test";
import {
  Commitment,
  MATH_COMPATIBILITY_VERSION,
  Network,
  quoteCriticalTopics,
  type ChainCursor,
  type DeploymentConfig,
} from "@lunarbase/client";
import { parseAddress, type Address } from "@lunarbase/math";
import { JsonRpcHttpClient, RpcSnapshotProvider, keccak256Hex } from "./rpc.js";

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

test("snapshot canonicalizes lane and partner address keys", async () => {
  const blockHash = `0x${"11".repeat(32)}` as `0x${string}`;
  const implementation = parseAddress("0x00000000000000000000000000000000000000aa");
  const implementationWord = `0x${"0".repeat(24)}${implementation.slice(2)}` as `0x${string}`;
  const runtimeCode = "0x6000" as const;
  const core = parseAddress("0x0000000000000000000000000000000000000001");
  const router = parseAddress("0x0000000000000000000000000000000000000002");
  const laneAsset = "0x21f52a1d45DAb30b518b31CA8e44f91B588A8DEC" as Address;
  const cash = "0x2c10647a0D96cab7fE26044CA6d3F854280dC906" as Address;
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
