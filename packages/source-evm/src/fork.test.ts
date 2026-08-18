import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Commitment, Network, type BlockRef } from "@lunarbase-lab/pmm-v2-client";
import {
  BLOCK_REF_RETAINED_BYTES,
  CanonicalWindow,
  ForkError,
  ForkResolver,
  JsonRpcHttpClient,
  RpcHttpBackend,
} from "./index.js";

type RpcRequest = {
  readonly id: number;
  readonly method: string;
  readonly params: readonly unknown[];
};

const CHAIN_ID = 97n;

test("canonical window enforces independent count and byte budgets", () => {
  const countWindow = new CanonicalWindow({ maxBlocks: 2, maxBytes: BLOCK_REF_RETAINED_BYTES * 4 });
  countWindow.pushHead(block(10n, 10, 9));
  countWindow.pushHead(block(11n, 11, 10));
  assert.throws(
    () => countWindow.pushHead(block(12n, 12, 11)),
    (error: unknown) => error instanceof ForkError && error.code === "BLOCK_BUDGET",
  );
  assert.equal(countWindow.length, 2);

  const byteWindow = new CanonicalWindow({ maxBlocks: 4, maxBytes: BLOCK_REF_RETAINED_BYTES * 2 });
  byteWindow.pushHead(block(10n, 10, 9));
  byteWindow.pushHead(block(11n, 11, 10));
  assert.throws(
    () => byteWindow.pushHead(block(12n, 12, 11)),
    (error: unknown) => error instanceof ForkError && error.code === "BYTE_BUDGET",
  );
  assert.equal(byteWindow.retainedBytes, BLOCK_REF_RETAINED_BYTES * 2);
});

test("finality uses a start index and retains the boundary", () => {
  const window = new CanonicalWindow();
  window.pushHead(block(10n, 10, 9));
  window.pushHead(block(11n, 11, 10));
  window.pushHead(block(12n, 12, 11));

  window.advanceFinalized(block(11n, 11, 10, Commitment.Finalized));

  assert.deepEqual(
    [...window.blocks()].map((entry) => entry.cursor.blockNumber),
    [11n, 12n],
  );
  assert.equal(window.finalized?.cursor.blockNumber, 11n);
});

test("direct fork resolves without HTTP and applies atomically", async () => {
  let calls = 0;
  const rpc = new JsonRpcHttpClient("https://rpc.example", (async () => {
    calls += 1;
    throw new Error("HTTP must not be called");
  }) as typeof fetch);
  const resolver = resolverFor(rpc, 4);
  const window = seededWindow();
  const replacement = block(11n, 31, 10);

  const resolution = await resolver.resolve(window, replacement);

  assert.equal(calls, 0);
  assert.deepEqual(resolution.oldBranch.map(hashByte), [21]);
  assert.deepEqual(resolution.newBranch.map(hashByte), [31]);
  window.applyResolution(resolution);
  assert.equal(window.tip?.cursor.blockHash, replacement.cursor.blockHash);
});

test("deep fork walks only missing replacement parents", async () => {
  const requests: RpcRequest[] = [];
  const rpc = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher((request) => {
      if (request.method === "eth_chainId") return "0x61";
      assert.equal(request.method, "eth_getBlockByHash");
      const requested = request.params[0];
      if (requested === hash(42)) return rpcBlock(12n, 42, 41);
      if (requested === hash(41)) return rpcBlock(11n, 41, 10);
      throw new Error(`unexpected block ${String(requested)}`);
    }, requests),
  );
  const resolver = resolverFor(rpc, 4);
  const window = new CanonicalWindow();
  window.pushHead(block(10n, 10, 9));
  window.pushHead(block(11n, 21, 10));
  window.pushHead(block(12n, 22, 21));
  window.pushHead(block(13n, 23, 22));

  const resolution = await resolver.resolve(window, block(13n, 43, 42));

  assert.deepEqual(resolution.oldBranch.map(hashByte), [21, 22, 23]);
  assert.deepEqual(resolution.newBranch.map(hashByte), [41, 42, 43]);
  assert.equal(requests.filter((request) => request.method === "eth_getBlockByHash").length, 2);
});

test("fork walk rejects a foreign HTTP chain before block lookup", async () => {
  const requests: RpcRequest[] = [];
  const rpc = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher((request) => {
      if (request.method === "eth_chainId") return "0x62";
      throw new Error("block lookup must not run");
    }, requests),
  );
  const resolver = resolverFor(rpc, 4);
  const window = seededWindow();

  await assert.rejects(resolver.resolve(window, block(11n, 31, 30)), /expected 97, got 98/);
  assert.deepEqual(
    requests.map((request) => request.method),
    ["eth_chainId"],
  );
});

test("depth, finality, and malformed correction failures preserve the tip", async () => {
  const rpc = new JsonRpcHttpClient(
    "https://rpc.example",
    rpcFetcher((request) => (request.method === "eth_chainId" ? "0x61" : rpcBlock(12n, 42, 41)), []),
  );
  const shallowResolver = resolverFor(rpc, 2);
  const deepWindow = new CanonicalWindow();
  deepWindow.pushHead(block(10n, 10, 9));
  deepWindow.pushHead(block(11n, 21, 10));
  deepWindow.pushHead(block(12n, 22, 21));
  deepWindow.pushHead(block(13n, 23, 22));
  const oldDeepTip = deepWindow.tip;
  await assert.rejects(
    shallowResolver.resolve(deepWindow, block(13n, 43, 42)),
    (error: unknown) => error instanceof ForkError && error.code === "DEPTH_EXCEEDED",
  );
  assert.equal(deepWindow.tip, oldDeepTip);

  const directResolver = resolverFor(rpc, 4);
  const window = seededWindow();
  window.advanceFinalized(block(10n, 10, 9, Commitment.Finalized));
  await assert.rejects(
    directResolver.resolve(window, block(11n, 51, 50)),
    (error: unknown) => error instanceof ForkError && error.code === "FINALIZED_CONFLICT",
  );
  const resolution = await directResolver.resolve(window, block(11n, 31, 10));
  const oldTip = window.tip;
  const malformed = {
    ...resolution,
    newBranch: [{ ...resolution.newBranch[0]!, parentHash: hash(99) }],
  };
  assert.throws(
    () => window.applyResolution(malformed),
    (error: unknown) => error instanceof ForkError && error.code === "DISCONNECTED",
  );
  assert.equal(window.tip, oldTip);
});

function seededWindow(): CanonicalWindow {
  const window = new CanonicalWindow();
  window.pushHead(block(10n, 10, 9));
  window.pushHead(block(11n, 21, 10));
  return window;
}

function resolverFor(rpc: JsonRpcHttpClient, maxDepth: number): ForkResolver {
  return new ForkResolver(new RpcHttpBackend(rpc, Network.Evm, CHAIN_ID), maxDepth);
}

function block(
  number: bigint,
  hashByteValue: number,
  parentByteValue: number,
  commitment = Commitment.Canonical,
): BlockRef {
  return {
    cursor: {
      chainId: CHAIN_ID,
      blockNumber: number,
      executionBlockNumber: number,
      blockHash: hash(hashByteValue),
      commitment,
    },
    parentHash: hash(parentByteValue),
  };
}

function rpcBlock(number: bigint, hashByteValue: number, parentByteValue: number): Record<string, unknown> {
  return {
    number: `0x${number.toString(16)}`,
    hash: hash(hashByteValue),
    parentHash: hash(parentByteValue),
    transactions: [],
  };
}

function hash(byte: number): `0x${string}` {
  return `0x${byte.toString(16).padStart(2, "0").repeat(32)}`;
}

function hashByte(value: BlockRef): number {
  return Number.parseInt(value.cursor.blockHash!.slice(2, 4), 16);
}

function rpcFetcher(responder: (request: RpcRequest) => unknown, requests: RpcRequest[]): typeof fetch {
  return (async (_input: string | URL | Request, init?: RequestInit) => {
    const request = JSON.parse(String(init?.body)) as RpcRequest;
    requests.push(request);
    return new Response(JSON.stringify({ jsonrpc: "2.0", id: request.id, result: responder(request) }), {
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
}
