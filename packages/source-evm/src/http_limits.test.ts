import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Commitment, quoteCriticalTopics } from "@lunarbase-lab/pmm-v2-client";
import { parseAddress } from "@lunarbase-lab/pmm-v2-math";
import { JsonRpcHttpClient, RpcError } from "./rpc.js";

test("request byte budget fails before invoking fetch", async () => {
  let calls = 0;
  const fetcher = (async () => {
    calls += 1;
    throw new Error("must not execute");
  }) as typeof fetch;
  const client = new JsonRpcHttpClient("https://rpc.example", fetcher, { maxRequestBytes: 1 });

  await assert.rejects(client.chainId(), (error: unknown) => error instanceof RpcError && error.code === "LIMIT");
  assert.equal(calls, 0);
});

test("streaming response byte budget fails before JSON deserialization", async () => {
  const fetcher = (async () => new Response("x".repeat(128))) as typeof fetch;
  const client = new JsonRpcHttpClient("https://rpc.example", fetcher, { maxResponseBytes: 64 });

  await assert.rejects(client.chainId(), (error: unknown) => error instanceof RpcError && error.code === "LIMIT");
});

test("backfill adaptively bisects only an oversized response range", async () => {
  const ranges: Array<[bigint, bigint]> = [];
  const fetcher = (async (_input: RequestInfo | URL, init?: RequestInit) => {
    const request = JSON.parse(String(init?.body)) as {
      id: number;
      params: readonly [{ fromBlock: string; toBlock: string }];
    };
    const from = BigInt(request.params[0].fromBlock);
    const to = BigInt(request.params[0].toBlock);
    ranges.push([from, to]);
    if (from !== to) return new Response("x".repeat(512));
    return new Response(JSON.stringify({ jsonrpc: "2.0", id: request.id, result: [] }));
  }) as typeof fetch;
  const client = new JsonRpcHttpClient("https://rpc.example", fetcher, { maxResponseBytes: 256 });

  const logs = await client.getLogs(
    {
      fromBlock: 10n,
      toBlock: 11n,
      filter: {
        address: parseAddress("0x0000000000000000000000000000000000000001"),
        topics: quoteCriticalTopics(),
      },
    },
    8453n,
    Commitment.Canonical,
  );

  assert.deepEqual(logs, []);
  assert.deepEqual(ranges, [
    [10n, 11n],
    [10n, 10n],
    [11n, 11n],
  ]);
});
