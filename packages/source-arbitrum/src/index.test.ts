import { strict as assert } from "node:assert";
import { test } from "node:test";
import { ArbitrumNitroSource } from "./index.js";

test("Arbitrum keeps standard logs and explicit execution context", () => {
  const source = new ArbitrumNitroSource(
    {
      httpRpcUrl: "http://unused",
      realtimeUrl: "ws://unused",
      chainId: 42161n,
    },
    {
      fetcher: (() => Promise.reject(new Error("unused"))) as typeof fetch,
      webSocketFactory: () => {
        throw new Error("unused");
      },
    },
  );
  assert.equal(source.config.logsSubscription, "logs");
});
