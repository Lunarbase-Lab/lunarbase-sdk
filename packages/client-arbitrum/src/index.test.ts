import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Network, type ClientConnectConfig } from "@lunarbase/client-core";
import { ArbitrumDataSource } from "./index.js";

test("Arbitrum keeps standard logs and explicit execution context", () => {
  const config: ClientConnectConfig = {
    deployment: {
      network: Network.Arbitrum,
      chainId: 42161n,
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
  const source = new ArbitrumDataSource(config, {
    fetcher: (() => Promise.reject(new Error("unused"))) as typeof fetch,
    webSocketFactory: () => {
      throw new Error("unused");
    },
  });
  assert.equal(source.config.logsSubscription, "logs");
});
