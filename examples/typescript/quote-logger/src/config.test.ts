import assert from "node:assert/strict";
import test from "node:test";
import { deriveWebSocketUrl, readEnvironment } from "./config.js";

test("derives WebSocket URLs from HTTP URLs", () => {
  assert.equal(deriveWebSocketUrl("https://rpc.example/v1/key"), "wss://rpc.example/v1/key");
  assert.equal(deriveWebSocketUrl("http://127.0.0.1:8545"), "ws://127.0.0.1:8545/");
});

test("requires only RPC and Core", () => {
  const config = readEnvironment({
    RPC_URL: "https://rpc.example",
    CORE_ADDRESS: "0x0000000000000000000000000000000000000001",
  });
  assert.equal(config.wsUrl, "wss://rpc.example/");
  assert.equal(config.quoteAmount, 1_000_000_000_000_000_000n);
  assert.equal(config.usesDemoRouter, true);
});
