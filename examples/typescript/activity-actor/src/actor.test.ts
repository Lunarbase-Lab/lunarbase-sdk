import assert from "node:assert/strict";
import test from "node:test";
import { createActor } from "./actor.js";
import { readConfig } from "./config.js";

const PRIVATE_KEY = `0x${"22".repeat(32)}`;
const DEPLOYMENT = {
  CORE_ADDRESS: "0x0000000000000000000000000000000000000001",
  CASH_ADDRESS: "0x0000000000000000000000000000000000000002",
  ASSET1_ADDRESS: "0x0000000000000000000000000000000000000003",
  ASSET2_ADDRESS: "0x0000000000000000000000000000000000000004",
  PAIRING_START_BLOCK: "1",
  EXPECTED_IMPLEMENTATION: "0x0000000000000000000000000000000000000005",
  EXPECTED_IMPLEMENTATION_CODE_HASH: "0x" + "11".repeat(32),
  EXPECTED_PROXY_CODE_HASH: "0x" + "22".repeat(32),
} as const;

test("all write methods fail before RPC when the CLI live gate is closed", async () => {
  const config = readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, BROADCAST: "true" });
  const actor = createActor(config, false);

  await assert.rejects(actor.mint(config.cash), /write gate is closed/);
  await assert.rejects(actor.approve(config.cash, 1n), /write gate is closed/);
  await assert.rejects(actor.swapExactIn(config.cash, config.asset1, 1n, 1n, 1n), /write gate is closed/);
});

test("the environment gate independently closes writes", async () => {
  const config = readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, BROADCAST: "false" });
  const actor = createActor(config, true);

  await assert.rejects(actor.mint(config.cash), /write gate is closed/);
});
