import assert from "node:assert/strict";
import test from "node:test";
import { createActor } from "./actor.js";
import { readConfig } from "./config.js";

const PRIVATE_KEY = `0x${"22".repeat(32)}`;

test("all write methods fail before RPC when the CLI live gate is closed", async () => {
  const config = readConfig({ ACTOR_PRIVATE_KEY: PRIVATE_KEY, BROADCAST: "true" });
  const actor = createActor(config, false);

  await assert.rejects(actor.mint(config.cash), /write gate is closed/);
  await assert.rejects(actor.approve(config.cash, 1n), /write gate is closed/);
  await assert.rejects(actor.swapExactIn(config.cash, config.asset1, 1n, 1n, 1n), /write gate is closed/);
});

test("the environment gate independently closes writes", async () => {
  const config = readConfig({ ACTOR_PRIVATE_KEY: PRIVATE_KEY, BROADCAST: "false" });
  const actor = createActor(config, true);

  await assert.rejects(actor.mint(config.cash), /write gate is closed/);
});
