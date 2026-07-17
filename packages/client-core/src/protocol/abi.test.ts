import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Commitment } from "../model.js";
import { decodeCoreEvent, quoteCriticalTopics } from "./abi.js";

const depositTopic = 0x9fb4891ffe3e11f428f3f10fa362b7938a364beebc215a2ec1db56a8d05ba20fn;
const withdrawalTopic = 0x722ca578dc087cbf283dc08891a94ba45b7119723568feda14da8f4c9c35d251n;
const assetTopic = 0x1111111111111111111111111111111111111111n;

test("position topics match the pinned Solidity ABI", () => {
  assert.equal(quoteCriticalTopics().includes(depositTopic), true);
  assert.equal(quoteCriticalTopics().includes(withdrawalTopic), true);
});

test("withdrawal accepts all four non-indexed ABI words", () => {
  const payload = `0x${[7n, 6n, 1n, 0x2222222222222222222222222222222222222222n]
    .map((value) => value.toString(16).padStart(64, "0"))
    .join("")}`;

  assert.deepEqual(
    decodeCoreEvent({
      address: "0x2222222222222222222222222222222222222222",
      topics: [withdrawalTopic, 1n, 2n, assetTopic],
      data: payload,
      removed: false,
      cursor: {
        chainId: 1n,
        blockNumber: 1n,
        executionBlockNumber: 1n,
        sourceSequence: 1n,
        commitment: Commitment.Canonical,
      },
    }),
    {
      kind: "WithdrawalExecuted",
      asset: "0x1111111111111111111111111111111111111111",
      principal: 7n,
    },
  );
});
