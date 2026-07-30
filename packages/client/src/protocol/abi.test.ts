import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Commitment } from "../model.js";
import { decodeCoreEvent, quoteCriticalTopics } from "./abi.js";
import { CORE_EVENT_TOPICS } from "./core.js";
import * as Hex from "ox/Hex";

const depositTopic = "0x9fb4891ffe3e11f428f3f10fa362b7938a364beebc215a2ec1db56a8d05ba20f";
const withdrawalTopic = "0x722ca578dc087cbf283dc08891a94ba45b7119723568feda14da8f4c9c35d251";
const assetTopic = Hex.padLeft("0x1111111111111111111111111111111111111111", 32);

test("position topics match the pinned Solidity ABI", () => {
  assert.equal(quoteCriticalTopics().includes(depositTopic), true);
  assert.equal(quoteCriticalTopics().includes(withdrawalTopic), true);
});

test("lane control topics and payloads match the pinned Solidity ABI", () => {
  assert.equal(CORE_EVENT_TOPICS.LaneAdded, "0x1c61848d54083be4bfb8a26449add9f919cf1efd4ca608005f7f3f6aa0cef958");
  assert.equal(CORE_EVENT_TOPICS.LanePausedSet, "0x457fade720abbce2ed945bda9c751bcadaddbd87a70e8d0c79b156e9aa4d3399");
  assert.equal(
    CORE_EVENT_TOPICS.PricePushThresholdSet,
    "0x6b38206650880c4736891c797636196db2056062d3a8011e4074feecbe8ae337",
  );

  assert.deepEqual(decodeCoreEvent(laneLog(CORE_EVENT_TOPICS.LaneAdded, [])), {
    kind: "LaneAdded",
    asset: "0x1111111111111111111111111111111111111111",
  });
  assert.deepEqual(decodeCoreEvent(laneLog(CORE_EVENT_TOPICS.LanePausedSet, [0n, 1n])), {
    kind: "LanePausedSet",
    asset: "0x1111111111111111111111111111111111111111",
    paused: true,
  });
  assert.deepEqual(decodeCoreEvent(laneLog(CORE_EVENT_TOPICS.PricePushThresholdSet, [9n, 17n, 1n, 0n])), {
    kind: "PricePushThresholdSet",
    asset: "0x1111111111111111111111111111111111111111",
    pricePushThreshold: 17,
    enabled: false,
  });
});

test("withdrawal accepts all four non-indexed ABI words", () => {
  const payload = `0x${[7n, 6n, 1n, 0x2222222222222222222222222222222222222222n]
    .map((value) => value.toString(16).padStart(64, "0"))
    .join("")}` as Hex.Hex;

  assert.deepEqual(
    decodeCoreEvent({
      address: "0x2222222222222222222222222222222222222222",
      topics: [withdrawalTopic, Hex.fromNumber(1, { size: 32 }), Hex.fromNumber(2, { size: 32 }), assetTopic],
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

function laneLog(topic0: Hex.Hex, words: readonly bigint[]) {
  return {
    address: "0x2222222222222222222222222222222222222222" as const,
    topics: [topic0, assetTopic],
    data: `0x${words.map((word) => word.toString(16).padStart(64, "0")).join("")}` as Hex.Hex,
    removed: false,
    cursor: {
      chainId: 1n,
      blockNumber: 1n,
      executionBlockNumber: 1n,
      sourceSequence: 1n,
      commitment: Commitment.Canonical,
    },
  };
}
