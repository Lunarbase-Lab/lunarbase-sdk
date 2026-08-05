import assert from "node:assert/strict";
import test from "node:test";
import { readConfig } from "./config.js";

const PRIVATE_KEY = `0x${"11".repeat(32)}`;
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

test("loads bounded defaults with an explicit deployment", () => {
  const config = readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY });
  assert.equal(config.chainId, 97);
  assert.equal(config.broadcast, false);
  assert.equal(config.autoMint, true);
  assert.equal(config.allowanceBatchSwaps, 1_000);
  assert.equal(config.core, "0x0000000000000000000000000000000000000001");
  assert.equal(config.expectedImplementation, "0x0000000000000000000000000000000000000005");
  assert.equal(config.expectedImplementationCodeHash, "0x" + "11".repeat(32));
  assert.equal(config.maximumOutputReservePpm, 1_000);
  assert.equal(config.maximumSessionOutputReservePpm, 10_000);
  assert.equal(config.minimumLaneHeadroomBlocks, 2);
  assert.equal(config.receiptPollingMilliseconds, 250);
  assert.equal(config.minimumDelaySeconds, 0);
  assert.equal(config.maximumDelaySeconds, 0);
  assert.equal(config.retryDelaySeconds, 2);
  assert.equal(config.confirmations, 1);
  assert.equal(config.pairingFinalityConfirmations, 3);
  assert.equal(config.pairingStartBlock, 1n);
  assert.equal(config.pairingMaximumReplayBlocks, 50_000);
  assert.equal(config.maximumSwaps, 50);
});

test("requires an exact private key without exposing it", () => {
  assert.throws(() => readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: "0x1234" }), /exactly 32 testnet-only bytes/);
});

test("requires explicit deployment binding", () => {
  assert.throws(() => readConfig({ ACTOR_PRIVATE_KEY: PRIVATE_KEY }), /CORE_ADDRESS/);
});

test("rejects non-testnet chain ids, inverted delays, and unbounded runs", () => {
  assert.throws(() => readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, CHAIN_ID: "56" }), /chain id 97/);
  assert.throws(
    () =>
      readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, MIN_DELAY_SECONDS: "10", MAX_DELAY_SECONDS: "9" }),
    /must not exceed/,
  );
  assert.throws(() => readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, MAX_SWAPS: "0" }));
});
assert.throws(() => readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, MAX_SWAPS: "1" }));
assert.throws(() => readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, PAIRING_START_BLOCK: "-1" }));

test("rejects malformed deployment hashes", () => {
  assert.throws(() =>
    readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, EXPECTED_IMPLEMENTATION_CODE_HASH: "0x1234" }),
  );
});

test("validates receipt polling interval bounds", () => {
  assert.throws(() =>
    readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, RECEIPT_POLLING_MILLISECONDS: "99" }),
  );
  assert.throws(() =>
    readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, RECEIPT_POLLING_MILLISECONDS: "10001" }),
  );
  assert.equal(
    readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, RECEIPT_POLLING_MILLISECONDS: "100" })
      .receiptPollingMilliseconds,
    100,
  );
  assert.equal(
    readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, RECEIPT_POLLING_MILLISECONDS: "10000" })
      .receiptPollingMilliseconds,
    10_000,
  );
});

test("requires pairing replay finality independently from receipt latency", () => {
  const config = readConfig({
    ...DEPLOYMENT,
    ACTOR_PRIVATE_KEY: PRIVATE_KEY,
    CONFIRMATIONS: "1",
    PAIRING_FINALITY_CONFIRMATIONS: "5",
  });
  assert.equal(config.confirmations, 1);
  assert.equal(config.pairingFinalityConfirmations, 5);
  assert.throws(() =>
    readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, PAIRING_FINALITY_CONFIRMATIONS: "1" }),
  );
  assert.throws(() =>
    readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, PAIRING_FINALITY_CONFIRMATIONS: "65" }),
  );
});

test("bounds pairing replay before an RPC provider can prune the requested range", () => {
  assert.throws(() => readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, PAIRING_MAX_REPLAY_BLOCKS: "999" }));
  assert.throws(() =>
    readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, PAIRING_MAX_REPLAY_BLOCKS: "1000001" }),
  );
  assert.equal(
    readConfig({ ...DEPLOYMENT, ACTOR_PRIVATE_KEY: PRIVATE_KEY, PAIRING_MAX_REPLAY_BLOCKS: "5000" })
      .pairingMaximumReplayBlocks,
    5_000,
  );
});
