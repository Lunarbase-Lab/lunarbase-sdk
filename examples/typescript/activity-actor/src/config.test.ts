import assert from "node:assert/strict";
import test from "node:test";
import { readConfig } from "./config.js";

const PRIVATE_KEY = `0x${"11".repeat(32)}`;

test("loads safe pinned BSC Testnet defaults", () => {
  const config = readConfig({ ACTOR_PRIVATE_KEY: PRIVATE_KEY });
  assert.equal(config.chainId, 97);
  assert.equal(config.broadcast, false);
  assert.equal(config.autoMint, true);
  assert.equal(config.expectedImplementation, "0xCFa7de4418707d4FDC06e4634A4B2aE95Af528c7");
  assert.equal(
    config.expectedImplementationCodeHash,
    "0xdd4f26f3b1ff31ea9aef19ddffd549ca8669c91fc4d0355e9677c6f5b2b96897",
  );
  assert.equal(config.maximumOutputReservePpm, 1_000);
  assert.equal(config.maximumSessionOutputReservePpm, 10_000);
  assert.equal(config.minimumLaneHeadroomBlocks, 2);
  assert.equal(config.pairingStartBlock, 123_101_134n);
  assert.equal(config.maximumSwaps, 50);
});

test("requires an exact private key without exposing it", () => {
  assert.throws(() => readConfig({ ACTOR_PRIVATE_KEY: "0x1234" }), /exactly 32 testnet-only bytes/);
});

test("rejects non-testnet chain ids, inverted delays, and unbounded runs", () => {
  assert.throws(() => readConfig({ ACTOR_PRIVATE_KEY: PRIVATE_KEY, CHAIN_ID: "56" }), /chain id 97/);
  assert.throws(
    () =>
      readConfig({
        ACTOR_PRIVATE_KEY: PRIVATE_KEY,
        MIN_DELAY_SECONDS: "10",
        MAX_DELAY_SECONDS: "9",
      }),
    /must not exceed/,
  );
  assert.throws(() => readConfig({ ACTOR_PRIVATE_KEY: PRIVATE_KEY, MAX_SWAPS: "0" }));
});
assert.throws(() => readConfig({ ACTOR_PRIVATE_KEY: PRIVATE_KEY, MAX_SWAPS: "1" }));
assert.throws(() => readConfig({ ACTOR_PRIVATE_KEY: PRIVATE_KEY, PAIRING_START_BLOCK: "-1" }));

test("rejects malformed deployment hashes", () => {
  assert.throws(() =>
    readConfig({
      ACTOR_PRIVATE_KEY: PRIVATE_KEY,
      EXPECTED_IMPLEMENTATION_CODE_HASH: "0x1234",
    }),
  );
});
