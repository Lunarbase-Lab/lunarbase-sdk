import assert from "node:assert/strict";
import test from "node:test";
import { getAddress, type Address } from "viem";
import type { ObservedSwap } from "./actor.js";
import { pairingPhaseAfterHistoryReset, pairingPhaseFromPlan, replayPairingHistory } from "./pairing-recovery.js";

const CASH = getAddress("0x0000000000000000000000000000000000000002");
const ASSET1 = getAddress("0x0000000000000000000000000000000000000003");
const HASH = `0x${"ab".repeat(32)}` as const;
const TRANSACTION_HASH = `0x${"cd".repeat(32)}` as const;
const SUPPORTED = new Set<Address>([CASH, ASSET1]);

function swap(assetIn: Address, assetOut: Address, amountIn: bigint, amountOut: bigint): ObservedSwap {
  return {
    transactionHash: TRANSACTION_HASH,
    blockNumber: 123n,
    blockHash: HASH,
    transactionIndex: 0,
    logIndex: 0,
    assetIn,
    assetOut,
    amountIn,
    amountOut,
  };
}

test("replays an opening and return into a closed cycle", () => {
  const plan = replayPairingHistory(
    { kind: "opening" },
    [swap(CASH, ASSET1, 10n, 7n), swap(ASSET1, CASH, 7n, 9n)],
    SUPPORTED,
  );
  assert.equal(plan, undefined);
  assert.deepEqual(pairingPhaseFromPlan(plan), { kind: "opening" });
});

test("continues a locally checkpointed pending return", () => {
  const plan = replayPairingHistory(
    { kind: "return", assetIn: ASSET1, assetOut: CASH, maximumAmountIn: 7n },
    [swap(ASSET1, CASH, 6n, 9n)],
    SUPPORTED,
  );
  assert.equal(plan, undefined);
});

test("stale recovery always starts fresh", () => {
  assert.deepEqual(pairingPhaseAfterHistoryReset(), { kind: "opening" });
});

test("fails closed when replay does not match the pending direction or amount", () => {
  const pending = { kind: "return", assetIn: ASSET1, assetOut: CASH, maximumAmountIn: 7n } as const;
  assert.throws(() => replayPairingHistory(pending, [swap(CASH, ASSET1, 6n, 9n)], SUPPORTED), /pending return/);
  assert.throws(() => replayPairingHistory(pending, [swap(ASSET1, CASH, 8n, 9n)], SUPPORTED), /exceeds/);
  assert.throws(
    () =>
      replayPairingHistory(
        { ...pending, assetIn: getAddress("0x0000000000000000000000000000000000000001") },
        [],
        SUPPORTED,
      ),
    /unsupported/,
  );
});
