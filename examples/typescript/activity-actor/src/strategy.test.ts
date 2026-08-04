import assert from "node:assert/strict";
import test from "node:test";
import {
  directedPairs,
  findSafeReturnAmount,
  isWithinReserveCap,
  minimumOutput,
  PairedSwapPlan,
  randomBigIntInclusive,
  randomDelayMilliseconds,
  SessionReserveBudget,
} from "./strategy.js";

test("builds every directed route without self-pairs", () => {
  const pairs = directedPairs(["cash", "asset1", "asset2"]);
  assert.equal(pairs.length, 6);
  assert.ok(pairs.every((pair) => pair.assetIn !== pair.assetOut));
});

test("alternates a fixed pair only after matching confirmed swaps", () => {
  const plan = new PairedSwapPlan({ assetIn: "A", assetOut: "B" });
  assert.equal(plan.pendingReturn, undefined);

  plan.recordConfirmed("A", "B", 17n);
  assert.deepEqual(plan.pendingReturn, { assetIn: "B", assetOut: "A", maximumAmountIn: 17n });

  plan.recordConfirmed("B", "A", 13n);
  assert.equal(plan.pendingReturn, undefined);
});

test("rejects mismatched confirmations without changing the pending direction", () => {
  const opening = new PairedSwapPlan({ assetIn: "A", assetOut: "B" });
  assert.throws(() => opening.recordConfirmed("B", "A", 5n), /required opening leg/);
  assert.equal(opening.pendingReturn, undefined);

  opening.recordConfirmed("A", "B", 5n);
  const pending = opening.pendingReturn;
  assert.throws(() => opening.recordConfirmed("A", "B", 4n), /pending return leg/);
  assert.deepEqual(opening.pendingReturn, pending);
  assert.throws(() => opening.recordConfirmed("B", "A", 0n), /positive uint256/);
  assert.deepEqual(opening.pendingReturn, pending);
});

test("preserves below-minimum raw output without assuming token decimals", () => {
  const restored = new PairedSwapPlan(
    { assetIn: "six-decimal-token", assetOut: "eighteen-decimal-token" },
    {
      assetIn: "eighteen-decimal-token",
      assetOut: "six-decimal-token",
      maximumAmountIn: 1n,
    },
  );

  assert.equal(restored.pendingReturn?.maximumAmountIn, 1n);
});

test("halves a return input until its non-zero quote is allowed", async () => {
  const attempted: bigint[] = [];
  const result = await findSafeReturnAmount(
    64n,
    (amountIn) => {
      attempted.push(amountIn);
      return amountIn * 10n;
    },
    (quotedOutput) => quotedOutput <= 100n,
  );

  assert.deepEqual(attempted, [64n, 32n, 16n, 8n]);
  assert.deepEqual(result, { amountIn: 8n, quotedOutput: 80n });
});

test("returns undefined when every halved return quote is zero", async () => {
  const attempted: bigint[] = [];
  const result = await findSafeReturnAmount(
    8n,
    (amountIn) => {
      attempted.push(amountIn);
      return 0n;
    },
    () => true,
  );

  assert.deepEqual(attempted, [8n, 4n, 2n, 1n]);
  assert.equal(result, undefined);
  assert.equal(
    await findSafeReturnAmount(
      0n,
      async () => 1n,
      () => true,
    ),
    undefined,
  );
});

test("applies ppm slippage with downward rounding", () => {
  assert.equal(minimumOutput(1_000_000n, 5_000), 995_000n);
  assert.equal(minimumOutput(3n, 500_000), 1n);
});

test("enforces the output reserve cap", () => {
  assert.equal(isWithinReserveCap(1n, 1_000n, 1_000), true);
  assert.equal(isWithinReserveCap(2n, 1_000n, 1_000), false);
  assert.equal(isWithinReserveCap(0n, 1_000n, 1_000), false);
});

test("caps cumulative session output against the first positive reserve", () => {
  const budget = new SessionReserveBudget<string>(10_000);
  budget.observe("asset", 0n);
  assert.equal(budget.allows("asset", 1n), false);

  budget.observe("asset", 10_000n);
  assert.equal(budget.allows("asset", 60n), true);
  budget.record("asset", 60n);
  assert.equal(budget.allows("asset", 40n), true);
  assert.equal(budget.allows("asset", 41n), false);

  budget.observe("asset", 100_000n);
  assert.deepEqual(budget.status("asset"), { baseline: 10_000n, spent: 60n, limit: 100n });
  assert.throws(() => budget.record("asset", 41n), /session reserve budget/);
});

test("random helpers stay inside inclusive bounds", () => {
  for (let index = 0; index < 64; index += 1) {
    const value = randomBigIntInclusive(7n, 19n);
    assert.ok(value >= 7n && value <= 19n);
  }
  const delay = randomDelayMilliseconds(2, 4);
  assert.ok(delay >= 2_000 && delay <= 4_000);
  assert.equal(delay % 1_000, 0);
});
