import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import {
  BPS,
  EMPTY_SLOT0,
  U256_MAX,
  WAD,
  calculateFeeBpsForRouter,
  createLaneState,
  decodeLaneSlot0,
  encodeLaneSlot0,
  fullMulDivDown,
  mulDivDown256,
  parseAddress,
  quote,
  solidityExactInAmount,
  applyLaneUpdateSlot0,
  encodeUpdateFees,
  solidityExactOutAmount,
  solidityExactOutAmountForRequest,
  type LaneState,
  type Address,
  type QuoteState,
} from "./index.js";

interface GoldenLane {
  price: string;
  askFeeBps: string;
  bidFeeBps: string;
  latestUpdateBlock: string;
  exists: boolean;
  paused: boolean;
  blockDelay: string;
  slippageKBps: string;
  principal: string;
}

interface GoldenExpected {
  amountIn: string;
  amountOut: string;
  feeAmount: string;
}

interface GoldenVector {
  name: string;
  cash: string;
  router: string;
  assetIn: string;
  assetOut: string;
  mode: "ExactIn" | "ExactOut";
  amount: string;
  executionBlockNumber: string;
  blacklistFeeMultiplier: string;
  whitelisted: boolean;
  partnerFeeBps: string;
  laneIn: GoldenLane | null;
  laneOut: GoldenLane | null;
  expected?: GoldenExpected;
  expectedPublicAmount?: string;
  expectedError?: "Overflow";
}

const address = (last: string): Address => parseAddress(`0x${last.padStart(40, "0")}`);

test("slot0 round-trips boundaries and reserved bits", () => {
  const fields = {
    price: (1n << 112n) - 1n,
    askFeeBps: (1n << 20n) - 1n,
    bidFeeBps: (1n << 20n) - 1n,
    pricePushThreshold: (1n << 7n) - 1n,
    thresholdEnabled: true,
    latestUpdateBlock: (1n << 40n) - 1n,
    reservedHighBits: (1n << 56n) - 1n,
  };
  assert.deepEqual(decodeLaneSlot0(encodeLaneSlot0(fields)), fields);
});

test("full-width and checked-product multiplication differ", () => {
  assert.equal(fullMulDivDown(U256_MAX, 2n, 2n), U256_MAX);
  assert.throws(() => mulDivDown256(U256_MAX, 2n, 2n));
});

test("zero multiplier and whitelist behavior are explicit", () => {
  assert.equal(calculateFeeBpsForRouter(false, 0n, BPS), 0n);
  assert.equal(calculateFeeBpsForRouter(true, 0n, BPS + 1n), BPS);
});

test("address parsing and map lookups are case canonical", () => {
  const checksummed = "0x52908400098527886E0F7030069857D2E4169EE7";
  assert.equal(parseAddress(checksummed), checksummed.toLowerCase());
});

test("direct quote returns complete result and Solidity sentinels", () => {
  const cash = address("1");
  const asset = address("2");
  const state: QuoteState = {
    cash,
    lanes: new Map([
      [
        asset,
        createLaneState(
          encodeLaneSlot0({ ...EMPTY_SLOT0, price: 2n * WAD, askFeeBps: 10_000n }),
          1_000_000n,
          0,
          0,
          true,
          false,
        ),
      ],
    ]),
    feeProfile: {
      whitelisted: false,
      blacklistFeeMultiplier: 1n,
      partnerFeeBps: new Map([[asset, 500_000]]),
    },
  };
  const outcome = quote({ assetIn: cash, assetOut: asset, amount: 100n, mode: "ExactIn" }, 1n, state);
  assert.equal(outcome.kind, "Available");
  if (outcome.kind === "Available") assert.equal(outcome.result.feeAsset, asset);
  const unavailable = quote({ assetIn: cash, assetOut: asset, amount: 1n, mode: "ExactOut" }, 1n, {
    ...state,
    lanes: new Map(),
  });
  assert.equal(solidityExactInAmount(unavailable), 0n);
  assert.equal(solidityExactOutAmount(unavailable), U256_MAX);
});

test("packed update preserves threshold and reserved bits", () => {
  const previous = encodeLaneSlot0({
    price: 7n,
    askFeeBps: 8n,
    bidFeeBps: 9n,
    pricePushThreshold: 63n,
    thresholdEnabled: true,
    latestUpdateBlock: 10n,
    reservedHighBits: (1n << 56n) - 1n,
  });
  const updated = decodeLaneSlot0(applyLaneUpdateSlot0(previous, 11n, encodeUpdateFees(12n, 13n), 14n));
  assert.equal(updated.price, 11n);
  assert.equal(updated.askFeeBps, 12n);
  assert.equal(updated.bidFeeBps, 13n);
  assert.equal(updated.latestUpdateBlock, 14n);
  assert.equal(updated.pricePushThreshold, 63n);
  assert.equal(updated.thresholdEnabled, true);
});

test("shared golden vectors match TypeScript engine", () => {
  const fixture = JSON.parse(
    readFileSync(new URL("../../../fixtures/quote-vectors.json", import.meta.url), "utf8"),
  ) as { vectors: GoldenVector[] };
  for (const vector of fixture.vectors) {
    const cash = parseAddress(vector.cash);
    const assetIn = parseAddress(vector.assetIn);
    const assetOut = parseAddress(vector.assetOut);
    const lanes = new Map<Address, LaneState>();
    for (const [asset, lane] of [
      [assetIn, vector.laneIn],
      [assetOut, vector.laneOut],
    ] as const) {
      if (!lane) continue;
      lanes.set(
        asset,
        createLaneState(
          encodeLaneSlot0({
            price: BigInt(lane.price),
            askFeeBps: BigInt(lane.askFeeBps),
            bidFeeBps: BigInt(lane.bidFeeBps),
            pricePushThreshold: 0n,
            thresholdEnabled: false,
            latestUpdateBlock: BigInt(lane.latestUpdateBlock),
            reservedHighBits: 0n,
          }),
          BigInt(lane.principal),
          Number(lane.slippageKBps),
          Number(lane.blockDelay),
          lane.exists,
          lane.paused,
        ),
      );
    }
    const feeAsset = vector.mode === "ExactIn" ? assetOut : assetIn;
    const state: QuoteState = {
      cash,
      lanes,
      feeProfile: {
        whitelisted: vector.whitelisted,
        blacklistFeeMultiplier: BigInt(vector.blacklistFeeMultiplier),
        partnerFeeBps: new Map([[feeAsset, Number(vector.partnerFeeBps)]]),
      },
    };
    const request = {
      assetIn,
      assetOut,
      amount: BigInt(vector.amount),
      mode: vector.mode as "ExactIn" | "ExactOut",
    };
    if (vector.expectedError) {
      assert.throws(
        () => quote(request, BigInt(vector.executionBlockNumber), state),
        (error: unknown) =>
          error instanceof Error && "code" in error && (error as { code: unknown }).code === "OVERFLOW",
        vector.name,
      );
      continue;
    }
    const outcome = quote(request, BigInt(vector.executionBlockNumber), state);
    if (vector.expected) {
      assert.equal(outcome.kind, "Available", vector.name);
      if (outcome.kind === "Available") {
        assert.equal(outcome.result.amountIn, BigInt(vector.expected.amountIn));
        assert.equal(outcome.result.amountOut, BigInt(vector.expected.amountOut));
        assert.equal(outcome.result.feeAmount, BigInt(vector.expected.feeAmount));
      }
    } else {
      assert.equal(
        solidityExactOutAmountForRequest(request, outcome),
        BigInt(vector.expectedPublicAmount ?? "missing"),
      );
    }
  }
});
