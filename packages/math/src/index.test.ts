import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import {
  BPS,
  WAD,
  createLaneState,
  parseAddress,
  quote,
  solidityQuoteAmount,
  type Address,
  type LaneSlot0,
  type LaneState,
  type QuotePolicy,
  type QuoteState,
} from "./index.js";
import { checkedAdd, checkedMul, checkedSub, ensureDenominator } from "./arithmetic.js";
import { decimalNumberToBigInt } from "./decimal.js";
import { calculateFeeBpsForRouter, splitFee } from "./fees.js";
import { U256_MAX, ceilDiv, fullMulDivDown, mulDivDown256 } from "./public-arithmetic.js";
import {
  applyLaneUpdateSlot0,
  decodeLaneSlot0,
  encodeLaneSlot0,
  encodeUpdateFees,
  laneFeeBpsFromConventionalBps,
  lanePriceFromNumber,
  modelQuoteToLaneSlot0Fields,
} from "./slot0.js";

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
  feeAsset: string;
  feeAmount: string;
  partnerFee: string;
  treasuryFee: string;
}

interface GoldenVector {
  name: string;
  cash: string;
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
const WHITELISTED: QuotePolicy = { feeClass: "Whitelisted" };

const EMPTY_SLOT0: LaneSlot0 = {
  price: 0n,
  askFeeBps: 0n,
  bidFeeBps: 0n,
  pricePushThreshold: 0n,
  thresholdEnabled: false,
  latestUpdateBlock: 0n,
  exists: false,
  paused: false,
  blockDelay: 0,
  slippageKBps: 0,
  reservedHighBits: 0n,
};

test("decimal numbers scale through their canonical representation without binary multiplication drift", () => {
  assert.equal(decimalNumberToBigInt(2.824467842, 20), 282_446_784_200_000_000_000n);
  assert.equal(decimalNumberToBigInt(1e-7, 18), 100_000_000_000n);
  assert.equal(decimalNumberToBigInt(1.005, 2, "down"), 100n);
  assert.equal(decimalNumberToBigInt(1.005, 2, "nearest"), 101n);
  assert.equal(decimalNumberToBigInt(1.005, 2, "up"), 101n);
  assert.throws(() => decimalNumberToBigInt(1.005, 2), /not exactly representable/);
  assert.throws(() => decimalNumberToBigInt(Number.NaN, 18), /non-negative finite/);
  assert.throws(() => decimalNumberToBigInt(-1, 18), /non-negative finite/);
});

test("model quote numbers convert into exact LaneSlot0 integer fields", () => {
  const fields = modelQuoteToLaneSlot0Fields({
    anchorPrice: 63_975.3802738,
    askSpreadBps: 9.000016834202746,
    bidSpreadBps: 1.0000000000006477,
    cashDecimals: 18,
    assetDecimals: 18,
  });

  assert.deepEqual(fields, {
    price: 63_975_380_273_800_000_000_000n,
    askFeeBps: 900n,
    bidFeeBps: 100n,
  });
  const decoded = decodeLaneSlot0(encodeLaneSlot0({ ...EMPTY_SLOT0, ...fields, exists: true }));
  assert.equal(decoded.price, fields.price);
  assert.equal(decoded.askFeeBps, fields.askFeeBps);
  assert.equal(decoded.bidFeeBps, fields.bidFeeBps);
  assert.equal(lanePriceFromNumber(63_968.98273577262, 18, 18), 63_968_982_735_772_620_000_000n);
  assert.equal(lanePriceFromNumber(2.824467842, 18, 16), 282_446_784_200_000_000_000n);
  assert.equal(laneFeeBpsFromConventionalBps(9.009, "down"), 900n);
  assert.equal(laneFeeBpsFromConventionalBps(9.009, "up"), 901n);
});

test("slot0 round-trips boundaries and reserved bits", () => {
  const fields = {
    price: (1n << 112n) - 1n,
    askFeeBps: (1n << 20n) - 1n,
    bidFeeBps: (1n << 20n) - 1n,
    pricePushThreshold: (1n << 7n) - 1n,
    thresholdEnabled: true,
    latestUpdateBlock: (1n << 40n) - 1n,
    exists: true,
    paused: true,
    blockDelay: 0xff,
    slippageKBps: 0xffff_ffff,
    reservedHighBits: (1n << 14n) - 1n,
  };
  assert.deepEqual(decodeLaneSlot0(encodeLaneSlot0(fields)), fields);
});

test("full-width and checked-product multiplication differ", () => {
  assert.equal(fullMulDivDown(U256_MAX, 2n, 2n), U256_MAX);
  assert.throws(() => mulDivDown256(U256_MAX, 2n, 2n));
});

test("checked arithmetic rejects operands outside uint256", () => {
  assert.throws(() => checkedAdd(-1n, 2n), /outside uint256/);
  assert.throws(() => checkedSub(U256_MAX + 1n, U256_MAX), /outside uint256/);
  assert.throws(() => checkedMul(-1n, 0n), /outside uint256/);
  assert.throws(() => ceilDiv(-1n, 1n), /outside uint256/);
  assert.throws(() => ensureDenominator(-1n), /outside uint256/);
});

test("fee split applies the partner share to the explicit fee", () => {
  assert.deepEqual(splitFee(1_000_000n, 250_000n), [250_000n, 750_000n]);
  assert.deepEqual(splitFee(1n, 500_000n), [0n, 1n]);
  assert.deepEqual(splitFee(0n, 500_000n), [0n, 0n]);
  assert.deepEqual(splitFee(1_000_000n, BPS), [1_000_000n, 0n]);
  assert.throws(
    () => splitFee(U256_MAX, 2n),
    (error: unknown) => error instanceof Error && "code" in error && error.code === "OVERFLOW",
  );
  assert.throws(
    () => splitFee(1_000_000n, BPS + 1n),
    (error: unknown) => error instanceof Error && "code" in error && error.code === "OVERFLOW",
  );
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
    cashReserve: (1n << 128n) - 1n,
    lanes: new Map([
      [
        asset,
        createLaneState(
          encodeLaneSlot0({
            ...EMPTY_SLOT0,
            price: 2n * WAD,
            askFeeBps: 10_000n,
            latestUpdateBlock: 1n,
            exists: true,
          }),
          (1n << 128n) - 1n,
          1_000_000n,
        ),
      ],
    ]),
    blacklistFeeMultiplier: 1n,
  };
  const policy: QuotePolicy = { feeClass: "NonWhitelisted", verifiedPartnerFeeBps: 500_000 };
  const outcome = quote({ assetIn: cash, assetOut: asset, amount: 100n, mode: "ExactIn" }, 1n, state, policy);
  assert.equal(outcome.kind, "Available");
  if (outcome.kind === "Available") assert.equal(outcome.result.feeAsset, asset);
  const unavailable = quote(
    { assetIn: cash, assetOut: asset, amount: 1n, mode: "ExactOut" },
    1n,
    { ...state, lanes: new Map() },
    policy,
  );
  const exactIn = {
    assetIn: address("1"),
    assetOut: address("2"),
    amount: 1n,
    mode: "ExactIn",
  } as const;
  const exactOut = {
    ...exactIn,
    mode: "ExactOut",
  } as const;
  assert.equal(solidityQuoteAmount(exactIn, unavailable), 0n);
  assert.equal(solidityQuoteAmount(exactOut, unavailable), U256_MAX);
  assert.equal(
    solidityQuoteAmount(
      {
        ...exactOut,
        amount: 0n,
      },
      unavailable,
    ),
    0n,
  );
});

test("verified partner profiles change only accounting allocation", () => {
  const cash = address("1");
  const asset = address("2");
  const state: QuoteState = {
    cash,
    cashReserve: (1n << 128n) - 1n,
    lanes: new Map([
      [
        asset,
        createLaneState(
          encodeLaneSlot0({
            ...EMPTY_SLOT0,
            price: WAD,
            askFeeBps: 10_000n,
            latestUpdateBlock: 1n,
            exists: true,
          }),
          (1n << 128n) - 1n,
          1_000_000n,
        ),
      ],
    ]),
    blacklistFeeMultiplier: 1n,
  };
  const request = { assetIn: cash, assetOut: asset, amount: 100_000n, mode: "ExactIn" } as const;
  const low = quote(request, 1n, state, { feeClass: "Whitelisted", verifiedPartnerFeeBps: 100_000 });
  const high = quote(request, 1n, state, { feeClass: "Whitelisted", verifiedPartnerFeeBps: 900_000 });
  assert.equal(low.kind, "Available");
  assert.equal(high.kind, "Available");
  if (low.kind !== "Available" || high.kind !== "Available") return;
  assert.deepEqual({ ...low.result, feeAllocation: undefined }, { ...high.result, feeAllocation: undefined });
  assert.notDeepEqual(low.result.feeAllocation, high.result.feeAllocation);
});

test("lane quote TTL includes boundary and expires next block", () => {
  const cash = address("1");
  const asset = address("2");
  const state: QuoteState = {
    cash,
    cashReserve: (1n << 128n) - 1n,
    lanes: new Map([
      [
        asset,
        createLaneState(
          encodeLaneSlot0({
            ...EMPTY_SLOT0,
            price: WAD,
            latestUpdateBlock: 100n,
            exists: true,
            blockDelay: 3,
          }),
          (1n << 128n) - 1n,
          1_000_000n,
        ),
      ],
    ]),
    blacklistFeeMultiplier: 1n,
  };
  const request = { assetIn: cash, assetOut: asset, amount: 100n, mode: "ExactIn" as const };

  assert.equal(quote(request, 100n, state, WHITELISTED).kind, "Available");
  assert.equal(quote(request, 103n, state, WHITELISTED).kind, "Available");
  assert.deepEqual(quote(request, 104n, state, WHITELISTED), {
    kind: "Unavailable",
    reason: { kind: "StaleLane", asset },
  });
});

test("packed update preserves threshold and reserved bits", () => {
  const previous = encodeLaneSlot0({
    price: 7n,
    askFeeBps: 8n,
    bidFeeBps: 9n,
    pricePushThreshold: 63n,
    thresholdEnabled: true,
    latestUpdateBlock: 10n,
    exists: true,
    paused: false,
    blockDelay: 15,
    slippageKBps: 16,
    reservedHighBits: (1n << 14n) - 1n,
  });
  const updated = decodeLaneSlot0(applyLaneUpdateSlot0(previous, 11n, encodeUpdateFees(12n, 13n), 14n));
  assert.equal(updated.price, 11n);
  assert.equal(updated.askFeeBps, 12n);
  assert.equal(updated.bidFeeBps, 13n);
  assert.equal(updated.latestUpdateBlock, 14n);
  assert.equal(updated.pricePushThreshold, 63n);
  assert.equal(updated.thresholdEnabled, true);
});

test("packed update applies strict symmetric threshold pause and accepts later updates", () => {
  const base = encodeLaneSlot0({
    ...EMPTY_SLOT0,
    price: 100n,
    pricePushThreshold: 10n,
    thresholdEnabled: true,
    exists: true,
  });
  const boundary = decodeLaneSlot0(applyLaneUpdateSlot0(base, 110n, 0n, 7n));
  assert.equal(boundary.price, 110n);
  assert.equal(boundary.paused, false);
  for (const price of [89n, 111n]) {
    const paused = decodeLaneSlot0(applyLaneUpdateSlot0(base, price, 0n, 8n));
    assert.equal(paused.price, price);
    assert.equal(paused.paused, true);
  }
  const paused = applyLaneUpdateSlot0(base, 89n, 0n, 8n);
  const refreshed = decodeLaneSlot0(applyLaneUpdateSlot0(paused, 77n, encodeUpdateFees(12n, 13n), 9n));
  assert.equal(refreshed.price, 77n);
  assert.equal(refreshed.askFeeBps, 12n);
  assert.equal(refreshed.bidFeeBps, 13n);
  assert.equal(refreshed.latestUpdateBlock, 9n);
  assert.equal(refreshed.paused, true);
});

test("reserve boundary matches exact-in and exact-out settlement", () => {
  const cash = address("1");
  const asset = address("2");
  const lane = createLaneState(
    encodeLaneSlot0({
      ...EMPTY_SLOT0,
      price: WAD,
      askFeeBps: 10_000n,
      latestUpdateBlock: 1n,
      exists: true,
    }),
    (1n << 128n) - 1n,
    1_000_000n,
  );
  const state: QuoteState = {
    cash,
    cashReserve: (1n << 128n) - 1n,
    lanes: new Map([[asset, lane]]),
    blacklistFeeMultiplier: 1n,
  };
  const exactIn = { assetIn: cash, assetOut: asset, amount: 100n, mode: "ExactIn" as const };
  const reference = quote(exactIn, 1n, state, WHITELISTED);
  assert.equal(reference.kind, "Available");
  if (reference.kind !== "Available") return;
  const required = reference.result.amountOut + reference.result.feeAmount;
  lane.assetReserve = required;
  assert.equal(quote(exactIn, 1n, state, WHITELISTED).kind, "Available");
  lane.assetReserve = required - 1n;
  assert.deepEqual(quote(exactIn, 1n, state, WHITELISTED), {
    kind: "Unavailable",
    reason: { kind: "InsufficientOutputReserve", asset },
  });

  const exactOut = { ...exactIn, amount: 100n, mode: "ExactOut" as const };
  lane.assetReserve = 100n;
  assert.equal(quote(exactOut, 1n, state, WHITELISTED).kind, "Available");
  lane.assetReserve = 99n;
  assert.deepEqual(quote(exactOut, 1n, state, WHITELISTED), {
    kind: "Unavailable",
    reason: { kind: "InsufficientOutputReserve", asset },
  });
});

test("route preserves contract evaluation order before zero-price sentinel", () => {
  const cash = address("1");
  const assetIn = address("2");
  const assetOut = address("3");
  const state: QuoteState = {
    cash,
    cashReserve: (1n << 128n) - 1n,
    lanes: new Map([
      [
        assetIn,
        createLaneState(
          encodeLaneSlot0({ ...EMPTY_SLOT0, price: 0n, latestUpdateBlock: 1n, exists: true }),
          (1n << 128n) - 1n,
          1n,
        ),
      ],
      [
        assetOut,
        createLaneState(
          encodeLaneSlot0({
            ...EMPTY_SLOT0,
            price: (1n << 112n) - 1n,
            latestUpdateBlock: 1n,
            exists: true,
          }),
          (1n << 128n) - 1n,
          1n,
        ),
      ],
    ]),
    blacklistFeeMultiplier: 1n,
  };

  assert.throws(
    () => quote({ assetIn, assetOut, amount: U256_MAX, mode: "ExactOut" }, 1n, state, WHITELISTED),
    (error: unknown) => error instanceof Error && "code" in error && error.code === "OVERFLOW",
  );
});

test("shared golden vectors match TypeScript engine", () => {
  const fixture = JSON.parse(
    readFileSync(new URL("../../../fixtures/quote-vectors.json", import.meta.url), "utf8"),
  ) as { schemaVersion: string; mathCompatibilityVersion: string; vectors: GoldenVector[] };
  assert.equal(fixture.schemaVersion, "1");
  assert.equal(fixture.mathCompatibilityVersion, "lunarbase-pmm-v2");
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
            exists: lane.exists,
            paused: lane.paused,
            blockDelay: Number(lane.blockDelay),
            slippageKBps: Number(lane.slippageKBps),
            reservedHighBits: 0n,
          }),
          (1n << 128n) - 1n,
          BigInt(lane.principal),
        ),
      );
    }
    const state: QuoteState = {
      cash,
      cashReserve: (1n << 128n) - 1n,
      lanes,
      blacklistFeeMultiplier: BigInt(vector.blacklistFeeMultiplier),
    };
    const policy: QuotePolicy = {
      feeClass: vector.whitelisted ? "Whitelisted" : "NonWhitelisted",
      verifiedPartnerFeeBps: Number(vector.partnerFeeBps),
    };
    const request = {
      assetIn,
      assetOut,
      amount: BigInt(vector.amount),
      mode: vector.mode as "ExactIn" | "ExactOut",
    };
    if (vector.expectedError) {
      assert.throws(
        () => quote(request, BigInt(vector.executionBlockNumber), state, policy),
        (error: unknown) =>
          error instanceof Error && "code" in error && (error as { code: unknown }).code === "OVERFLOW",
        vector.name,
      );
      continue;
    }
    const outcome = quote(request, BigInt(vector.executionBlockNumber), state, policy);
    if (vector.expected) {
      assert.equal(outcome.kind, "Available", vector.name);
      if (outcome.kind === "Available") {
        assert.equal(outcome.result.amountIn, BigInt(vector.expected.amountIn));
        assert.equal(outcome.result.amountOut, BigInt(vector.expected.amountOut));
        assert.equal(outcome.result.feeAsset, parseAddress(vector.expected.feeAsset));
        assert.equal(outcome.result.feeAmount, BigInt(vector.expected.feeAmount));
        assert.equal(outcome.result.feeAllocation?.partnerFee, BigInt(vector.expected.partnerFee));
        assert.equal(outcome.result.feeAllocation?.treasuryFee, BigInt(vector.expected.treasuryFee));
      }
    } else {
      assert.equal(solidityQuoteAmount(request, outcome), BigInt(vector.expectedPublicAmount ?? "missing"));
    }
  }
});
