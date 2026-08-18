import { readFile } from "node:fs/promises";

const fail = (message) => {
  throw new Error(`math public API policy: ${message}`);
};

const names = (body) =>
  body
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => entry.split(/\s+as\s+/).at(-1));

const assertSet = (actual, expected, label) => {
  const actualSorted = [...new Set(actual)].sort();
  const expectedSorted = [...expected].sort();
  if (JSON.stringify(actualSorted) !== JSON.stringify(expectedSorted)) {
    fail(`${label} changed: actual=${actualSorted.join(",")} expected=${expectedSorted.join(",")}`);
  }
};

const tsRoot = await readFile("packages/math/src/index.ts", "utf8");
if (/^export\s+\*/m.test(tsRoot)) fail("TypeScript root facade must not use export *");

const tsRuntime = [...tsRoot.matchAll(/^export \{([^}]+)\}/gms)].flatMap((match) => names(match[1]));
const tsTypes = [...tsRoot.matchAll(/^export type \{([^}]+)\}/gms)].flatMap((match) => names(match[1]));
assertSet(
  tsRuntime,
  [
    "BPS",
    "MathError",
    "WAD",
    "createLaneState",
    "laneExists",
    "lanePaused",
    "parseAddress",
    "quote",
    "solidityQuoteAmount",
  ],
  "TypeScript root runtime exports",
);
assertSet(
  tsTypes,
  [
    "Address",
    "FeeAllocation",
    "FeeClass",
    "LaneSlot0",
    "LaneState",
    "QuoteMode",
    "QuoteOutcome",
    "QuotePolicy",
    "QuoteRequest",
    "QuoteResult",
    "QuoteState",
    "UnavailableReason",
    "Word",
  ],
  "TypeScript root type exports",
);

const arithmeticFacade = await readFile("packages/math/src/public-arithmetic.ts", "utf8");
const arithmeticExports = [...arithmeticFacade.matchAll(/^export \{([^}]+)\}/gms)].flatMap((match) => names(match[1]));
assertSet(
  arithmeticExports,
  ["U256_MAX", "ceilDiv", "fullMulDivDown", "fullMulDivUp", "mulDivDown256"],
  "TypeScript arithmetic subpath exports",
);

const manifest = JSON.parse(await readFile("packages/math/package.json", "utf8"));
assertSet(Object.keys(manifest.exports ?? {}), [".", "./arithmetic", "./slot0"], "TypeScript package subpaths");
const expectedTargets = {
  ".": ["./dist/index.d.ts", "./dist/index.js"],
  "./arithmetic": ["./dist/public-arithmetic.d.ts", "./dist/public-arithmetic.js"],
  "./slot0": ["./dist/slot0.d.ts", "./dist/slot0.js"],
};
for (const [subpath, expected] of Object.entries(expectedTargets)) {
  const target = manifest.exports[subpath];
  assertSet([target?.types, target?.import].filter(Boolean), expected, `${subpath} targets`);
}

const rustRoot = await readFile("crates/lunarbase-math/src/lib.rs", "utf8");
assertSet(
  [...rustRoot.matchAll(/^pub mod ([a-z0-9_]+);$/gm)].map((match) => match[1]),
  ["arithmetic", "prelude", "slot0"],
  "Rust public modules",
);
const rustExports = [...rustRoot.matchAll(/^pub use [a-z0-9_]+::\{([^}]+)\};/gms)].flatMap((match) => names(match[1]));
assertSet(
  rustExports,
  [
    "Address",
    "B256",
    "BPS",
    "Bytes",
    "FeeAllocation",
    "FeeClass",
    "LaneSlot0",
    "LaneState",
    "MathError",
    "QuoteError",
    "QuoteMode",
    "QuoteOutcome",
    "QuotePolicy",
    "QuoteRequest",
    "QuoteResult",
    "QuoteState",
    "U256",
    "UnavailableReason",
    "WAD",
    "decode_lane_slot0",
    "encode_lane_slot0",
    "quote",
    "solidity_quote_amount",
  ],
  "Rust root exports",
);

const forbidden = [
  [/pub mod (?:fees|quote|state|types);/, "internal Rust module is public"],
  [/pub fn decode_update_fees/, "internal Rust update-fee decoder is public"],
  [/export function decodeUpdateFees/, "internal TypeScript update-fee decoder is public"],
  [/solidity_(?:exact_in|exact_out)_amount/, "retired Rust scalar helper remains"],
  [/solidityExact(?:In|Out)Amount/, "retired TypeScript scalar helper remains"],
  [/quote_lane_route_exact_(?:in|out)_fee/, "dead Rust route fee helper remains"],
  [/quoteLaneRouteExact(?:In|Out)Fee/, "dead TypeScript route fee helper remains"],
  [/lane_price_from_word/, "dead Rust lane-price helper remains"],
  [/EMPTY_SLOT0/, "test fixture escaped into TypeScript source API"],
];
const mathSources = [
  await readFile("crates/lunarbase-math/src/lib.rs", "utf8"),
  await readFile("crates/lunarbase-math/src/fees.rs", "utf8"),
  await readFile("crates/lunarbase-math/src/quote.rs", "utf8"),
  await readFile("crates/lunarbase-math/src/slot0.rs", "utf8"),
  await readFile("packages/math/src/fees.ts", "utf8"),
  await readFile("packages/math/src/quote.ts", "utf8"),
  await readFile("packages/math/src/slot0.ts", "utf8"),
  await readFile("packages/math/src/types.ts", "utf8"),
].join("\n");
for (const [pattern, message] of forbidden) {
  if (pattern.test(mathSources)) fail(message);
}

console.log("Math public API check passed.");
