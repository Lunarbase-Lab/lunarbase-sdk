import {
  encodeLaneSlot0,
  createLaneState,
  quote,
  solidityExactInAmount,
  solidityExactOutAmountForRequest,
  type LaneState,
  type QuoteRequest,
  type QuoteState,
} from "./index.js";

declare const process: { argv: string[]; stdout: { write(value: string): void } };
declare const Bun: { file(path: string): { text(): Promise<string> } };

type LaneVector = {
  price: string;
  askFeeBps: string;
  bidFeeBps: string;
  latestUpdateBlock: string;
  exists: boolean;
  paused: boolean;
  blockDelay: string;
  slippageKBps: string;
  principal: string;
};
type Vector = {
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
  laneIn: LaneVector | null;
  laneOut: LaneVector | null;
};

const value = (input: string): bigint => BigInt(input);
const lane = (input: LaneVector): LaneState =>
  createLaneState(
    encodeLaneSlot0({
      price: value(input.price),
      askFeeBps: value(input.askFeeBps),
      bidFeeBps: value(input.bidFeeBps),
      pricePushThreshold: 0n,
      thresholdEnabled: false,
      latestUpdateBlock: value(input.latestUpdateBlock),
      reservedHighBits: 0n,
    }),
    value(input.principal),
    Number(input.slippageKBps),
    Number(input.blockDelay),
    input.exists,
    input.paused,
  );
function build(vector: Vector): { state: QuoteState; request: QuoteRequest; executionBlockNumber: bigint } {
  const state = {
    cash: vector.cash,
    lanes: new Map<string, LaneState>(),
    feeProfile: {
      whitelisted: vector.whitelisted,
      blacklistFeeMultiplier: value(vector.blacklistFeeMultiplier),
      partnerFeeBps: new Map<string, number>(),
    },
  };
  const feeAsset = vector.mode === "ExactIn" ? vector.assetOut : vector.assetIn;
  state.feeProfile.partnerFeeBps.set(feeAsset, Number(vector.partnerFeeBps));
  for (const [asset, input] of [
    [vector.assetIn, vector.laneIn],
    [vector.assetOut, vector.laneOut],
  ] as const)
    if (input) {
      state.lanes.set(asset, lane(input));
    }
  return {
    state,
    request: {
      assetIn: vector.assetIn,
      assetOut: vector.assetOut,
      amount: value(vector.amount),
      mode: vector.mode,
    },
    executionBlockNumber: value(vector.executionBlockNumber),
  };
}
function word(valueToWrite: bigint): Uint8Array {
  const output = new Uint8Array(32);
  let valueToShift = valueToWrite;
  for (let index = 31; index >= 0; index -= 1) {
    output[index] = Number(valueToShift & 0xffn);
    valueToShift >>= 8n;
  }
  return output;
}
function output(vector: Vector): Uint8Array {
  const built = build(vector);
  let words: bigint[];
  try {
    const outcome = quote(built.request, built.executionBlockNumber, built.state);
    words =
      outcome.kind === "Available"
        ? [
            1n,
            outcome.result.amountIn,
            outcome.result.amountOut,
            BigInt(outcome.result.feeAsset),
            outcome.result.feeAmount,
            outcome.result.partnerFee,
            outcome.result.treasuryFee,
          ]
        : [
            0n,
            solidityExactInAmount(outcome),
            solidityExactOutAmountForRequest(built.request, outcome),
            0n,
            0n,
            0n,
            0n,
          ];
  } catch {
    words = [2n, 0n, 0n, 0n, 0n, 0n, 0n];
  }
  const bytes = new Uint8Array(words.length * 32);
  words.forEach((item, index) => bytes.set(word(item), index * 32));
  return bytes;
}

const args = process.argv;
const file = args[args.indexOf("--file") + 1];
const index = Number(args[args.indexOf("--index") + 1]);
const fixture = JSON.parse(await Bun.file(file).text()) as { vectors: Vector[] };
process.stdout.write(
  `hex:${[...output(fixture.vectors[index])].map((item) => item.toString(16).padStart(2, "0")).join("")}`,
);
