#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import {
  createLaneState,
  quote,
  parseAddress,
  solidityQuoteAmount,
  type LaneState,
  type Address,
  type QuoteRequest,
  type QuoteState,
} from "@lunarbase-lab/pmm-v2-math";
import { encodeLaneSlot0 } from "@lunarbase-lab/pmm-v2-math/slot0";
import * as AbiParameters from "ox/AbiParameters";
import * as Hex from "ox/Hex";

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
  assetIn: string;
  assetOut: string;
  mode: "ExactIn" | "ExactOut";
  amount: string;
  executionBlockNumber: string;
  blacklistFeeMultiplier: string;
  whitelisted: boolean;
  partnerFeeBps: string;
  outputReserve?: string;
  laneIn: LaneVector | null;
  laneOut: LaneVector | null;
};

type AbiInteger = number | bigint;
type AbiLane = {
  present: boolean;
  price: bigint;
  askFeeBps: AbiInteger;
  bidFeeBps: AbiInteger;
  latestUpdateBlock: AbiInteger;
  exists: boolean;
  paused: boolean;
  blockDelay: AbiInteger;
  slippageKBps: AbiInteger;
  principal: bigint;
};

const abiLane =
  "(bool present,uint112 price,uint32 askFeeBps,uint32 bidFeeBps,uint40 latestUpdateBlock,bool exists,bool paused,uint8 blockDelay,uint32 slippageKBps,uint128 principal)";
const abiFuzzVector = AbiParameters.from(
  `(address cash,address assetIn,address assetOut,bool exactIn,uint256 amount,uint40 executionBlockNumber,uint256 blacklistFeeMultiplier,bool whitelisted,uint32 partnerFeeBps,uint128 outputReserve,${abiLane} laneIn,${abiLane} laneOut) vector`,
);

const value = (input: string): bigint => BigInt(input);
const lane = (input: LaneVector, assetReserve: bigint): LaneState =>
  createLaneState(
    encodeLaneSlot0({
      price: value(input.price),
      askFeeBps: value(input.askFeeBps),
      bidFeeBps: value(input.bidFeeBps),
      pricePushThreshold: 0n,
      thresholdEnabled: false,
      latestUpdateBlock: value(input.latestUpdateBlock),
      exists: input.exists,
      paused: input.paused,
      blockDelay: Number(input.blockDelay),
      slippageKBps: Number(input.slippageKBps),
      reservedHighBits: 0n,
    }),
    assetReserve,
    value(input.principal),
  );
function build(vector: Vector): { state: QuoteState; request: QuoteRequest; executionBlockNumber: bigint } {
  const cash = parseAddress(vector.cash);
  const assetIn = parseAddress(vector.assetIn);
  const assetOut = parseAddress(vector.assetOut);
  const maxReserve = (1n << 128n) - 1n;
  const outputReserve = vector.outputReserve === undefined ? maxReserve : value(vector.outputReserve);
  const state = {
    cash,
    cashReserve: assetOut.toLowerCase() === cash.toLowerCase() ? outputReserve : maxReserve,
    lanes: new Map<Address, LaneState>(),
    feeProfile: {
      whitelisted: vector.whitelisted,
      blacklistFeeMultiplier: value(vector.blacklistFeeMultiplier),
      partnerFeeBps: new Map<Address, number>(),
    },
  };
  const feeAsset = vector.mode === "ExactIn" ? assetOut : assetIn;
  state.feeProfile.partnerFeeBps.set(feeAsset, Number(vector.partnerFeeBps));
  for (const [asset, input] of [
    [assetIn, vector.laneIn],
    [assetOut, vector.laneOut],
  ] as const)
    if (input) {
      const assetReserve = asset.toLowerCase() === assetOut.toLowerCase() ? outputReserve : maxReserve;
      state.lanes.set(asset, lane(input, assetReserve));
    }
  return {
    state,
    request: {
      assetIn,
      assetOut,
      amount: value(vector.amount),
      mode: vector.mode,
    },
    executionBlockNumber: value(vector.executionBlockNumber),
  };
}

function decodeLane(input: AbiLane): LaneVector | null {
  if (!input.present) return null;
  return {
    price: String(input.price),
    askFeeBps: String(input.askFeeBps),
    bidFeeBps: String(input.bidFeeBps),
    latestUpdateBlock: String(input.latestUpdateBlock),
    exists: input.exists,
    paused: input.paused,
    blockDelay: String(input.blockDelay),
    slippageKBps: String(input.slippageKBps),
    principal: String(input.principal),
  };
}

function decodeFuzzVector(encoded: Hex.Hex): Vector {
  const [input] = AbiParameters.decode(abiFuzzVector, encoded);
  return {
    cash: input.cash,
    assetIn: input.assetIn,
    assetOut: input.assetOut,
    mode: input.exactIn ? "ExactIn" : "ExactOut",
    amount: String(input.amount),
    executionBlockNumber: String(input.executionBlockNumber),
    blacklistFeeMultiplier: String(input.blacklistFeeMultiplier),
    whitelisted: input.whitelisted,
    partnerFeeBps: String(input.partnerFeeBps),
    outputReserve: String(input.outputReserve),
    laneIn: decodeLane(input.laneIn),
    laneOut: decodeLane(input.laneOut),
  };
}

function output(vector: Vector): Hex.Hex {
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
            built.request.mode === "ExactOut" ? solidityQuoteAmount(built.request, outcome) : 0n,
            0n,
            0n,
            0n,
            0n,
            0n,
          ];
  } catch {
    words = [2n, 0n, 0n, 0n, 0n, 0n, 0n];
  }
  return Hex.concat(...words.map((item) => Hex.fromNumber(item, { size: 32 })));
}

const args = process.argv;
const vectorIndex = args.indexOf("--vector");
let vector: Vector;
if (vectorIndex !== -1) {
  vector = decodeFuzzVector(args[vectorIndex + 1] as Hex.Hex);
} else {
  const file = args[args.indexOf("--file") + 1];
  const index = Number(args[args.indexOf("--index") + 1]);
  const fixture = JSON.parse(await readFile(file, "utf8")) as { vectors: Vector[] };
  vector = fixture.vectors[index];
}
process.stdout.write(`hex:${output(vector).slice(2)}`);
