/** Strict Core event ABI decoder. */
import { U256_MAX, type Address, type Word } from "@lunarbase/math";
import type { ContractLog, QuoteEvent } from "../model.js";
import { LogDecodeError } from "../model.js";

const EVENT_TOPICS = {
  LaneAdded: 0x1c61848d54083be4bfb8a26449add9f919cf1efd4ca608005f7f3f6aa0cef958n,
  LaneRemoved: 0xdaa054a7d9aa74d7b3ee43f36a9a292169f22fbf60106608accc3161633fba98n,
  LaneUpdated: 0x4c5259bbfc22dbcf1f2d79e1e95c193e979499cc1b29f4b9f38d972cb383bd7an,
  SlippageKSet: 0x284eddda3b70079855640ccd104ec7b972a8a3f8a46b157278ad1a26812cbdf8n,
  PartnerInfoSet: 0x5155dfcae951816ec9ea329d96736edf3278b9d1cbe75191c70f004efe67a377n,
  PartnerFeeSet: 0x785135eb22f3bdb08e949e200b6e47291b10a11aa8d27879046da64dff565b85n,
  WhitelistSet: 0x0aa5ec5ffdc7f6f9c4d0dded489d7450297155cb2f71cb771e02427f7dff4f51n,
  BlacklistFeeMultiplierSet: 0xa15057886e6ebcdf47294bcb091d686031124d1041cafe00740e93667bacd186n,
  DepositExecuted: 0x9fb4891ffe3e11f428f3f10fa362b7938a364beebc215a2ec1db56a8d05ba20fn,
  WithdrawalExecuted: 0x722ca578dc087cbf283dc08891a94ba45b7119723568feda14da8f4c9c35d251n,
} as const;

/** Returns all Core topic0 values which can change quote-critical state. */
export function quoteCriticalTopics(): readonly Word[] {
  return Object.values(EVENT_TOPICS);
}
function bytes(data: string): Uint8Array {
  if (!/^0x(?:[0-9a-f]{2})*$/i.test(data))
    throw new LogDecodeError("INVALID_DATA_LENGTH", "event data is not even-length hex");
  const value = data.slice(2);
  const result = new Uint8Array(value.length / 2);
  for (let i = 0; i < result.length; i += 1) result[i] = Number.parseInt(value.slice(i * 2, i * 2 + 2), 16);
  return result;
}
function word(data: Uint8Array, index: number): Word {
  const start = index * 32;
  if (start < 0 || start + 32 > data.length) throw new LogDecodeError("INVALID_DATA_LENGTH", "missing ABI data word");
  let value = 0n;
  for (const byte of data.slice(start, start + 32)) value = (value << 8n) | BigInt(byte);
  return value;
}
function address(topic: Word): Address {
  if (topic < 0n || topic > U256_MAX) throw new LogDecodeError("INVALID_ADDRESS", "topic outside uint256");
  const value = topic.toString(16).padStart(64, "0");
  if (value.slice(0, 24) !== "0".repeat(24))
    throw new LogDecodeError("INVALID_ADDRESS", "indexed address is not ABI padded");
  return `0x${value.slice(24)}`;
}
function topics(value: readonly Word[], count: number): void {
  if (value.length !== count) throw new LogDecodeError("INVALID_TOPIC_COUNT", "invalid event topic count");
}
function data(value: Uint8Array, count: number): void {
  if (value.length !== count * 32) throw new LogDecodeError("INVALID_DATA_LENGTH", "invalid event data length");
}
function bool(value: Word): boolean {
  if (value === 0n) return false;
  if (value === 1n) return true;
  throw new LogDecodeError("INVALID_BOOLEAN", "invalid ABI boolean");
}

/**
 * Decodes a normalized Core event log with strict topic/data arity checks.
 * Unknown topic0 values return `undefined`; malformed known events throw
 * `LogDecodeError` so the reducer can fail closed.
 */
export function decodeCoreEvent(log: ContractLog): QuoteEvent | undefined {
  const topic0 = log.topics[0];
  if (topic0 === undefined) throw new LogDecodeError("MISSING_TOPIC0", "event log has no topic0");
  const payload = bytes(log.data);
  const input = log.topics;
  if (topic0 === EVENT_TOPICS.LaneAdded) {
    topics(input, 2);
    data(payload, 0);
    return { kind: "LaneAdded", asset: address(input[1]) };
  }
  if (topic0 === EVENT_TOPICS.LaneRemoved) {
    topics(input, 2);
    data(payload, 0);
    return { kind: "LaneRemoved", asset: address(input[1]) };
  }
  if (topic0 === EVENT_TOPICS.LaneUpdated) {
    topics(input, 2);
    data(payload, 1);
    return { kind: "LaneUpdated", asset: address(input[1]), slot0: word(payload, 0) };
  }
  if (topic0 === EVENT_TOPICS.SlippageKSet) {
    topics(input, 2);
    data(payload, 2);
    return { kind: "SlippageKSet", asset: address(input[1]), newK: word(payload, 1) };
  }
  if (topic0 === EVENT_TOPICS.PartnerInfoSet) {
    topics(input, 4);
    data(payload, 1);
    return { kind: "PartnerInfoSet", router: address(input[1]), asset: address(input[2]), fee: word(payload, 0) };
  }
  if (topic0 === EVENT_TOPICS.PartnerFeeSet) {
    topics(input, 3);
    data(payload, 1);
    return { kind: "PartnerFeeSet", router: address(input[1]), asset: address(input[2]), fee: word(payload, 0) };
  }
  if (topic0 === EVENT_TOPICS.WhitelistSet) {
    topics(input, 2);
    data(payload, 1);
    return { kind: "WhitelistSet", router: address(input[1]), whitelisted: bool(word(payload, 0)) };
  }
  if (topic0 === EVENT_TOPICS.BlacklistFeeMultiplierSet) {
    topics(input, 1);
    data(payload, 1);
    return { kind: "BlacklistFeeMultiplierSet", multiplier: word(payload, 0) };
  }
  if (topic0 === EVENT_TOPICS.DepositExecuted) {
    topics(input, 4);
    data(payload, 1);
    return { kind: "DepositExecuted", asset: address(input[3]), principal: word(payload, 0) };
  }
  if (topic0 === EVENT_TOPICS.WithdrawalExecuted) {
    topics(input, 4);
    data(payload, 4);
    return { kind: "WithdrawalExecuted", asset: address(input[3]), principal: word(payload, 0) };
  }
  return undefined;
}
