/** Strict Core event ABI decoder backed by Ox. */
import type { Address, Word } from "@lunarbase/math";
import * as AbiEvent from "ox/AbiEvent";
import * as Hex from "ox/Hex";
import type { ContractLog, QuoteEvent } from "../model.js";
import { LogDecodeError } from "../model.js";
import { CORE_EVENTS, CORE_EVENT_TOPICS } from "./core.js";

/** Returns all Core topic0 values which can change quote-critical state. */
export function quoteCriticalTopics(): readonly Hex.Hex[] {
  return Object.values(CORE_EVENT_TOPICS);
}

/** Returns the two Core topic0 values used to discover active lanes. */
export function laneDiscoveryTopics(): readonly [Hex.Hex, Hex.Hex] {
  return [CORE_EVENT_TOPICS.LaneAdded, CORE_EVENT_TOPICS.LaneRemoved];
}

function expectShape(log: ContractLog, topicCount: number, dataWords: number): void {
  if (log.topics.length !== topicCount) throw new LogDecodeError("INVALID_TOPIC_COUNT", "invalid event topic count");
  if (Hex.size(log.data) !== dataWords * 32)
    throw new LogDecodeError("INVALID_DATA_LENGTH", "invalid event data length");
}

function decode<T>(event: AbiEvent.AbiEvent, log: ContractLog): T {
  try {
    return AbiEvent.decode(event, { topics: log.topics, data: log.data }) as T;
  } catch (error) {
    throw new LogDecodeError(
      "INVALID_DATA_LENGTH",
      `invalid Core event ABI payload: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Decodes a normalized Core event log with strict topic/data arity checks.
 * Unknown topic0 values return `undefined`; malformed known events throw
 * `LogDecodeError` so the reducer fails closed.
 */
export function decodeCoreEvent(log: ContractLog): QuoteEvent | undefined {
  const topic0 = log.topics[0];
  if (topic0 === undefined) throw new LogDecodeError("MISSING_TOPIC0", "event log has no topic0");

  if (topic0 === CORE_EVENT_TOPICS.LaneAdded) {
    expectShape(log, 2, 0);
    const { asset } = decode<{ asset: Address }>(CORE_EVENTS.LaneAdded, log);
    return { kind: "LaneAdded", asset };
  }
  if (topic0 === CORE_EVENT_TOPICS.LaneRemoved) {
    expectShape(log, 2, 0);
    const { asset } = decode<{ asset: Address }>(CORE_EVENTS.LaneRemoved, log);
    return { kind: "LaneRemoved", asset };
  }
  if (topic0 === CORE_EVENT_TOPICS.LaneUpdated) {
    expectShape(log, 2, 1);
    const { asset, slot0 } = decode<{ asset: Address; slot0: Hex.Hex }>(CORE_EVENTS.LaneUpdated, log);
    return { kind: "LaneUpdated", asset, slot0: Hex.toBigInt(slot0) };
  }
  if (topic0 === CORE_EVENT_TOPICS.SlippageKSet) {
    expectShape(log, 2, 2);
    const { asset, newK } = decode<{ asset: Address; newK: number }>(CORE_EVENTS.SlippageKSet, log);
    return { kind: "SlippageKSet", asset, newK: BigInt(newK) };
  }
  if (topic0 === CORE_EVENT_TOPICS.PartnerInfoSet) {
    expectShape(log, 4, 1);
    const { router, asset, fee } = decode<{ router: Address; asset: Address; fee: number }>(
      CORE_EVENTS.PartnerInfoSet,
      log,
    );
    return { kind: "PartnerInfoSet", router, asset, fee: BigInt(fee) };
  }
  if (topic0 === CORE_EVENT_TOPICS.PartnerFeeSet) {
    expectShape(log, 3, 1);
    const { router, asset, fee } = decode<{ router: Address; asset: Address; fee: number }>(
      CORE_EVENTS.PartnerFeeSet,
      log,
    );
    return { kind: "PartnerFeeSet", router, asset, fee: BigInt(fee) };
  }
  if (topic0 === CORE_EVENT_TOPICS.WhitelistSet) {
    expectShape(log, 2, 1);
    const { account, whitelisted } = decode<{ account: Address; whitelisted: boolean }>(CORE_EVENTS.WhitelistSet, log);
    return { kind: "WhitelistSet", router: account, whitelisted };
  }
  if (topic0 === CORE_EVENT_TOPICS.BlacklistFeeMultiplierSet) {
    expectShape(log, 1, 1);
    const { multiplier } = decode<{ multiplier: Word }>(CORE_EVENTS.BlacklistFeeMultiplierSet, log);
    return { kind: "BlacklistFeeMultiplierSet", multiplier };
  }
  if (topic0 === CORE_EVENT_TOPICS.DepositExecuted) {
    expectShape(log, 4, 1);
    const { asset, principalAmount } = decode<{ asset: Address; principalAmount: bigint }>(
      CORE_EVENTS.DepositExecuted,
      log,
    );
    return { kind: "DepositExecuted", asset, principal: principalAmount };
  }
  if (topic0 === CORE_EVENT_TOPICS.WithdrawalExecuted) {
    expectShape(log, 4, 4);
    const { asset, principalAmount } = decode<{ asset: Address; principalAmount: bigint }>(
      CORE_EVENTS.WithdrawalExecuted,
      log,
    );
    return { kind: "WithdrawalExecuted", asset, principal: principalAmount };
  }
  return undefined;
}
