import { assertU256, parseAddress } from "@lunarbase/math";
import type { ChainCursor, ChainUpdate, Commitment, ContractLog } from "./model.js";
import { Commitment as CommitmentValue } from "./model.js";

/** Decode the core normalized-event schema used by replay fixtures. */
export function parseNormalizedUpdate(value: unknown): ChainUpdate {
  const object = record(value, "normalized update");
  switch (object.kind) {
    case "Head":
      return { kind: "Head", cursor: parseCursor(object.cursor) };
    case "Log": {
      const log: ContractLog = {
        address: parseAddress(stringValue(object.address, "log.address")),
        topics: arrayValue(object.topics, "log.topics").map((topic) =>
          assertU256(BigInt(stringValue(topic, "log.topic")), "log.topic"),
        ),
        data: hexData(object.data, "log.data"),
        removed: booleanValue(object.removed, "log.removed"),
        cursor: parseCursor(object.cursor),
      };
      return { kind: "Log", log };
    }
    case "Gap":
      return {
        kind: "Gap",
        cursor: object.cursor === null || object.cursor === undefined ? undefined : parseCursor(object.cursor),
        reason: stringValue(object.reason, "gap.reason"),
      };
    case "SourceHealth":
      return {
        kind: "SourceHealth",
        healthy: booleanValue(object.healthy, "health.healthy"),
        detail: stringValue(object.detail, "health.detail"),
      };
    default:
      throw new Error("normalized update kind is invalid");
  }
}

function parseCursor(value: unknown): ChainCursor {
  const object = record(value, "cursor");
  return {
    chainId: decimalU64(object.chainId, "cursor.chainId"),
    blockNumber: decimalU64(object.blockNumber, "cursor.blockNumber"),
    blockHash: optionalHash(object.blockHash, "cursor.blockHash"),
    transactionIndex: optionalU64(object.transactionIndex, "cursor.transactionIndex"),
    logIndex: optionalU64(object.logIndex, "cursor.logIndex"),
    sourceSequence: optionalU64(object.sourceSequence, "cursor.sourceSequence"),
    sourceSubIndex: optionalU64(object.sourceSubIndex, "cursor.sourceSubIndex"),
    commitment: parseCommitment(object.commitment),
  };
}

function parseCommitment(value: unknown): Commitment {
  if (value === "Realtime") return CommitmentValue.Realtime;
  if (value === "Canonical") return CommitmentValue.Canonical;
  if (value === "Finalized") return CommitmentValue.Finalized;
  throw new Error("cursor.commitment is invalid");
}
function decimalU64(value: unknown, field: string): bigint {
  const text = stringValue(value, field);
  if (!/^(0|[1-9][0-9]*)$/.test(text)) throw new Error(`${field} is not an unsigned decimal`);
  const result = BigInt(text);
  if (result > (1n << 64n) - 1n) throw new Error(`${field} exceeds uint64`);
  return result;
}
function optionalU64(value: unknown, field: string): bigint | undefined {
  return value === null || value === undefined ? undefined : decimalU64(value, field);
}
function optionalHash(value: unknown, field: string): string | undefined {
  if (value === null || value === undefined) return undefined;
  const text = stringValue(value, field);
  if (!/^0x[0-9a-f]{64}$/i.test(text)) throw new Error(`${field} is not bytes32`);
  return text.toLowerCase();
}
function hexData(value: unknown, field: string): string {
  const text = stringValue(value, field);
  if (!/^0x(?:[0-9a-f]{2})*$/i.test(text)) throw new Error(`${field} is not even-length hex`);
  return text.toLowerCase();
}
function record(value: unknown, field: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${field} is not an object`);
  return value as Record<string, unknown>;
}
function arrayValue(value: unknown, field: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(`${field} is not an array`);
  return value;
}
function stringValue(value: unknown, field: string): string {
  if (typeof value !== "string") throw new Error(`${field} is not a string`);
  return value;
}
function booleanValue(value: unknown, field: string): boolean {
  if (typeof value !== "boolean") throw new Error(`${field} is not a boolean`);
  return value;
}
