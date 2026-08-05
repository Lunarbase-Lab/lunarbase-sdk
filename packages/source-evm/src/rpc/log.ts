import { parseAddress, type Address } from "@lunarbase-lab/pmm-v2-math";
import type { ChainCursor, ContractLog } from "@lunarbase-lab/pmm-v2-client";
import * as Hex from "ox/Hex";
import { formatLog, type RpcLog } from "viem";
import { RpcError } from "./error.js";

/** Parses one raw JSON-RPC log after validating every consensus-relevant field. */
export function parseRpcLog(value: unknown, chainId: bigint, commitment: ChainCursor["commitment"]): ContractLog {
  try {
    validateRawRpcLog(value);
    return normalizeViemLog(formatLog(value), chainId, commitment);
  } catch (error) {
    if (error instanceof RpcError) throw error;
    throw new RpcError("INVALID", error instanceof Error ? error.message : "invalid RPC log");
  }
}

function validateRawRpcLog(value: unknown): asserts value is RpcLog {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new RpcError("INVALID", "RPC log is not an object");
  const log = value as Record<string, unknown>;
  parseRpcAddress(log.address, "log.address");
  parseRpcTopics(log.topics, "log.topics");
  parseRpcData(log.data, "log.data");
  parseHexU64(log.blockNumber, "log.blockNumber");
  parseRawLogIndex(log.transactionIndex, "log.transactionIndex");
  parseRawLogIndex(log.logIndex, "log.logIndex");
  if (log.blockHash !== null && log.blockHash !== undefined) parseHash(log.blockHash, "log.blockHash");
  if (log.transactionHash !== null && log.transactionHash !== undefined)
    parseHash(log.transactionHash, "log.transactionHash");
  if (typeof log.removed !== "boolean") throw new RpcError("INVALID", "log.removed is not boolean");
}

export function normalizeViemLog(
  log: ReturnType<typeof formatLog>,
  chainId: bigint,
  commitment: ChainCursor["commitment"],
): ContractLog {
  const blockNumber = parseNormalizedBlockNumber(log.blockNumber, "log.blockNumber");
  return {
    address: parseRpcAddress(log.address, "log.address"),
    topics: parseRpcTopics(log.topics, "log.topics"),
    data: parseRpcData(log.data, "log.data"),
    removed: parseRpcRemoved(log.removed),
    cursor: {
      chainId,
      blockNumber,
      executionBlockNumber: blockNumber,
      blockHash:
        log.blockHash === null || log.blockHash === undefined ? undefined : parseHash(log.blockHash, "log.blockHash"),
      transactionIndex: parseNormalizedLogIndex(log.transactionIndex, "log.transactionIndex"),
      logIndex: parseNormalizedLogIndex(log.logIndex, "log.logIndex"),
      commitment,
    },
  };
}

function parseRpcAddress(value: unknown, field: string): Address {
  if (typeof value !== "string") throw new RpcError("INVALID", field + " is not an address");
  try {
    return parseAddress(value);
  } catch {
    throw new RpcError("INVALID", field + " is not an address");
  }
}

function parseRpcTopics(value: unknown, field: string): readonly Hex.Hex[] {
  if (!Array.isArray(value) || value.length > 4) throw new RpcError("INVALID", field + " is not a valid topic array");
  return value.map((topic, index) => parseHash(topic, field + "[" + index + "]"));
}

function parseRpcData(value: unknown, field: string): Hex.Hex {
  if (typeof value !== "string" || !Hex.validate(value, { strict: true }) || (value.length - 2) % 2 !== 0)
    throw new RpcError("INVALID", field + " is not valid bytes");
  return value.toLowerCase() as Hex.Hex;
}

function parseRpcRemoved(value: unknown): boolean {
  if (typeof value !== "boolean") throw new RpcError("INVALID", "log.removed is not boolean");
  return value;
}

function parseRawLogIndex(value: unknown, field: string): bigint {
  const index = parseHexU64(value, field);
  if (index > 0xffff_ffffn) throw new RpcError("INVALID", field + " exceeds uint32");
  return index;
}

function parseNormalizedLogIndex(value: unknown, field: string): bigint {
  if (!Number.isSafeInteger(value) || Number(value) < 0 || Number(value) > 0xffff_ffff)
    throw new RpcError("INVALID", field + " is not a safe uint32");
  return BigInt(Number(value));
}

function parseNormalizedBlockNumber(value: unknown, field: string): bigint {
  if (typeof value !== "bigint" || value < 0n || value > (1n << 64n) - 1n)
    throw new RpcError("INVALID", field + " is not uint64");
  return value;
}

/** Parses an unsigned hexadecimal uint64 RPC field through Ox. */
export function parseHexU64(value: unknown, field: string): bigint {
  if (typeof value !== "string" || !/^0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)$/.test(value))
    throw new RpcError("INVALID", `${field} is not a canonical hex quantity`);
  const result = Hex.toBigInt(value as Hex.Hex);
  if (result > (1n << 64n) - 1n) throw new RpcError("INVALID", `${field} exceeds uint64`);
  return result;
}

/** Parses a canonical 32-byte hash through Ox. */
export function parseHash(value: unknown, field: string): Hex.Hex {
  if (typeof value !== "string" || value.length !== 66 || !Hex.validate(value, { strict: true }))
    throw new RpcError("INVALID", `${field} is not bytes32`);
  return value.toLowerCase() as Hex.Hex;
}
