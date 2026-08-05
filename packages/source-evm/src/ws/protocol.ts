/** Ethereum subscription requests and normalized head parsing. */
import { Commitment, type ChainCursor, type ChainUpdate, type ContractFilter } from "@lunarbase-lab/pmm-v2-client";
import type { Hex } from "ox/Hex";
import { parseHash, parseHexU64, RpcError } from "../rpc.js";

export function subscriptionRequest(id: number, filter: ContractFilter, kind: "logs" | "pendingLogs"): string {
  const options: Record<string, unknown> = { address: filter.address };
  if (filter.topics.length > 0) options.topics = [filter.topics];
  return JSON.stringify({ jsonrpc: "2.0", id, method: "eth_subscribe", params: [kind, options] });
}

export function parseHead(
  value: unknown,
  chainId: bigint,
  requireExecutionBlockNumber = false,
): { cursor: ChainCursor; parentHash?: Hex } {
  if (!value || typeof value !== "object") throw new RpcError("INVALID", "newHeads result is not an object");
  const object = value as Record<string, unknown>;
  if (requireExecutionBlockNumber && (object.l1BlockNumber === undefined || object.l1BlockNumber === null))
    throw new RpcError("INVALID", "newHeads result has no l1BlockNumber");
  const blockHash = object.hash === null || object.hash === undefined ? undefined : parseHash(object.hash, "head.hash");
  const parentHash =
    object.parentHash === null || object.parentHash === undefined
      ? undefined
      : parseHash(object.parentHash, "head.parentHash");
  return {
    cursor: {
      chainId,
      blockNumber: parseHexU64(object.number, "head.number"),
      executionBlockNumber:
        object.l1BlockNumber === undefined || object.l1BlockNumber === null
          ? parseHexU64(object.number, "head.number")
          : parseHexU64(object.l1BlockNumber, "head.l1BlockNumber"),
      blockHash,
      commitment: Commitment.Realtime,
    },
    parentHash,
  };
}

export function gap(reason: string, cursor?: ChainCursor): ChainUpdate {
  return { kind: "Gap", cursor, reason };
}
