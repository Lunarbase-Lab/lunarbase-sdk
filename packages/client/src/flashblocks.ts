import type { BackfillRequest, ChainCursor, ChainUpdate, ContractFilter, ContractLog, Network } from "./model.js";
import { Commitment as CommitmentValue, Network as NetworkValue } from "./model.js";
import { BaseFlashblocksNormalizer, type FlashblockHeader, type FlashblockLog } from "./sources.js";
import { CursorReorderBuffer } from "./ordering.js";
import { BoundedFrameQueue, defaultWebSocketFactory, type SocketEvent, type WebSocketFactory, type WebSocketLike } from "./ws.js";
import { JsonRpcHttpClient, parseHash, parseHexU64, parseRpcLog, RpcError, RpcHttpBackend } from "./rpc.js";
import type { NormalizedBackend } from "./sources.js";

export interface BaseFlashblocksConfig { readonly maxFrameBytes: number; readonly reorderCapacity: number; readonly queueCapacity: number; }
export const DEFAULT_BASE_FLASHBLOCKS_CONFIG: BaseFlashblocksConfig = Object.freeze({ maxFrameBytes: 512 * 1024, reorderCapacity: 4096, queueCapacity: 4096 });

/** Base preconfirmation source using the documented pendingLogs/newFlashblocks subscriptions. */
export class BaseFlashblocksBackend implements NormalizedBackend {
  private readonly http: RpcHttpBackend;
  private readonly factory: WebSocketFactory;
  readonly config: BaseFlashblocksConfig;
  constructor(readonly rpc: JsonRpcHttpClient, readonly wsEndpoint: string, readonly network: Network, readonly chainId: bigint, readonly snapshotTag = "finalized", config: Partial<BaseFlashblocksConfig> = {}, factory: WebSocketFactory = defaultWebSocketFactory) {
    this.config = validateConfig({ ...DEFAULT_BASE_FLASHBLOCKS_CONFIG, ...config });
    this.factory = factory;
    this.http = new RpcHttpBackend(rpc, network, chainId, snapshotTag);
  }
  snapshotCursor(network: Network): Promise<ChainCursor> { return this.http.snapshotCursor(network); }
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]> { return this.http.backfill(request); }
  subscribe(network: Network, filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> { if (network !== this.network || network !== NetworkValue.Base) throw new RpcError("INVALID", "Base Flashblocks backend network mismatch"); return this.readSocket(this.factory(this.wsEndpoint), filter, signal); }

  private async *readSocket(socket: WebSocketLike, filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    const queue = new BoundedFrameQueue(this.config.queueCapacity);
    const onOpen = () => queue.open();
    const onMessage = (event: SocketEvent) => { const frame = decodeFrame(event.data); if (frame === undefined) queue.fail(new Error("Base Flashblocks delivered a non-text frame")); else if (new TextEncoder().encode(frame).byteLength > this.config.maxFrameBytes) queue.fail(new Error("Base Flashblocks frame exceeded configured bound")); else queue.push(frame); };
    const onError = (event: SocketEvent) => queue.fail(event.error instanceof Error ? event.error : new Error("Base Flashblocks WebSocket error"));
    const onClose = (event: SocketEvent) => event.reason ? queue.fail(new Error(event.reason)) : queue.close();
    socket.addEventListener("open", onOpen); socket.addEventListener("message", onMessage); socket.addEventListener("error", onError); socket.addEventListener("close", onClose);
    const abort = () => queue.close(); signal?.addEventListener("abort", abort, { once: true });
    try {
      if (socket.readyState === 1) queue.open();
      await queue.waitUntilOpen(signal);
      socket.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "eth_subscribe", params: ["pendingLogs", filterOptions(filter)] }));
      socket.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "eth_subscribe", params: ["newFlashblocks"] }));
      let pendingSubscription: string | undefined;
      let flashblocksSubscription: string | undefined;
      const normalizer = new BaseFlashblocksNormalizer(this.chainId);
      const headers = new Map<bigint, FlashblockHeader[]>();
      let reorder = new CursorReorderBuffer(this.config.reorderCapacity);
      while (!signal?.aborted) {
        let frame: string | undefined;
        try { frame = await queue.next(signal); } catch (error) { yield gap(`Base Flashblocks failed; canonical recovery required: ${message(error)}`); return; }
        if (frame === undefined) { yield gap("Base Flashblocks closed; canonical recovery required"); return; }
        let value: Record<string, unknown>;
        try { value = JSON.parse(frame) as Record<string, unknown>; } catch (error) { yield gap(`invalid Base Flashblocks JSON: ${message(error)}`); return; }
        if (value.error) { yield gap(`Base Flashblocks subscription error: ${JSON.stringify(value.error)}`); return; }
        if (value.id !== undefined && typeof value.result === "string") { if (Number(value.id) === 1) pendingSubscription = value.result; if (Number(value.id) === 2) flashblocksSubscription = value.result; continue; }
        if (value.method !== "eth_subscription" || !value.params || typeof value.params !== "object") continue;
        const params = value.params as Record<string, unknown>; const subscription = typeof params.subscription === "string" ? params.subscription : undefined; const result = params.result;
        if (!subscription || result === undefined) { yield gap("Base Flashblocks notification is missing subscription/result"); return; }
        if (subscription === flashblocksSubscription) {
          let payloadId: string;
          try { payloadId = parsePayloadId(result); } catch (error) { yield gap(`invalid Base Flashblock payload: ${message(error)}`); return; }
          const previous = [...headers.values()].flat().find((header) => header.payloadId.toLowerCase() === payloadId.toLowerCase())?.blockNumber;
          let header: FlashblockHeader;
          try { header = parseFlashblockHeader(result, previous); } catch (error) { yield gap(`invalid Base Flashblock payload: ${message(error)}`); return; }
          const previousBlock = headers.keys().next().value as bigint | undefined;
          if (previousBlock !== undefined && header.blockNumber > previousBlock + 1n) { yield gap("Base Flashblocks skipped one or more block payloads; canonical recovery required"); return; }
          const items = headers.get(header.blockNumber) ?? []; items.push(header); if (items.length > 128) items.shift(); headers.set(header.blockNumber, items); while (headers.size > 64) headers.delete(headers.keys().next().value as bigint);
          const head = normalizer.normalizeHeader(header);
          if (head) { try { reorder.push(head); } catch (error) { yield gap(`Base Flashblocks reorder failed: ${message(error)}`); return; } for (const update of reorder.drainThrough(headerCursor(header, this.chainId))) yield update; }
          continue;
        }
        if (subscription === pendingSubscription) {
          let log: ContractLog;
          try { log = parseRpcLog(result, this.chainId, CommitmentValue.Realtime); } catch (error) { yield gap(`invalid Base pending log: ${message(error)}`); return; }
          const header = selectHeader(headers.get(log.cursor.blockNumber), log.cursor.blockHash);
          if (!header) { yield { kind: "Gap", cursor: log.cursor, reason: "pending log arrived without a matching Flashblock header" }; return; }
          const flashblockLog: FlashblockLog = { header, transactionIndex: log.cursor.transactionIndex ?? 0n, logIndex: log.cursor.logIndex ?? 0n, address: log.address, topics: log.topics, data: log.data, removed: log.removed };
          let updates: ChainUpdate[];
          try { updates = normalizer.normalizeLog(flashblockLog); } catch (error) { yield gap(`Base Flashblocks normalization failed: ${message(error)}`); return; }
          try { for (const update of updates) reorder.push(update); } catch (error) { yield gap(`Base Flashblocks reorder failed: ${message(error)}`); return; }
          const latest = headers.get(log.cursor.blockNumber)?.at(-1); if (latest) for (const update of reorder.drainThrough(headerCursor(latest, this.chainId))) yield update;
        }
      }
    } finally {
      signal?.removeEventListener("abort", abort); socket.removeEventListener?.("open", onOpen); socket.removeEventListener?.("message", onMessage); socket.removeEventListener?.("error", onError); socket.removeEventListener?.("close", onClose); if (!signal?.aborted) socket.close(1000, "source consumer stopped");
    }
  }
}

function validateConfig(config: BaseFlashblocksConfig): BaseFlashblocksConfig { for (const [name, value] of Object.entries(config)) if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Base Flashblocks ${name} must be positive`); return Object.freeze(config); }
function filterOptions(filter: ContractFilter): Record<string, unknown> { const options: Record<string, unknown> = { address: filter.address }; if (filter.topics.length > 0) options.topics = filter.topics.map((topic) => `0x${topic.toString(16).padStart(64, "0")}`); return options; }
function parsePayloadId(value: unknown): string { if (!value || typeof value !== "object") throw new RpcError("INVALID", "Flashblock result is not an object"); const payload = (value as Record<string, unknown>).payload_id; if (typeof payload !== "string" || !/^0x[0-9a-f]+$/i.test(payload) || payload.length > 66 || payload.length % 2 !== 0) throw new RpcError("INVALID", "invalid Flashblock payload_id"); return payload.toLowerCase(); }
function parseFlashblockHeader(value: unknown, previousBlockNumber?: bigint): FlashblockHeader {
  if (!value || typeof value !== "object") throw new RpcError("INVALID", "Flashblock result is not an object");
  const object = value as Record<string, unknown>;
  const payloadId = parsePayloadId(value);
  const indexValue = object.index;
  let index: bigint;
  if (typeof indexValue === "number") {
    if (!Number.isSafeInteger(indexValue) || indexValue < 0) throw new RpcError("INVALID", "invalid Flashblock index");
    index = BigInt(indexValue);
  } else if (typeof indexValue === "string" && /^0x[0-9a-f]+$/i.test(indexValue)) {
    index = BigInt(indexValue);
  } else if (typeof indexValue === "string" && /^(0|[1-9][0-9]*)$/.test(indexValue)) {
    index = BigInt(indexValue);
  } else {
    throw new RpcError("INVALID", "invalid Flashblock index");
  }
  const diff = object.diff;
  if (!diff || typeof diff !== "object") throw new RpcError("INVALID", "Flashblock diff is missing");
  const hash = parseHash((diff as Record<string, unknown>).block_hash, "Flashblock diff.block_hash");
  const base = object.base;
  const blockNumber = base && typeof base === "object" ? parseHexU64((base as Record<string, unknown>).block_number, "Flashblock base.block_number") : previousBlockNumber;
  if (blockNumber === undefined) throw new RpcError("INVALID", "index > 0 requires observed index-0 block context");
  return { payloadId, blockNumber, blockHash: hash, index };
}
function headerCursor(header: FlashblockHeader, chainId: bigint): ChainCursor { return { chainId, blockNumber: header.blockNumber, blockHash: header.blockHash, sourceSequence: header.index, commitment: CommitmentValue.Realtime }; }
function selectHeader(headers: FlashblockHeader[] | undefined, blockHash?: string): FlashblockHeader | undefined { if (!headers) return undefined; return (blockHash ? headers.slice().reverse().find((header) => header.blockHash?.toLowerCase() === blockHash.toLowerCase()) : undefined) ?? headers.at(-1); }
function decodeFrame(data: unknown): string | undefined { if (typeof data === "string") return data; if (data instanceof ArrayBuffer) return new TextDecoder().decode(data); if (ArrayBuffer.isView(data)) return new TextDecoder().decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength)); return undefined; }
function gap(reason: string): ChainUpdate { return { kind: "Gap", reason }; }
function message(error: unknown): string { return error instanceof Error ? error.message : String(error); }
