import type { BackfillRequest, ChainCursor, ChainUpdate, ContractFilter, ContractLog, Network } from "./model.js";
import { Commitment as CommitmentValue, Network as NetworkValue } from "./model.js";
import { MonadExecutionNormalizer } from "./sources.js";
import { BoundedFrameQueue, defaultWebSocketFactory, type SocketEvent, type WebSocketFactory, type WebSocketLike } from "./ws.js";
import { JsonRpcHttpClient, parseHash, parseRpcLog, RpcError, RpcHttpBackend } from "./rpc.js";
import type { NormalizedBackend } from "./sources.js";

export interface MonadSidecarConfig { readonly maxFrameBytes: number; readonly queueCapacity: number; }
export const DEFAULT_MONAD_SIDECAR_CONFIG: MonadSidecarConfig = Object.freeze({ maxFrameBytes: 64 * 1024, queueCapacity: 4096 });

/** Consume the normalized WebSocket protocol exposed by the local Monad parser sidecar. */
export class MonadSidecarBackend implements NormalizedBackend {
  private readonly http: RpcHttpBackend;
  private readonly factory: WebSocketFactory;
  readonly config: MonadSidecarConfig;
  constructor(readonly rpc: JsonRpcHttpClient, readonly wsEndpoint: string, readonly network: Network, readonly chainId: bigint, readonly snapshotTag = "finalized", config: Partial<MonadSidecarConfig> = {}, factory: WebSocketFactory = defaultWebSocketFactory) {
    this.config = validateConfig({ ...DEFAULT_MONAD_SIDECAR_CONFIG, ...config });
    this.factory = factory;
    this.http = new RpcHttpBackend(rpc, network, chainId, snapshotTag);
  }
  snapshotCursor(network: Network): Promise<ChainCursor> { return this.http.snapshotCursor(network); }
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]> { return this.http.backfill(request); }
  subscribe(network: Network, filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    if (network !== NetworkValue.Monad || network !== this.network) throw new RpcError("INVALID", "Monad sidecar backend network mismatch");
    return this.readSocket(this.factory(this.wsEndpoint), filter, signal);
  }

  private async *readSocket(socket: WebSocketLike, filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    const queue = new BoundedFrameQueue(this.config.queueCapacity);
    const onOpen = () => queue.open();
    const onMessage = (event: SocketEvent) => {
      const frame = decodeFrame(event.data);
      if (frame === undefined) queue.fail(new Error("Monad sidecar delivered a non-text frame"));
      else if (new TextEncoder().encode(frame).byteLength > this.config.maxFrameBytes) queue.fail(new Error("Monad sidecar frame exceeded configured bound"));
      else queue.push(frame);
    };
    const onError = (event: SocketEvent) => queue.fail(event.error instanceof Error ? event.error : new Error("Monad sidecar WebSocket error"));
    const onClose = (event: SocketEvent) => event.reason ? queue.fail(new Error(event.reason)) : queue.close();
    socket.addEventListener("open", onOpen); socket.addEventListener("message", onMessage); socket.addEventListener("error", onError); socket.addEventListener("close", onClose);
    const abort = () => queue.close(); signal?.addEventListener("abort", abort, { once: true });
    try {
      if (socket.readyState === 1) queue.open();
      await queue.waitUntilOpen(signal);
      socket.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "subscribe", params: ["logs", sidecarFilter(filter)] }));
      socket.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "subscribe", params: ["all"] }));
      let logsSubscription: string | undefined;
      let allSubscription: string | undefined;
      const normalizer = new MonadExecutionNormalizer(this.chainId);
      const commitments = new Map<bigint, CommitmentValue>();
      while (!signal?.aborted) {
        let frame: string | undefined;
        try { frame = await queue.next(signal); } catch (error) { yield gap(`Monad sidecar failed; canonical recovery required: ${message(error)}`); return; }
        if (frame === undefined) { yield gap("Monad sidecar closed; canonical recovery required"); return; }
        let value: Record<string, unknown>;
        try { value = JSON.parse(frame) as Record<string, unknown>; } catch (error) { yield gap(`invalid Monad sidecar JSON: ${message(error)}`); return; }
        if (value.error) { yield gap(`Monad sidecar subscription error: ${JSON.stringify(value.error)}`); return; }
        if (value.result !== undefined && typeof value.result === "string") {
          if (Number(value.id) === 1) logsSubscription = value.result;
          if (Number(value.id) === 2) allSubscription = value.result;
          continue;
        }
        if (value.method === "subscriptionGap") {
          const reason = `Monad parser subscription gap; skipped=${parseU64((value.params as Record<string, unknown> | undefined)?.skipped ?? 0, "subscriptionGap.skipped")}`;
          yield normalizer.normalizeGap(reason);
          return;
        }
        if (value.method !== "subscription" || !value.result || typeof value.result !== "object") continue;
        const result = value.result as Record<string, unknown>;
        const type = result.type;
        if (type === "alert") {
          const detail = typeof result.message === "string" ? result.message : "Monad parser alert";
          if (isRecoveryAlert(detail)) { yield normalizer.normalizeGap(detail); return; }
          continue;
        }
        if (type === "health") {
          if (result.stalled === true) { yield normalizer.normalizeGap("Monad parser reports stalled reader"); return; }
          continue;
        }
        const subscription = typeof value.params === "object" && value.params !== null ? (value.params as Record<string, unknown>).subscription : undefined;
        if (typeof subscription !== "string") { yield gap("Monad sidecar notification is missing subscription id"); return; }
        if (subscription === allSubscription && (type === "newHead" || type === "blockStart")) {
          let head: ChainCursor;
          try { head = parseSidecarHead(result, this.chainId, type === "blockStart" ? CommitmentValue.Realtime : parseCommitment(result.commitment)); } catch (error) { yield gap(`invalid Monad sidecar head: ${message(error)}`); return; }
          commitments.set(head.blockNumber, head.commitment);
          while (commitments.size > 64) commitments.delete(commitments.keys().next().value as bigint);
          yield { kind: "Head", cursor: head };
          continue;
        }
        if (subscription === logsSubscription && type === "log" && result.kind === "event") {
          let log: ContractLog;
          try { log = parseSidecarLog(result, this.chainId, commitments); } catch (error) { yield gap(`invalid Monad sidecar log: ${message(error)}`); return; }
          if (log.address.toLowerCase() !== filter.address.toLowerCase()) continue;
          const normalized = normalizer.normalizeTxnLog({ sequence: log.cursor.sourceSequence!, sourceSubIndex: log.cursor.sourceSubIndex!, blockNumber: log.cursor.blockNumber, blockHash: log.cursor.blockHash, transactionIndex: log.cursor.transactionIndex!, logIndex: log.cursor.logIndex!, address: log.address, topics: log.topics, data: log.data, commitment: log.cursor.commitment });
          if (normalized) yield normalized;
        }
      }
    } finally {
      signal?.removeEventListener("abort", abort); socket.removeEventListener?.("open", onOpen); socket.removeEventListener?.("message", onMessage); socket.removeEventListener?.("error", onError); socket.removeEventListener?.("close", onClose); if (!signal?.aborted) socket.close(1000, "source consumer stopped");
    }
  }
}

function validateConfig(config: MonadSidecarConfig): MonadSidecarConfig { for (const [name, value] of Object.entries(config)) if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Monad sidecar ${name} must be positive`); return Object.freeze(config); }
function sidecarFilter(filter: ContractFilter): Record<string, unknown> { const options: Record<string, unknown> = { address: filter.address }; if (filter.topics.length > 0) options.topics = filter.topics.map((topic) => `0x${topic.toString(16).padStart(64, "0")}`); return options; }
function parseSidecarHead(value: Record<string, unknown>, chainId: bigint, commitment: CommitmentValue): ChainCursor { const header = value.header; const blockTag = header && typeof header === "object" ? (header as Record<string, unknown>).blockTag : undefined; const blockHash = blockTag && typeof blockTag === "object" && (blockTag as Record<string, unknown>).id !== undefined ? parseHash((blockTag as Record<string, unknown>).id, "head.header.blockTag.id") : undefined; return { chainId, blockNumber: parseU64(value.blockNumber, "head.blockNumber"), blockHash, sourceSequence: parseU64(value.seqno, "head.seqno"), commitment }; }
function parseSidecarLog(value: Record<string, unknown>, chainId: bigint, commitments: ReadonlyMap<bigint, CommitmentValue>): ContractLog { const blockNumber = parseU64(value.blockNumber, "log.blockNumber"); const transactionIndex = parseU64(value.transactionIndex, "log.transactionIndex"); const logIndex = parseU64(value.logIndex, "log.logIndex"); if (transactionIndex > 0xffff_ffffn || logIndex > 0xffff_ffffn) throw new RpcError("INVALID", "Monad log position exceeds uint32"); const blockHash = value.blockHash === undefined || value.blockHash === null ? undefined : parseHash(value.blockHash, "log.blockHash"); const rpcLog = parseRpcLog({ address: value.address, topics: value.topics, data: value.data, blockNumber: hexU64(blockNumber), blockHash, transactionIndex: hexU64(transactionIndex), logIndex: hexU64(logIndex), removed: value.removed === true }, chainId, commitments.get(blockNumber) ?? CommitmentValue.Realtime); rpcLog.cursor.sourceSequence = parseU64(value.seqno, "log.seqno"); rpcLog.cursor.sourceSubIndex = logIndex; return rpcLog; }
function parseCommitment(value: unknown): CommitmentValue { if (value === "proposed") return CommitmentValue.Realtime; if (value === "finalized") return CommitmentValue.Canonical; if (value === "verified") return CommitmentValue.Finalized; throw new RpcError("INVALID", "Monad sidecar commitment is invalid"); }
function parseU64(value: unknown, field: string): bigint { if (typeof value === "number") { if (!Number.isSafeInteger(value) || value < 0) throw new RpcError("INVALID", `${field} is not a safe uint64`); return BigInt(value); } if (typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value)) { const result = BigInt(value); if (result > (1n << 64n) - 1n) throw new RpcError("INVALID", `${field} exceeds uint64`); return result; } throw new RpcError("INVALID", `${field} is not a decimal uint64`); }
function hexU64(value: bigint): string { return `0x${value.toString(16)}`; }
function decodeFrame(data: unknown): string | undefined { if (typeof data === "string") return data; if (data instanceof ArrayBuffer) return new TextDecoder().decode(data); if (ArrayBuffer.isView(data)) return new TextDecoder().decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength)); return undefined; }
function gap(reason: string): ChainUpdate { return { kind: "Gap", reason }; }
function message(error: unknown): string { return error instanceof Error ? error.message : String(error); }
function isRecoveryAlert(value: string): boolean { const lower = value.toLowerCase(); return ["gap", "expired", "stalled", "ring"].some((needle) => lower.includes(needle)); }
