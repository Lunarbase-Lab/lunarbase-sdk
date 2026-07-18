import type {
  BackfillRequest,
  BootstrapSnapshot,
  ChainCursor,
  ChainDataSource,
  ChainUpdate,
  Checkpoint,
  ContractFilter,
  ContractLog,
  DeploymentConfig,
  Network,
} from "../model.js";
import type { Hex } from "ox/Hex";
import { Commitment as CommitmentValue } from "../model.js";
import { CursorReorderBuffer } from "../state/ordering.js";
import {
  JsonRpcHttpClient,
  parseHash,
  parseHexU64,
  parseRpcLog,
  RpcError,
  RpcHttpBackend,
  RpcSnapshotProvider,
} from "./rpc.js";

/** Resource bounds for generic Ethereum WebSocket ingestion. */
export interface WsRpcConfig {
  readonly maxFrameBytes: number;
  readonly reorderCapacity: number;
  readonly queueCapacity: number;
  readonly logsSubscription: "logs" | "pendingLogs";
  readonly progressiveHeads: boolean;
}

export const DEFAULT_WS_RPC_CONFIG: WsRpcConfig = Object.freeze({
  maxFrameBytes: 256 * 1024,
  reorderCapacity: 4096,
  queueCapacity: 4096,
  logsSubscription: "logs",
  progressiveHeads: false,
});

export interface WebSocketLike {
  readonly readyState?: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open" | "message" | "error" | "close", listener: (event: SocketEvent) => void): void;
  removeEventListener?(type: "open" | "message" | "error" | "close", listener: (event: SocketEvent) => void): void;
}

export type SocketEvent = { data?: unknown; error?: unknown; reason?: string };
/** WebSocket factory abstraction used for browser, Node, and test transports. */
export type WebSocketFactory = (url: string) => WebSocketLike;

/**
 * Standard Ethereum JSON-RPC WebSocket source. HTTP remains authoritative
 * for block-tagged bootstrap and canonical backfill; a socket gap is emitted
 * instead of being silently hidden by reconnecting from an unknown cursor.
 */
export class WsRpcBackend implements ChainDataSource {
  private readonly http: RpcHttpBackend;
  private readonly snapshots: RpcSnapshotProvider;
  private readonly factory: WebSocketFactory;
  readonly config: WsRpcConfig;

  /** Creates a WebSocket backend with bounded frame, queue, and reorder memory. */
  constructor(
    readonly rpc: JsonRpcHttpClient,
    readonly wsEndpoint: string,
    readonly network: Network,
    readonly chainId: bigint,
    readonly snapshotTag = "finalized",
    config: Partial<WsRpcConfig> = {},
    factory: WebSocketFactory = defaultWebSocketFactory,
  ) {
    this.config = validateConfig({ ...DEFAULT_WS_RPC_CONFIG, ...config });
    this.factory = factory;
    this.http = new RpcHttpBackend(rpc, network, chainId, snapshotTag);
    this.snapshots = new RpcSnapshotProvider(rpc, snapshotTag);
  }

  /** Reads one coherent quote state through block-tagged HTTP calls. */
  snapshot(deployment: DeploymentConfig): Promise<BootstrapSnapshot> {
    if (deployment.network !== this.network)
      return Promise.reject(new RpcError("INVALID", "RPC source network mismatch"));
    return this.snapshots.snapshot(deployment);
  }

  /** Delegates canonical log backfill to HTTP. */
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]> {
    return this.http.backfill(request);
  }

  /** Opens logs and new-head subscriptions and emits normalized updates/gaps. */
  subscribe(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    const config = this.config;
    const socket = this.factory(this.wsEndpoint);
    return this.readSocket(socket, filter, config, signal);
  }

  /** Returns the canonical HTTP recovery head. */
  canonicalHead(): Promise<ChainCursor> {
    return this.http.canonicalHead();
  }

  /** Verifies a checkpoint block hash through canonical HTTP RPC. */
  validateCheckpoint(checkpoint: Checkpoint): Promise<boolean> {
    return this.http.validateCheckpoint(checkpoint);
  }

  private async *readSocket(
    socket: WebSocketLike,
    filter: ContractFilter,
    config: WsRpcConfig,
    signal?: AbortSignal,
  ): AsyncIterable<ChainUpdate> {
    const queue = new BoundedFrameQueue(config.queueCapacity);
    const onOpen = () => queue.open();
    const onMessage = (event: SocketEvent) => {
      const frame = decodeFrame(event.data);
      if (frame === undefined) {
        queue.fail(new Error("RPC WebSocket delivered a non-text frame"));
      } else if (new TextEncoder().encode(frame).byteLength > config.maxFrameBytes) {
        queue.fail(new Error("RPC WebSocket frame exceeded configured bound"));
      } else {
        queue.push(frame);
      }
    };
    const onError = (event: SocketEvent) =>
      queue.fail(event.error instanceof Error ? event.error : new Error("RPC WebSocket error"));
    const onClose = (event: SocketEvent) => (event.reason ? queue.fail(new Error(event.reason)) : queue.close());
    socket.addEventListener("open", onOpen);
    socket.addEventListener("message", onMessage);
    socket.addEventListener("error", onError);
    socket.addEventListener("close", onClose);

    const cleanup = () => {
      socket.removeEventListener?.("open", onOpen);
      socket.removeEventListener?.("message", onMessage);
      socket.removeEventListener?.("error", onError);
      socket.removeEventListener?.("close", onClose);
      if (!signal?.aborted) socket.close(1000, "source consumer stopped");
    };
    const abort = () => queue.close();
    signal?.addEventListener("abort", abort, { once: true });

    try {
      if (socket.readyState === 1) queue.open();
      await queue.waitUntilOpen(signal);
      socket.send(subscriptionRequest(1, filter, config.logsSubscription));
      socket.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "eth_subscribe", params: ["newHeads"] }));

      let logsSubscription: string | undefined;
      let headsSubscription: string | undefined;
      let lastHead: ChainCursor | undefined;
      let lastParentHash: Hex | undefined;
      let sourceSequence = 0n;
      let reorder = new CursorReorderBuffer(config.reorderCapacity);

      while (!signal?.aborted) {
        let frame: string | undefined;
        try {
          frame = await queue.next(signal);
        } catch (error) {
          yield gap(
            `RPC WebSocket failed; canonical recovery required: ${error instanceof Error ? error.message : String(error)}`,
            lastHead,
          );
          return;
        }
        if (frame === undefined) {
          yield gap("RPC WebSocket closed; canonical recovery required", lastHead);
          return;
        }
        let value: Record<string, unknown>;
        try {
          value = JSON.parse(frame) as Record<string, unknown>;
        } catch (error) {
          yield gap(
            `invalid RPC WebSocket JSON; canonical recovery required: ${error instanceof Error ? error.message : String(error)}`,
            lastHead,
          );
          return;
        }
        if (value.error) {
          yield gap(`RPC subscription error: ${JSON.stringify(value.error)}`, lastHead);
          return;
        }
        if (value.id !== undefined && typeof value.result === "string") {
          if (Number(value.id) === 1) logsSubscription = value.result;
          if (Number(value.id) === 2) headsSubscription = value.result;
          continue;
        }
        if (value.method !== "eth_subscription" || !value.params || typeof value.params !== "object") continue;
        const params = value.params as Record<string, unknown>;
        const subscription = typeof params.subscription === "string" ? params.subscription : undefined;
        if (!subscription || params.result === undefined) {
          yield gap("RPC subscription notification is missing subscription/result", lastHead);
          return;
        }

        if (subscription === logsSubscription) {
          let log: ContractLog;
          try {
            log = parseRpcLog(params.result, this.chainId, CommitmentValue.Realtime);
          } catch (error) {
            yield gap(
              `invalid RPC log notification: ${error instanceof Error ? error.message : String(error)}`,
              lastHead,
            );
            return;
          }
          sourceSequence += 1n;
          log = { ...log, cursor: { ...log.cursor, sourceSequence } };
          try {
            reorder.push({ kind: "Log", log });
          } catch (error) {
            yield gap(`RPC reorder buffer failed: ${error instanceof Error ? error.message : String(error)}`, lastHead);
            return;
          }
          if (lastHead) for (const update of reorder.drainThrough(lastHead)) yield update;
          continue;
        }

        if (subscription === headsSubscription) {
          let parsed: { cursor: ChainCursor; parentHash?: Hex };
          try {
            parsed = parseHead(params.result, this.chainId);
          } catch (error) {
            yield gap(
              `invalid RPC head notification: ${error instanceof Error ? error.message : String(error)}`,
              lastHead,
            );
            return;
          }
          sourceSequence += 1n;
          parsed.cursor = { ...parsed.cursor, sourceSequence };
          if (lastHead) {
            if (parsed.cursor.blockNumber > lastHead.blockNumber + 1n) {
              yield gap("RPC WebSocket skipped one or more block heads; canonical recovery required", lastHead);
              return;
            }
            const sameHeight = parsed.cursor.blockNumber === lastHead.blockNumber;
            const sameHeightDiscontinuity =
              sameHeight &&
              (!config.progressiveHeads ||
                (parsed.parentHash !== undefined &&
                  lastParentHash !== undefined &&
                  parsed.parentHash !== lastParentHash));
            const discontinuity =
              parsed.cursor.blockNumber < lastHead.blockNumber ||
              sameHeightDiscontinuity ||
              (parsed.cursor.blockNumber === lastHead.blockNumber + 1n &&
                parsed.parentHash !== undefined &&
                lastHead.blockHash !== undefined &&
                parsed.parentHash !== lastHead.blockHash);
            if (discontinuity) {
              yield { kind: "Reorg", oldHead: lastHead, newHead: parsed.cursor };
              reorder = new CursorReorderBuffer(config.reorderCapacity);
            }
          }
          lastHead = parsed.cursor;
          lastParentHash = parsed.parentHash;
          try {
            reorder.push({ kind: "Head", cursor: parsed.cursor });
          } catch (error) {
            yield gap(`RPC reorder buffer failed: ${error instanceof Error ? error.message : String(error)}`, lastHead);
            return;
          }
          for (const update of reorder.drainThrough(parsed.cursor)) yield update;
        }
      }
    } finally {
      signal?.removeEventListener("abort", abort);
      cleanup();
    }
  }
}

function validateConfig(config: WsRpcConfig): WsRpcConfig {
  for (const [name, value] of Object.entries({
    maxFrameBytes: config.maxFrameBytes,
    reorderCapacity: config.reorderCapacity,
    queueCapacity: config.queueCapacity,
  }))
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`WS ${name} must be a positive safe integer`);
  return Object.freeze(config);
}

function subscriptionRequest(id: number, filter: ContractFilter, kind: "logs" | "pendingLogs"): string {
  const options: Record<string, unknown> = { address: filter.address };
  if (filter.topics.length > 0) options.topics = [filter.topics];
  return JSON.stringify({ jsonrpc: "2.0", id, method: "eth_subscribe", params: [kind, options] });
}

function parseHead(value: unknown, chainId: bigint): { cursor: ChainCursor; parentHash?: Hex } {
  if (!value || typeof value !== "object") throw new RpcError("INVALID", "newHeads result is not an object");
  const object = value as Record<string, unknown>;
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
      commitment: CommitmentValue.Realtime,
    },
    parentHash,
  };
}

function gap(reason: string, cursor?: ChainCursor): ChainUpdate {
  return { kind: "Gap", cursor, reason };
}

function decodeFrame(data: unknown): string | undefined {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data))
    return new TextDecoder().decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
  return undefined;
}

/** Creates the platform WebSocket or throws an actionable injection error. */
export function defaultWebSocketFactory(url: string): WebSocketLike {
  const constructor = (globalThis as typeof globalThis & { WebSocket?: new (url: string) => WebSocketLike }).WebSocket;
  if (!constructor) throw new RpcError("INVALID", "global WebSocket is unavailable; inject a WebSocketFactory");
  return new constructor(url);
}

export class BoundedFrameQueue {
  private readonly values: string[] = [];
  private readonly waiters: Array<(value: string | undefined) => void> = [];
  private opened = false;
  private ended = false;
  private failure?: Error;

  /** Creates a bounded frame queue for one socket reader. */
  constructor(private readonly capacity: number) {}

  /** Marks the socket open and releases open waiters. */
  open(): void {
    this.opened = true;
    this.resolveWaiters();
  }

  /** Enqueues one frame or fails closed on overflow. */
  push(value: string): void {
    if (this.ended) return;
    if (this.values.length >= this.capacity) {
      this.fail(new Error("RPC WebSocket frame queue overflow; canonical recovery required"));
      return;
    }
    this.values.push(value);
    this.resolveWaiters();
  }

  /** Terminates the queue with a transport error. */
  fail(error: Error): void {
    if (this.ended) return;
    this.failure = error;
    this.ended = true;
    this.resolveWaiters();
  }

  /** Terminates the queue normally. */
  close(): void {
    if (this.ended) return;
    this.ended = true;
    this.resolveWaiters();
  }

  /** Waits for socket open or propagates close/failure. */
  async waitUntilOpen(signal?: AbortSignal): Promise<void> {
    if (this.opened) return;
    await this.wait(signal);
    if (!this.opened) throw this.failure ?? new Error("RPC WebSocket closed before open");
  }

  /** Reads the next frame, waiting until data or terminal state exists. */
  async next(signal?: AbortSignal): Promise<string | undefined> {
    if (this.failure) throw this.failure;
    const value = this.values.shift();
    if (value !== undefined) return value;
    if (this.ended) return undefined;
    return this.wait(signal);
  }

  private wait(signal?: AbortSignal): Promise<string | undefined> {
    if (signal?.aborted) return Promise.resolve(undefined);
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        signal?.removeEventListener("abort", onAbort);
        resolve(undefined);
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      this.waiters.push((value) => {
        signal?.removeEventListener("abort", onAbort);
        if (this.failure) reject(this.failure);
        else resolve(value);
      });
    });
  }

  private resolveWaiters(): void {
    while (this.waiters.length > 0 && (this.values.length > 0 || this.ended || this.opened)) {
      const waiter = this.waiters.shift();
      waiter?.(this.values.shift());
      if (!this.values.length && !this.ended && !this.opened) break;
    }
  }
}
