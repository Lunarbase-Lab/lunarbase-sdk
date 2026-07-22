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
import { Commitment } from "../model.js";
import { CursorReorderBuffer } from "../state/ordering.js";
import { JsonRpcHttpClient, parseRpcLog, RpcError, RpcHttpBackend, RpcSnapshotProvider } from "./rpc.js";
import {
  defaultWebSocketFactory,
  establishSocket,
  type EstablishedSocket,
  type WebSocketFactory,
} from "./ws/connection.js";
import { gap, parseHead } from "./ws/protocol.js";

export { BoundedFrameQueue, defaultWebSocketFactory } from "./ws/connection.js";
export type { SocketEvent, WebSocketFactory, WebSocketLike } from "./ws/connection.js";

/** Resource bounds for generic Ethereum WebSocket ingestion. */
export interface WsRpcConfig {
  /** Maximum accepted WebSocket frame size before fail-closed recovery. */
  readonly maxFrameBytes: number;
  /** Maximum updates retained while awaiting an ordering watermark. */
  readonly reorderCapacity: number;
  /** Maximum decoded frames waiting for the socket consumer. */
  readonly queueCapacity: number;
  /** Ethereum subscription method used for Core event logs. */
  readonly logsSubscription: "logs" | "pendingLogs";
  /** Whether multiple sequenced heads at one block height are valid progress. */
  readonly progressiveHeads: boolean;
}

export const DEFAULT_WS_RPC_CONFIG: WsRpcConfig = Object.freeze({
  maxFrameBytes: 256 * 1024,
  reorderCapacity: 4096,
  queueCapacity: 4096,
  logsSubscription: "logs",
  progressiveHeads: false,
});

/**
 * Standard Ethereum JSON-RPC WebSocket source. HTTP remains authoritative
 * for block-tagged bootstrap and canonical backfill; a socket gap is emitted
 * instead of being silently hidden by reconnecting from an unknown cursor.
 */
export class WsRpcBackend implements ChainDataSource {
  /** Canonical HTTP backend used for heads, backfill, and validation. */
  private readonly http: RpcHttpBackend;
  /** Coherent block-tagged Core snapshot provider. */
  private readonly snapshots: RpcSnapshotProvider;
  /** Platform or injected WebSocket constructor. */
  private readonly factory: WebSocketFactory;
  /** Validated frame, queue, ordering, and subscription limits. */
  readonly config: WsRpcConfig;

  /** Creates a WebSocket backend with bounded frame, queue, and reorder memory. */
  constructor(
    /** Strict read-only HTTP client used outside the quote path. */
    readonly rpc: JsonRpcHttpClient,
    /** WebSocket endpoint used exclusively for realtime subscriptions. */
    readonly wsEndpoint: string,
    /** Network family exposed through the common source interface. */
    readonly network: Network,
    /** EIP-155 chain identifier attached to normalized cursors. */
    readonly chainId: bigint,
    /** Canonical block tag used for snapshot and recovery reads. */
    readonly snapshotTag = "latest",
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

  /**
   * Opens and acknowledges logs/new-head subscriptions before exposing the
   * stream. A client therefore cannot become ready while the socket is merely
   * connecting or while the node has rejected either subscription.
   */
  async subscribe(filter: ContractFilter, signal?: AbortSignal): Promise<AsyncIterable<ChainUpdate>> {
    const connection = await establishSocket(
      this.wsEndpoint,
      this.factory,
      filter,
      this.config.logsSubscription,
      this.chainId,
      this.config.queueCapacity,
      this.config.maxFrameBytes,
      signal,
    );
    return this.readSocket(connection, signal);
  }

  /** Returns the canonical HTTP recovery head. */
  canonicalHead(): Promise<ChainCursor> {
    return this.http.canonicalHead();
  }

  /** Verifies a checkpoint block hash through canonical HTTP RPC. */
  validateCheckpoint(checkpoint: Checkpoint): Promise<boolean> {
    return this.http.validateCheckpoint(checkpoint);
  }

  private async *readSocket(connection: EstablishedSocket, signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    const { queue, logsSubscription, headsSubscription, prefetched, close } = connection;
    let lastHead: ChainCursor | undefined;
    let lastParentHash: Hex | undefined;
    let sourceSequence = 0n;
    let reorder = new CursorReorderBuffer(this.config.reorderCapacity);

    try {
      while (!signal?.aborted) {
        let frame: string | undefined;
        try {
          frame = prefetched.shift() ?? (await queue.next(signal));
        } catch (error) {
          yield gap(`RPC WebSocket failed; canonical recovery required: ${message(error)}`, lastHead);
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
          yield gap(`invalid RPC WebSocket JSON; canonical recovery required: ${message(error)}`, lastHead);
          return;
        }
        if (value.error) {
          yield gap(`RPC subscription error: ${JSON.stringify(value.error)}`, lastHead);
          return;
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
            log = parseRpcLog(params.result, this.chainId, Commitment.Realtime);
          } catch (error) {
            yield gap(`invalid RPC log notification: ${message(error)}`, lastHead);
            return;
          }
          sourceSequence += 1n;
          log = { ...log, cursor: { ...log.cursor, sourceSequence } };
          if (lastHead?.blockNumber === log.cursor.blockNumber)
            log = withExecutionContext(log, lastHead.executionBlockNumber);
          try {
            reorder.push({ kind: "Log", log });
          } catch (error) {
            yield gap(`RPC reorder buffer failed: ${message(error)}`, lastHead);
            return;
          }
          if (lastHead)
            for (const update of reorder.drainThrough(lastHead)) yield annotateExecutionContext(update, lastHead);
          continue;
        }

        if (subscription !== headsSubscription) continue;
        let parsed: { cursor: ChainCursor; parentHash?: Hex };
        try {
          parsed = parseHead(params.result, this.chainId);
        } catch (error) {
          yield gap(`invalid RPC head notification: ${message(error)}`, lastHead);
          return;
        }
        sourceSequence += 1n;
        parsed.cursor = { ...parsed.cursor, sourceSequence };
        if (lastHead && headDiscontinuity(lastHead, lastParentHash, parsed, this.config.progressiveHeads)) {
          yield { kind: "Reorg", oldHead: lastHead, newHead: parsed.cursor };
          reorder = new CursorReorderBuffer(this.config.reorderCapacity);
        }
        lastHead = parsed.cursor;
        lastParentHash = parsed.parentHash;
        try {
          reorder.push({ kind: "Head", cursor: parsed.cursor });
        } catch (error) {
          yield gap(`RPC reorder buffer failed: ${message(error)}`, lastHead);
          return;
        }
        for (const update of reorder.drainThrough(parsed.cursor)) yield annotateExecutionContext(update, parsed.cursor);
      }
    } finally {
      close();
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

function headDiscontinuity(
  lastHead: ChainCursor,
  lastParentHash: Hex | undefined,
  next: { cursor: ChainCursor; parentHash?: Hex },
  progressiveHeads: boolean,
): boolean {
  if (next.cursor.blockNumber > lastHead.blockNumber + 1n) return true;
  const sameHeight = next.cursor.blockNumber === lastHead.blockNumber;
  const changedParent =
    next.parentHash !== undefined && lastParentHash !== undefined && next.parentHash !== lastParentHash;
  return (
    next.cursor.blockNumber < lastHead.blockNumber ||
    (sameHeight && (!progressiveHeads || changedParent)) ||
    (next.cursor.blockNumber === lastHead.blockNumber + 1n &&
      next.parentHash !== undefined &&
      lastHead.blockHash !== undefined &&
      next.parentHash !== lastHead.blockHash)
  );
}

function annotateExecutionContext(update: ChainUpdate, head: ChainCursor): ChainUpdate {
  if (update.kind !== "Log" || update.log.cursor.blockNumber !== head.blockNumber) return update;
  return { kind: "Log", log: withExecutionContext(update.log, head.executionBlockNumber) };
}

function withExecutionContext(log: ContractLog, executionBlockNumber: bigint): ContractLog {
  return { ...log, cursor: { ...log.cursor, executionBlockNumber } };
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
