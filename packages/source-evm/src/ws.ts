import {
  Commitment,
  CursorReorderBuffer,
  Network as NetworkValue,
  type BackfillRequest,
  type BootstrapSnapshot,
  type ChainCursor,
  type ChainDataSource,
  type ChainUpdate,
  type Checkpoint,
  type ContractFilter,
  type ContractLog,
  type DeploymentConfig,
  type Network,
} from "@lunarbase-lab/pmm-v2-client";
import { parseAddress, type Address } from "@lunarbase-lab/pmm-v2-math";
import type { Hex } from "ox/Hex";
import { JsonRpcHttpClient, parseRpcLog, RpcError, RpcHttpBackend, RpcSnapshotProvider } from "./rpc.js";
import {
  defaultWebSocketFactory,
  establishSocket,
  type BoundedFrameQueue,
  type EstablishedSocket,
  type WebSocketFactory,
} from "./ws/connection.js";
import { gap, parseHead } from "./ws/protocol.js";

const STANDARD_LOG_GRACE_MILLISECONDS = 2_000;
const STANDARD_HEAD_DEADLINE = Symbol("standard-head-deadline");

type ParsedHead = { cursor: ChainCursor; parentHash?: Hex };
type OpenStandardHead = { readonly observedAt: number; readonly head: ParsedHead };

export { BoundedFrameQueue, defaultWebSocketFactory } from "./ws/connection.js";
export type { SocketEvent, WebSocketFactory, WebSocketLike } from "./ws/connection.js";

/** Resource bounds for generic Ethereum WebSocket ingestion. */
export interface WsRpcConfig {
  /** Maximum accepted WebSocket frame size before fail-closed recovery. */
  readonly maxFrameBytes: number;
  /** Maximum total bytes retained by decoded socket frames. */
  readonly queueByteCapacity: number;
  /** Maximum total bytes retained by notifications received during handshake. */
  readonly prefetchByteCapacity: number;
  /** Maximum updates retained while awaiting an ordering watermark. */
  readonly reorderCapacity: number;
  /** Maximum total bytes retained while awaiting an ordering watermark. */
  readonly reorderByteCapacity: number;
  /** Maximum decoded frames waiting for the socket consumer. */
  readonly queueCapacity: number;
  /** Ethereum subscription method used for Core event logs. */
  readonly logsSubscription: "logs" | "pendingLogs";
  /** Whether multiple sequenced heads at one block height are valid progress. */
  readonly progressiveHeads: boolean;
}

export const DEFAULT_WS_RPC_CONFIG: WsRpcConfig = Object.freeze({
  maxFrameBytes: 256 * 1024,
  queueByteCapacity: 16 * 1024 * 1024,
  prefetchByteCapacity: 16 * 1024 * 1024,
  reorderCapacity: 4096,
  reorderByteCapacity: 64 * 1024 * 1024,
  queueCapacity: 4096,
  logsSubscription: "logs",
  progressiveHeads: false,
});

/**
 * Standard Ethereum JSON-RPC WebSocket source. HTTP remains authoritative
 * for block-tagged bootstrap and canonical backfill; a socket gap is emitted
 * instead of being silently hidden by reconnecting from an unknown cursor.
 */
export class EvmRpcSource implements ChainDataSource {
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
    if (deployment.chainId !== this.chainId)
      return Promise.reject(new RpcError("INVALID", "RPC source chain id mismatch"));
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
    const expectedAddress = parseAddress(filter.address);
    const connection = establishSocket(
      this.wsEndpoint,
      this.factory,
      filter,
      this.config.logsSubscription,
      this.chainId,
      this.config.queueCapacity,
      this.config.queueByteCapacity,
      this.config.prefetchByteCapacity,
      this.config.maxFrameBytes,
      signal,
    );
    try {
      const [established] = await Promise.all([connection, this.http.verifyChainId()]);
      return this.readSocket(established, expectedAddress, signal);
    } catch (error) {
      void connection.then(
        (established) => established.close(),
        () => undefined,
      );
      throw error;
    }
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
    connection: EstablishedSocket,
    expectedAddress: Address,
    signal?: AbortSignal,
  ): AsyncIterable<ChainUpdate> {
    const { queue, logsSubscription, headsSubscription, prefetched, close } = connection;
    let lastHead: ChainCursor | undefined;
    let lastParentHash: Hex | undefined;
    let sourceSequence = 0n;
    let reorder = new CursorReorderBuffer(this.config.reorderCapacity, this.config.reorderByteCapacity);
    const standardLogs = holdsStandardLogsUntilSuccessor(this.config);
    let openStandardHeads: OpenStandardHead[] = [];
    let firstStandardHeadBlock: bigint | undefined;
    let publishedWatermark: ChainCursor | undefined;

    try {
      while (!signal?.aborted) {
        let frame: string | undefined | typeof STANDARD_HEAD_DEADLINE;
        try {
          frame =
            prefetched.shift() ??
            (await nextFrameOrStandardDeadline(
              queue,
              standardLogs ? standardHeadDeadline(openStandardHeads) : undefined,
              signal,
            ));
        } catch (error) {
          yield gap(`RPC WebSocket failed; canonical recovery required: ${message(error)}`, lastHead);
          return;
        }
        if (frame === STANDARD_HEAD_DEADLINE) {
          const completedHead = takeReadyStandardHead(openStandardHeads, monotonicMilliseconds());
          if (completedHead === undefined) continue;
          let updates: ChainUpdate[];
          try {
            updates = drainCompletedBlock(
              reorder,
              completedHead,
              firstStandardHeadBlock === completedHead.cursor.blockNumber,
            );
            updates = await validatePrecedingStartupLogs(
              updates,
              completedHead,
              this.rpc,
              this.chainId,
              this.network === NetworkValue.Arbitrum,
            );
          } catch (error) {
            yield gap(`RPC completed block failed; canonical recovery required: ${message(error)}`, lastHead);
            return;
          }
          for (const update of updates) yield update;
          publishedWatermark = completedHead.cursor;
          continue;
        }
        if (frame === undefined) {
          if (signal?.aborted) return;
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
          if (log.address !== expectedAddress) {
            yield gap(`RPC log address mismatch: expected ${expectedAddress}, got ${log.address}`, log.cursor);
            return;
          }
          sourceSequence += 1n;
          log = { ...log, cursor: { ...log.cursor, sourceSequence } };
          if (log.removed) {
            yield gap("RPC retracted a subscription log; canonical recovery required", log.cursor);
            return;
          }
          if (standardLogs && isAtOrBeforeWatermark(log.cursor, publishedWatermark)) {
            yield gap("RPC delivered a log after its block watermark; canonical recovery required", log.cursor);
            return;
          }
          if (lastHead?.blockNumber === log.cursor.blockNumber)
            log = withExecutionContext(log, lastHead.executionBlockNumber);
          try {
            reorder.push({ kind: "Log", log });
          } catch (error) {
            yield gap(`RPC reorder buffer failed: ${message(error)}`, lastHead);
            return;
          }
          if (!standardLogs && lastHead)
            for (const update of reorder.drainThrough(lastHead)) yield annotateExecutionContext(update, lastHead);
          continue;
        }

        if (subscription !== headsSubscription) continue;
        let parsed: ParsedHead;
        try {
          parsed = parseHead(params.result, this.chainId, this.network === NetworkValue.Arbitrum);
        } catch (error) {
          yield gap(`invalid RPC head notification: ${message(error)}`, lastHead);
          return;
        }
        if (lastHead && sameHead(lastHead, lastParentHash, parsed)) continue;
        sourceSequence += 1n;
        parsed.cursor = { ...parsed.cursor, sourceSequence };
        if (standardLogs && firstStandardHeadBlock === undefined) firstStandardHeadBlock = parsed.cursor.blockNumber;
        if (lastHead && parsed.cursor.blockNumber > lastHead.blockNumber + 1n) {
          yield gap("RPC WebSocket skipped one or more block heads; canonical recovery required", lastHead);
          return;
        }
        if (lastHead && headDiscontinuity(lastHead, lastParentHash, parsed, this.config.progressiveHeads)) {
          yield { kind: "Reorg", oldHead: lastHead, newHead: parsed.cursor };
          reorder = new CursorReorderBuffer(this.config.reorderCapacity, this.config.reorderByteCapacity);
          openStandardHeads = [];
          firstStandardHeadBlock = parsed.cursor.blockNumber;
        }
        lastHead = parsed.cursor;
        lastParentHash = parsed.parentHash;

        if (standardLogs) {
          try {
            observeStandardHead(
              openStandardHeads,
              parsed,
              monotonicMilliseconds(),
              this.config.reorderCapacity,
              this.config.reorderByteCapacity,
            );
          } catch (error) {
            yield gap(`RPC pending heads failed: ${message(error)}`, lastHead);
            return;
          }
        }

        try {
          reorder.push({ kind: "Head", cursor: parsed.cursor });
        } catch (error) {
          yield gap(`RPC reorder buffer failed: ${message(error)}`, lastHead);
          return;
        }
        if (!standardLogs)
          for (const update of reorder.drainThrough(parsed.cursor))
            yield annotateExecutionContext(update, parsed.cursor);
      }
    } finally {
      close();
    }
  }
}

function validateConfig(config: WsRpcConfig): WsRpcConfig {
  for (const [name, value] of Object.entries({
    maxFrameBytes: config.maxFrameBytes,
    queueByteCapacity: config.queueByteCapacity,
    prefetchByteCapacity: config.prefetchByteCapacity,
    reorderCapacity: config.reorderCapacity,
    reorderByteCapacity: config.reorderByteCapacity,
    queueCapacity: config.queueCapacity,
  }))
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`WS ${name} must be a positive safe integer`);
  if (config.logsSubscription !== "logs" && config.logsSubscription !== "pendingLogs")
    throw new Error("WS logsSubscription must be logs or pendingLogs");
  return Object.freeze(config);
}

function sameHead(lastHead: ChainCursor, lastParentHash: Hex | undefined, next: ParsedHead): boolean {
  return (
    next.cursor.blockNumber === lastHead.blockNumber &&
    next.cursor.executionBlockNumber === lastHead.executionBlockNumber &&
    next.cursor.blockHash === lastHead.blockHash &&
    next.parentHash === lastParentHash
  );
}

function headDiscontinuity(
  lastHead: ChainCursor,
  lastParentHash: Hex | undefined,
  next: ParsedHead,
  progressiveHeads: boolean,
): boolean {
  if (sameHead(lastHead, lastParentHash, next)) return false;
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

function holdsStandardLogsUntilSuccessor(config: WsRpcConfig): boolean {
  return config.logsSubscription === "logs";
}

function isAtOrBeforeWatermark(cursor: ChainCursor, watermark: ChainCursor | undefined): boolean {
  return watermark !== undefined && cursor.blockNumber <= watermark.blockNumber;
}

function observeStandardHead(
  openHeads: OpenStandardHead[],
  head: ParsedHead,
  observedAt: number,
  countCapacity: number,
  byteCapacity: number,
): void {
  const nextCount = openHeads.length + 1;
  if (nextCount > countCapacity || nextCount * 256 > byteCapacity)
    throw new Error("pending head count or byte budget exceeded");
  openHeads.push({ observedAt, head });
}

function standardHeadDeadline(openHeads: readonly OpenStandardHead[]): number | undefined {
  const successor = openHeads[1];
  return successor === undefined ? undefined : successor.observedAt + STANDARD_LOG_GRACE_MILLISECONDS;
}

function takeReadyStandardHead(openHeads: OpenStandardHead[], observedAt: number): ParsedHead | undefined {
  const deadline = standardHeadDeadline(openHeads);
  if (deadline === undefined || observedAt < deadline) return undefined;
  return openHeads.shift()?.head;
}

async function nextFrameOrStandardDeadline(
  queue: BoundedFrameQueue,
  deadline: number | undefined,
  signal?: AbortSignal,
): Promise<string | undefined | typeof STANDARD_HEAD_DEADLINE> {
  if (deadline === undefined) return queue.next(signal);
  if (signal?.aborted) return undefined;

  const wait = new AbortController();
  const abort = () => wait.abort();
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) wait.abort();
  let deadlineElapsed = false;
  const timer = setTimeout(
    () => {
      deadlineElapsed = true;
      wait.abort();
    },
    Math.max(0, deadline - monotonicMilliseconds()),
  );
  try {
    const frame = await queue.next(wait.signal);
    return deadlineElapsed ? STANDARD_HEAD_DEADLINE : frame;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
  }
}

function drainCompletedBlock(
  reorder: CursorReorderBuffer,
  head: ParsedHead,
  allowPrecedingStartupLogs: boolean,
): ChainUpdate[] {
  const blockHash = head.cursor.blockHash;
  if (blockHash === undefined) throw new Error("completed RPC head has no block hash");

  const logs: ChainUpdate[] = [];
  let completedHead: ChainUpdate | undefined;
  for (const update of reorder.drainThrough(head.cursor)) {
    if (
      update.kind === "Head" &&
      update.cursor.blockNumber === head.cursor.blockNumber &&
      sameHash(update.cursor.blockHash, blockHash)
    ) {
      completedHead = update;
      continue;
    }
    if (
      update.kind === "Log" &&
      update.log.cursor.blockNumber === head.cursor.blockNumber &&
      sameHash(update.log.cursor.blockHash, blockHash)
    ) {
      logs.push(annotateExecutionContext(update, head.cursor));
      continue;
    }
    if (
      update.kind === "Log" &&
      allowPrecedingStartupLogs &&
      update.log.cursor.blockNumber < head.cursor.blockNumber &&
      update.log.cursor.blockHash !== undefined
    ) {
      logs.push(update);
      continue;
    }
    if (update.kind === "Gap") {
      const block = update.cursor?.blockNumber ?? 0n;
      throw new Error(`buffered gap at block ${block} cannot complete RPC block ${head.cursor.blockNumber}`);
    }
    const kind = update.kind.toLowerCase();
    const cursor = update.kind === "Log" ? update.log.cursor : update.kind === "Reorg" ? update.newHead : update.cursor;
    throw new Error(
      `buffered RPC ${kind} at block ${cursor.blockNumber} hash ${String(cursor.blockHash)} does not match completed block ${head.cursor.blockNumber} hash ${blockHash}`,
    );
  }
  if (completedHead === undefined) throw new Error("completed RPC block has no matching buffered head");
  return [...logs, completedHead];
}

async function validatePrecedingStartupLogs(
  updates: ChainUpdate[],
  completedHead: ParsedHead,
  rpc: JsonRpcHttpClient,
  chainId: bigint,
  requireExecutionBlockNumber: boolean,
): Promise<ChainUpdate[]> {
  const canonicalBlocks = new Map<bigint, ChainCursor>();
  for (let index = 0; index < updates.length; index += 1) {
    const update = updates[index];
    if (update?.kind !== "Log" || update.log.cursor.blockNumber >= completedHead.cursor.blockNumber) continue;
    const blockNumber = update.log.cursor.blockNumber;
    let canonical = canonicalBlocks.get(blockNumber);
    if (canonical === undefined) {
      canonical = await rpc.blockCursor(
        `0x${blockNumber.toString(16)}`,
        chainId,
        Commitment.Canonical,
        requireExecutionBlockNumber,
      );
      canonicalBlocks.set(blockNumber, canonical);
    }
    if (!sameHash(canonical.blockHash, update.log.cursor.blockHash))
      throw new Error(`startup RPC log at block ${blockNumber} does not match its canonical block hash`);
    updates[index] = {
      kind: "Log",
      log: withExecutionContext(update.log, canonical.executionBlockNumber),
    };
  }
  return updates;
}

function sameHash(left: Hex | undefined, right: Hex | undefined): boolean {
  return left !== undefined && right !== undefined && left.toLowerCase() === right.toLowerCase();
}

function monotonicMilliseconds(): number {
  return globalThis.performance.now();
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
