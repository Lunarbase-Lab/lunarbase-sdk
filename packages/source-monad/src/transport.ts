/** Portable Monad parser-WebSocket source with canonical RPC recovery. */
import {
  Commitment,
  Network,
  type BackfillRequest,
  type BootstrapSnapshot,
  type ChainCursor,
  type ChainDataSource,
  type ChainUpdate,
  type Checkpoint,
  type ContractFilter,
  type ContractLog,
  type DeploymentConfig,
} from "@lunarbase-lab/pmm-v2-client";
import {
  BoundedFrameQueue,
  defaultWebSocketFactory,
  parseHash,
  parseRpcLog,
  RpcError,
  RpcHttpBackend,
  RpcSnapshotProvider,
  type JsonRpcHttpClient,
  type SocketEvent,
  type WebSocketFactory,
  type WebSocketLike,
} from "@lunarbase-lab/pmm-v2-source-evm";
import * as Hex from "ox/Hex";
import { MonadExecutionNormalizer, type ExecutionEvent, type ExecutionEventReader } from "./execution.js";

/** Bounded parser-side WebSocket resources. */
export interface MonadParserConfig {
  /** Maximum accepted parser frame size before fail-closed recovery. */
  readonly maxFrameBytes: number;
  /** Maximum decoded parser messages waiting for consumption. */
  readonly queueCapacity: number;
  /** Maximum time for opening and acknowledging both subscriptions. */
  readonly handshakeTimeoutMilliseconds: number;
}

export const DEFAULT_MONAD_PARSER_CONFIG: MonadParserConfig = Object.freeze({
  maxFrameBytes: 64 * 1024,
  queueCapacity: 4096,
  handshakeTimeoutMilliseconds: 10_000,
});

interface EstablishedParserSocket {
  readonly socket: WebSocketLike;
  readonly queue: BoundedFrameQueue;
  readonly logsSubscription: string;
  readonly allSubscription: string;
  readonly prefetched: string[];
  readonly close: () => void;
}

/** Portable Monad parser source with canonical RPC recovery. */
export class MonadParserSource implements ChainDataSource, ExecutionEventReader {
  /** Network family exposed through the common data-source interface. */
  readonly network = Network.Monad;
  /** Latest executed HTTP authority used for backfill and checkpoint validation. */
  private readonly http: RpcHttpBackend;
  /** Coherent block-tagged Core snapshot provider. */
  private readonly snapshots: RpcSnapshotProvider;
  /** Platform or injected WebSocket constructor. */
  private readonly factory: WebSocketFactory;
  /** Validated parser frame and queue bounds. */
  readonly config: MonadParserConfig;

  /** Creates a parser source plus canonical RPC bootstrap/recovery backend. */
  constructor(
    /** Strict read-only HTTP client used for canonical operations. */
    readonly rpc: JsonRpcHttpClient,
    /** Portable parser WebSocket subscription endpoint. */
    readonly wsEndpoint: string,
    /** EIP-155 chain identifier attached to normalized updates. */
    readonly chainId: bigint,
    /** Canonical block tag used for bootstrap and recovery. */
    readonly snapshotTag = "latest",
    config: Partial<MonadParserConfig> = {},
    factory: WebSocketFactory = defaultWebSocketFactory,
  ) {
    this.config = validateConfig({ ...DEFAULT_MONAD_PARSER_CONFIG, ...config });
    this.factory = factory;
    this.http = new RpcHttpBackend(rpc, Network.Monad, chainId, snapshotTag);
    this.snapshots = new RpcSnapshotProvider(rpc, snapshotTag);
  }

  /** Reads one coherent quote state through canonical RPC. */
  snapshot(deployment: DeploymentConfig): Promise<BootstrapSnapshot> {
    if (deployment.network !== Network.Monad)
      return Promise.reject(new RpcError("INVALID", "Monad source network mismatch"));
    return this.snapshots.snapshot(deployment);
  }

  /** Reads canonical Core logs for recovery. */
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]> {
    return this.http.backfill(request);
  }

  /** Returns the canonical RPC head. */
  canonicalHead(): Promise<ChainCursor> {
    return this.http.canonicalHead();
  }

  /** Validates a checkpoint block hash through canonical RPC. */
  validateCheckpoint(checkpoint: Checkpoint): Promise<boolean> {
    return this.http.validateCheckpoint(checkpoint);
  }

  /** Normalizes parser records into the common update stream. */
  async subscribe(filter: ContractFilter, signal?: AbortSignal): Promise<AsyncIterable<ChainUpdate>> {
    const events = await this.subscribeExecution(filter, signal);
    const chainId = this.chainId;
    return (async function* () {
      const normalizer = new MonadExecutionNormalizer(chainId);
      for await (const event of events) {
        const update = normalizer.normalize(event);
        if (update) yield update;
        if (update?.kind === "Gap") return;
      }
    })();
  }

  /** Exposes raw portable parser records for live validation tooling. */
  async subscribeExecution(filter: ContractFilter, signal?: AbortSignal): Promise<AsyncIterable<ExecutionEvent>> {
    const connection = await establishParserSocket(this.factory(this.wsEndpoint), filter, this.config, signal);
    return this.readSocket(connection, filter, signal);
  }

  private async *readSocket(
    connection: EstablishedParserSocket,
    filter: ContractFilter,
    signal?: AbortSignal,
  ): AsyncIterable<ExecutionEvent> {
    const { queue, logsSubscription, allSubscription, prefetched, close } = connection;

    try {
      const commitments = new Map<bigint, Commitment>();

      while (!signal?.aborted) {
        let frame: string | undefined;
        try {
          frame = prefetched.shift() ?? (await queue.next(signal));
        } catch (error) {
          yield executionGap(`Monad parser failed: ${message(error)}`);
          return;
        }
        if (frame === undefined) {
          yield executionGap("Monad parser closed; canonical recovery required");
          return;
        }
        let value: Record<string, unknown>;
        try {
          value = JSON.parse(frame) as Record<string, unknown>;
        } catch (error) {
          yield executionGap(`invalid Monad parser JSON: ${message(error)}`);
          return;
        }
        if (value.error) {
          yield executionGap(`Monad parser subscription error: ${JSON.stringify(value.error)}`);
          return;
        }
        if (value.method === "subscriptionGap") {
          const params =
            value.params && typeof value.params === "object" ? (value.params as Record<string, unknown>) : {};
          yield executionGap(`Monad parser subscription gap; skipped=${parseU64(params.skipped ?? 0, "gap.skipped")}`);
          return;
        }
        if (value.method !== "subscription" || !value.result || typeof value.result !== "object") continue;
        const result = value.result as Record<string, unknown>;
        const type = result.type;
        if (type === "alert" || type === "health") {
          const detail = typeof result.message === "string" ? result.message : "Monad parser unhealthy";
          if (result.stalled === true || isRecoveryAlert(detail)) {
            yield executionGap(detail);
            return;
          }
          continue;
        }
        const subscription = result.subscription;
        if (typeof subscription !== "string") {
          yield executionGap("Monad parser notification has no subscription id");
          return;
        }
        if (subscription === allSubscription && (type === "newHead" || type === "blockStart")) {
          const head = parseParserHead(
            result,
            this.chainId,
            type === "blockStart" ? Commitment.Realtime : parseCommitment(result.commitment),
          );
          commitments.set(head.blockNumber, head.commitment);
          while (commitments.size > 64) commitments.delete(commitments.keys().next().value as bigint);
          yield {
            kind: "Head",
            head: {
              sequence: head.sourceSequence ?? 0n,
              blockNumber: head.blockNumber,
              blockHash: head.blockHash,
              commitment: head.commitment,
            },
          };
          continue;
        }
        if (subscription === logsSubscription && type === "log" && result.kind === "event") {
          const log = parseParserLog(result, this.chainId, commitments);
          if (log.address.toLowerCase() !== filter.address.toLowerCase()) continue;
          if (log.removed) {
            yield executionGap("Monad parser retracted a log; canonical recovery required");
            return;
          }
          yield {
            kind: "Log",
            log: {
              sequence: log.cursor.sourceSequence ?? 0n,
              sourceSubIndex: log.cursor.sourceSubIndex ?? 0n,
              blockNumber: log.cursor.blockNumber,
              blockHash: log.cursor.blockHash,
              transactionIndex: log.cursor.transactionIndex ?? 0n,
              logIndex: log.cursor.logIndex ?? 0n,
              address: log.address,
              topics: log.topics,
              data: log.data,
              commitment: log.cursor.commitment,
            },
          };
        }
      }
    } catch (error) {
      yield executionGap(`invalid Monad parser payload: ${message(error)}`);
    } finally {
      close();
    }
  }
}

async function establishParserSocket(
  socket: WebSocketLike,
  filter: ContractFilter,
  config: MonadParserConfig,
  signal?: AbortSignal,
): Promise<EstablishedParserSocket> {
  const handshakeDeadline = performance.now() + config.handshakeTimeoutMilliseconds;
  const queue = new BoundedFrameQueue(config.queueCapacity);
  const onOpen = () => queue.open();
  const onMessage = (event: SocketEvent) => {
    const frame = decodeFrame(event.data);
    if (frame === undefined) queue.fail(new Error("Monad parser delivered a non-text frame"));
    else if (new TextEncoder().encode(frame).byteLength > config.maxFrameBytes)
      queue.fail(new Error("Monad parser frame exceeded configured bound"));
    else queue.push(frame);
  };
  const onError = (event: SocketEvent) =>
    queue.fail(event.error instanceof Error ? event.error : new Error("Monad parser WebSocket error"));
  const onClose = (event: SocketEvent) => (event.reason ? queue.fail(new Error(event.reason)) : queue.close());
  socket.addEventListener("open", onOpen);
  socket.addEventListener("message", onMessage);
  socket.addEventListener("error", onError);
  socket.addEventListener("close", onClose);
  const abort = () => queue.close();
  signal?.addEventListener("abort", abort, { once: true });
  const close = () => {
    signal?.removeEventListener("abort", abort);
    socket.removeEventListener?.("open", onOpen);
    socket.removeEventListener?.("message", onMessage);
    socket.removeEventListener?.("error", onError);
    socket.removeEventListener?.("close", onClose);
    if (socket.readyState === undefined || socket.readyState < 2) {
      socket.close(1000, "source consumer stopped");
    }
  };
  try {
    if (socket.readyState === 1) queue.open();
    await beforeHandshakeDeadline(queue.waitUntilOpen(signal), handshakeDeadline);
    socket.send(subscriptionRequest(1, "logs", sidecarFilter(filter)));
    socket.send(subscriptionRequest(2, "all"));
    const acknowledgements = await readParserAcknowledgements(queue, config.queueCapacity, handshakeDeadline, signal);
    return { socket, queue, close, ...acknowledgements };
  } catch (error) {
    close();
    throw error;
  }
}

async function readParserAcknowledgements(
  queue: BoundedFrameQueue,
  prefetchCapacity: number,
  handshakeDeadline: number,
  signal?: AbortSignal,
): Promise<Pick<EstablishedParserSocket, "logsSubscription" | "allSubscription" | "prefetched">> {
  const state: ParserHandshakeState = { prefetched: [] };
  while (!state.logsSubscription || !state.allSubscription) {
    const frame = await beforeHandshakeDeadline(queue.next(signal), handshakeDeadline);
    if (frame === undefined) throw new RpcError("TRANSPORT", "Monad parser closed during handshake");
    observeParserHandshakeFrame(state, frame, prefetchCapacity);
  }
  return {
    logsSubscription: state.logsSubscription,
    allSubscription: state.allSubscription,
    prefetched: state.prefetched,
  };
}

interface ParserHandshakeState {
  logsSubscription?: string;
  allSubscription?: string;
  readonly prefetched: string[];
}

function observeParserHandshakeFrame(state: ParserHandshakeState, frame: string, prefetchCapacity: number): void {
  let parsed: unknown;
  try {
    parsed = JSON.parse(frame) as unknown;
  } catch (error) {
    throw new RpcError("INVALID", `invalid Monad parser handshake JSON: ${message(error)}`);
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed))
    throw new RpcError("INVALID", "Monad parser handshake frame is not an object");
  const value = parsed as Record<string, unknown>;
  if (value.error !== undefined && value.error !== null)
    throw new RpcError("TRANSPORT", `Monad parser subscription error: ${JSON.stringify(value.error)}`);

  if (!("id" in value)) {
    if (state.prefetched.length >= prefetchCapacity)
      throw new RpcError("INVALID", "Monad parser handshake prefetch exceeded configured queue capacity");
    state.prefetched.push(frame);
    return;
  }
  if (typeof value.id !== "number" || !Number.isSafeInteger(value.id) || (value.id !== 1 && value.id !== 2))
    throw new RpcError("INVALID", "Monad parser acknowledgement has an unexpected numeric id");
  if (typeof value.result !== "string" || value.result.length === 0)
    throw new RpcError("INVALID", "Monad parser acknowledgement has no subscription id");

  const key = value.id === 1 ? "logsSubscription" : "allSubscription";
  const previous = state[key];
  if (previous !== undefined && previous !== value.result)
    throw new RpcError("INVALID", `Monad parser acknowledgement ${value.id} changed subscription id`);
  state[key] = value.result;
}

function beforeHandshakeDeadline<T>(operation: Promise<T>, deadline: number): Promise<T> {
  const remaining = deadline - performance.now();
  if (remaining <= 0) return Promise.reject(new RpcError("TRANSPORT", "Monad parser subscription handshake timed out"));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new RpcError("TRANSPORT", "Monad parser subscription handshake timed out")),
      remaining,
    );
    operation.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function validateConfig(config: MonadParserConfig): MonadParserConfig {
  for (const [name, value] of Object.entries(config))
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Monad parser ${name} must be positive`);
  return Object.freeze(config);
}

function subscriptionRequest(id: number, kind: "logs" | "all", filter?: Record<string, unknown>): string {
  return JSON.stringify({
    jsonrpc: "2.0",
    id,
    method: "subscribe",
    params: filter ? [kind, filter] : [kind],
  });
}

function sidecarFilter(filter: ContractFilter): Record<string, unknown> {
  const options: Record<string, unknown> = { address: filter.address };
  if (filter.topics.length > 0) options.topics = [filter.topics];
  return options;
}

function parseParserHead(value: Record<string, unknown>, chainId: bigint, commitment: Commitment): ChainCursor {
  const blockNumber = parseU64(value.blockNumber, "head.blockNumber");
  return {
    chainId,
    blockNumber,
    executionBlockNumber: blockNumber,
    blockHash: parserHeadHash(value),
    sourceSequence: parseU64(value.seqno, "head.seqno"),
    commitment,
  };
}

function parserHeadHash(value: Record<string, unknown>): Hex.Hex | undefined {
  const header =
    value.header && typeof value.header === "object" ? (value.header as Record<string, unknown>) : undefined;
  const blockTag =
    header?.blockTag && typeof header.blockTag === "object" ? (header.blockTag as Record<string, unknown>) : undefined;
  const candidate = value.blockHash ?? blockTag?.id;
  return candidate === undefined || candidate === null ? undefined : parseHash(candidate, "head.blockHash");
}

function parseParserLog(
  value: Record<string, unknown>,
  chainId: bigint,
  commitments: ReadonlyMap<bigint, Commitment>,
): ContractLog {
  const blockNumber = parseU64(value.blockNumber, "log.blockNumber");
  const transactionIndex = parseU64(value.transactionIndex, "log.transactionIndex");
  const logIndex = parseU64(value.logIndex, "log.logIndex");
  const blockHash =
    value.blockHash === undefined || value.blockHash === null ? undefined : parseHash(value.blockHash, "log.blockHash");
  const log = parseRpcLog(
    {
      address: value.address,
      topics: value.topics,
      data: value.data,
      blockNumber: hexU64(blockNumber),
      blockHash,
      transactionIndex: hexU64(transactionIndex),
      logIndex: hexU64(logIndex),
      removed: parseRemoved(value.removed),
    },
    chainId,
    commitments.get(blockNumber) ?? Commitment.Realtime,
  );
  log.cursor.sourceSequence = parseU64(value.seqno, "log.seqno");
  log.cursor.sourceSubIndex = logIndex;
  return log;
}

function parseRemoved(value: unknown): boolean {
  if (value === undefined) return false;
  if (typeof value === "boolean") return value;
  throw new RpcError("INVALID", "log.removed is not boolean");
}

function parseCommitment(value: unknown): Commitment {
  if (value === "proposed") return Commitment.Realtime;
  if (value === "finalized") return Commitment.Canonical;
  if (value === "verified") return Commitment.Finalized;
  throw new RpcError("INVALID", "Monad parser commitment is invalid");
}

function parseU64(value: unknown, field: string): bigint {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return BigInt(value);
  if (typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value)) {
    const result = BigInt(value);
    if (result <= (1n << 64n) - 1n) return result;
  }
  throw new RpcError("INVALID", `${field} is not a decimal uint64`);
}

function hexU64(value: bigint): Hex.Hex {
  return Hex.fromNumber(value);
}

function decodeFrame(data: unknown): string | undefined {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data))
    return new TextDecoder().decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
  return undefined;
}

function executionGap(reason: string): ExecutionEvent {
  return { kind: "Gap", reason };
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isRecoveryAlert(value: string): boolean {
  const lower = value.toLowerCase();
  return ["gap", "expired", "stalled", "ring"].some((needle) => lower.includes(needle));
}
