/** Connected source/reducer lifecycle for embeddable TypeScript clients. */
import { parseAddress, type QuoteRequest } from "@lunarbase-lab/pmm-v2-math";
import { checkpointMatchesDeployment, validateDeploymentConfig } from "../bootstrap.js";
import { IndexerError } from "../model.js";
import { ownContractFilter, ownDeploymentConfig } from "../ownership.js";
import type {
  ChainDataSource,
  Checkpoint,
  ClientBatchQuote,
  ClientQuote,
  ContractFilter,
  DeploymentConfig,
  IndexerHealth,
} from "../model.js";
import { compareCursor } from "../source.js";
import { QuoteIndexer, validateCoreLogIdentity } from "./engine.js";
import { validateFilterTopics } from "./filter.js";
import { delay, pumpSource, SourceActivity } from "./source_task.js";
import { BoundedUpdateQueue } from "./update_queue.js";
import { monotonicMilliseconds, withDeadline } from "./lifecycle.js";

const RECOVERY_PAGE_BLOCKS = 1_000n;
const DEFAULT_SHUTDOWN_TIMEOUT_MILLISECONDS = 10_000;

/** Runtime-only configuration; persistence belongs to the Rust indexer. */
export interface ClientConnectConfig {
  /** Immutable network, Core contract, router, and endpoint identity. */
  readonly deployment: DeploymentConfig;
  /** Core address; topics are empty or the complete quote-critical set. */
  readonly filter: ContractFilter;
  /** Maximum normalized updates waiting for the ordered reducer. */
  readonly queueBound: number;
  /** Maximum retained bytes waiting for the ordered reducer. */
  readonly queueByteBound: number;
  /** Delay before reopening a failed realtime subscription. */
  readonly reconnectDelayMilliseconds: number;
  /** Maximum interval without an update before readiness is revoked. */
  readonly sourceStallTimeoutMilliseconds: number;
  /** Maximum duration of one source handshake, snapshot, or recovery operation. */
  readonly sourceOperationTimeoutMilliseconds: number;
}

/** High-level embeddable client with one active ordered reducer. */
export class ConnectedQuoteClient {
  private constructor(
    /** Mutable quote state owned by the single ordered reducer. */
    private readonly indexer: QuoteIndexer,
    /** Cooperative cancellation signal for source and reducer loops. */
    private readonly sourceController: AbortController,
    /** Reducer cancellation is delayed until accepted updates are drained. */
    private readonly reducerController: AbortController,
    /** Bounded handoff between asynchronous ingestion and ordered reduction. */
    private readonly queue: BoundedUpdateQueue,
    /** Acknowledged-subscription state shared with recovery. */
    private readonly activity: SourceActivity,
    /** Serialized recovery owner used by reducer and explicit callers. */
    private readonly recovery: RecoveryCoordinator,
    /** Realtime subscription task retained for deterministic shutdown. */
    private readonly pumpTask: Promise<void>,
    /** Ordered reducer task retained for deterministic shutdown. */
    private readonly reducerTask: Promise<void>,
  ) {}

  private stopping = false;
  private shutdownTask?: Promise<void>;

  /**
   * Connects and acknowledges a subscription before loading checkpoint or
   * snapshot state, preserving every update that races with bootstrap.
   */
  static async connect(
    config: ClientConnectConfig,
    source: ChainDataSource,
    optionalCheckpoint?: Checkpoint,
  ): Promise<ConnectedQuoteClient> {
    validateConnectConfig(config, source);
    const ownedConfig = ownConnectConfig(config);
    const sourceController = new AbortController();
    const reducerController = new AbortController();
    const queue = new BoundedUpdateQueue(ownedConfig.queueBound, ownedConfig.queueByteBound);
    const activity = new SourceActivity();
    const pumpTask = pumpSource(
      source,
      ownedConfig.filter,
      queue,
      activity,
      sourceController.signal,
      ownedConfig.reconnectDelayMilliseconds,
      ownedConfig.sourceStallTimeoutMilliseconds,
      ownedConfig.sourceOperationTimeoutMilliseconds,
    );
    try {
      const active = await waitForActivity(
        "initial source subscription",
        activity,
        ownedConfig.sourceOperationTimeoutMilliseconds,
        sourceController.signal,
      );
      if (!active) throw new IndexerError("SOURCE", "realtime source stopped before handshake");
      const indexer = await bootstrapIndexer(
        ownedConfig,
        source,
        queue,
        activity,
        sourceController.signal,
        optionalCheckpoint,
      );
      const recovery = new RecoveryCoordinator(indexer, source, ownedConfig, queue, activity, sourceController.signal);
      const reducerTask = reduceSource(
        indexer,
        queue,
        recovery,
        reducerController.signal,
        () => sourceController.signal.aborted,
      ).catch(() => {
        indexer.markNotReady();
        sourceController.abort();
        reducerController.abort();
        queue.close();
        activity.setActive(false);
      });
      return new ConnectedQuoteClient(
        indexer,
        sourceController,
        reducerController,
        queue,
        activity,
        recovery,
        pumpTask,
        reducerTask,
      );
    } catch (error) {
      sourceController.abort();
      reducerController.abort();
      queue.close();
      activity.setActive(false);
      await withDeadline("failed connect cleanup", ownedConfig.sourceOperationTimeoutMilliseconds, undefined, () =>
        Promise.allSettled([pumpTask]).then(() => undefined),
      ).catch(() => undefined);
      throw error;
    }
  }

  /** Computes one fully offchain quote. */
  quote(request: QuoteRequest): ClientQuote {
    this.requireRunning();
    return this.indexer.quote(request);
  }

  /** Computes a batch on one cursor without yielding to the event loop. */
  quoteMany(requests: readonly QuoteRequest[]): ClientBatchQuote {
    this.requireRunning();
    return this.indexer.quoteMany(requests);
  }

  /** Returns current readiness and cursor metadata. */
  health(): IndexerHealth {
    const health = this.indexer.health();
    return this.stopping ? { ...health, ready: false } : health;
  }

  /** Returns a checkpoint only while the current state is publishable. */
  checkpoint(): Checkpoint | undefined {
    return !this.stopping && this.indexer.health().ready ? this.indexer.checkpoint() : undefined;
  }

  /** Forces one serialized canonical recovery while subscription buffering continues. */
  recover(): Promise<void> {
    this.requireRunning();
    return this.recovery.run();
  }

  /** Stops all background work and revokes readiness. */
  shutdown(timeoutMilliseconds = DEFAULT_SHUTDOWN_TIMEOUT_MILLISECONDS): Promise<void> {
    if (!Number.isSafeInteger(timeoutMilliseconds) || timeoutMilliseconds <= 0)
      return Promise.reject(new IndexerError("SOURCE", "shutdown timeout must be a positive safe integer"));
    if (this.shutdownTask) return this.shutdownTask;
    this.stopping = true;
    this.sourceController.abort();
    this.activity.setActive(false);
    this.indexer.markNotReady();
    this.shutdownTask = this.finishShutdown(timeoutMilliseconds);
    return this.shutdownTask;
  }

  private async finishShutdown(timeoutMilliseconds: number): Promise<void> {
    const deadline = monotonicMilliseconds() + timeoutMilliseconds;
    let pumpError: unknown;
    try {
      await withDeadline("source pump shutdown", remaining(deadline), undefined, () => this.pumpTask);
    } catch (error) {
      pumpError = error;
    }
    this.queue.close();
    try {
      await withDeadline("reducer drain", remaining(deadline), undefined, () => this.reducerTask);
    } finally {
      this.reducerController.abort();
      this.queue.close();
    }
    if (pumpError) throw pumpError;
  }

  private requireRunning(): void {
    if (this.stopping) throw new IndexerError("NOT_READY", "client is shutting down");
  }
}

/** Functional constructor matching the public embeddable API. */
export function connect(
  config: ClientConnectConfig,
  source: ChainDataSource,
  optionalCheckpoint?: Checkpoint,
): Promise<ConnectedQuoteClient> {
  return ConnectedQuoteClient.connect(config, source, optionalCheckpoint);
}

function validateConnectConfig(config: ClientConnectConfig, source: ChainDataSource): void {
  validateDeploymentConfig(config.deployment);
  if (source.network !== config.deployment.network) throw new IndexerError("SOURCE", "source network mismatch");
  if (config.filter.address.toLowerCase() !== config.deployment.core.toLowerCase())
    throw new IndexerError("SOURCE", "filter must target deployment Core");
  validateFilterTopics(config.filter.topics);
  for (const [name, value] of Object.entries({
    queueBound: config.queueBound,
    queueByteBound: config.queueByteBound,
    reconnectDelayMilliseconds: config.reconnectDelayMilliseconds,
    sourceStallTimeoutMilliseconds: config.sourceStallTimeoutMilliseconds,
    sourceOperationTimeoutMilliseconds: config.sourceOperationTimeoutMilliseconds,
  }))
    if (!Number.isSafeInteger(value) || value <= 0)
      throw new IndexerError("SOURCE", `${name} must be a positive safe integer`);
  if (config.queueByteBound < 1024) throw new IndexerError("SOURCE", "queueByteBound must be at least 1024 bytes");
}

function ownConnectConfig(config: ClientConnectConfig): ClientConnectConfig {
  return Object.freeze({
    ...config,
    deployment: ownDeploymentConfig(config.deployment),
    filter: ownContractFilter(config.filter),
  });
}

async function reduceSource(
  indexer: QuoteIndexer,
  queue: BoundedUpdateQueue,
  recovery: RecoveryCoordinator,
  signal: AbortSignal,
  stopping: () => boolean,
): Promise<void> {
  let stateValid = true;
  while (!signal.aborted) {
    const update = await queue.next(signal);
    if (!update) return;
    if (!stateValid) continue;
    try {
      indexer.applyCoreUpdate(update);
    } catch {
      if (stopping()) {
        indexer.markNotReady();
        stateValid = false;
      } else {
        await recovery.run();
      }
    }
  }
}

async function bootstrapIndexer(
  config: ClientConnectConfig,
  source: ChainDataSource,
  queue: BoundedUpdateQueue,
  activity: SourceActivity,
  signal: AbortSignal,
  optionalCheckpoint?: Checkpoint,
): Promise<QuoteIndexer> {
  const active = await waitForActivity(
    "bootstrap source activity",
    activity,
    config.sourceOperationTimeoutMilliseconds,
    signal,
  );
  if (!active) throw new IndexerError("SOURCE", "bootstrap cancelled before source became active");
  if (optionalCheckpoint && checkpointMatchesDeployment(optionalCheckpoint, config.deployment)) {
    const valid = await withDeadline("checkpoint validation", config.sourceOperationTimeoutMilliseconds, signal, () =>
      source.validateCheckpoint(optionalCheckpoint),
    );
    if (valid) {
      try {
        const restored = await restoreCheckpoint(optionalCheckpoint, config, source, signal);
        restored.replayHandoff(queue.drainAll(), restored.health().cursor ?? optionalCheckpoint.cursor);
        return restored;
      } catch (error) {
        if (isPermanentBootstrapError(error)) throw error;
      }
    }
  }
  return snapshotIndexer(config, source, queue, signal);
}

async function restoreCheckpoint(
  checkpoint: Checkpoint,
  config: ClientConnectConfig,
  source: ChainDataSource,
  signal: AbortSignal,
): Promise<QuoteIndexer> {
  const indexer = QuoteIndexer.fromCheckpoint(checkpoint, config.deployment);
  const head = await withDeadline("checkpoint canonical head", config.sourceOperationTimeoutMilliseconds, signal, () =>
    source.canonicalHead(),
  );
  if (head.chainId !== checkpoint.cursor.chainId || head.blockNumber < checkpoint.cursor.blockNumber)
    throw new IndexerError("GAP", "checkpoint is ahead of canonical head");
  const fromBlock =
    checkpoint.cursor.transactionIndex === undefined && checkpoint.cursor.logIndex === undefined
      ? checkpoint.cursor.blockNumber + 1n
      : checkpoint.cursor.blockNumber;
  if (fromBlock <= head.blockNumber) {
    const expectedCore = parseAddress(config.deployment.core);
    let pageStart = fromBlock;
    while (pageStart <= head.blockNumber) {
      const candidateEnd = pageStart + RECOVERY_PAGE_BLOCKS - 1n;
      const pageEnd = candidateEnd < head.blockNumber ? candidateEnd : head.blockNumber;
      const received = [
        ...(await withDeadline("checkpoint backfill", config.sourceOperationTimeoutMilliseconds, signal, () =>
          source.backfill({ fromBlock: pageStart, toBlock: pageEnd, filter: config.filter }),
        )),
      ];
      for (const log of received) {
        validateCoreLogIdentity(log, expectedCore, config.deployment.chainId);
        if (
          log.removed ||
          log.cursor.blockNumber < pageStart ||
          log.cursor.blockNumber > pageEnd ||
          log.cursor.blockHash === undefined
        )
          throw new IndexerError("GAP", "canonical recovery backfill returned an invalid log");
      }
      received.sort((left, right) => compareCursor(left.cursor, right.cursor));
      for (const log of received)
        if (compareCursor(log.cursor, checkpoint.cursor) > 0) indexer.applyCoreUpdate({ kind: "Log", log });
      pageStart = pageEnd + 1n;
    }
  }
  indexer.applyCoreUpdate({ kind: "Head", head: { cursor: head } });
  indexer.setCanonicalFloor(head);
  return indexer;
}

async function snapshotIndexer(
  config: ClientConnectConfig,
  source: ChainDataSource,
  queue: BoundedUpdateQueue,
  signal: AbortSignal,
): Promise<QuoteIndexer> {
  const snapshot = await withDeadline("bootstrap snapshot", config.sourceOperationTimeoutMilliseconds, signal, () =>
    source.snapshot(config.deployment),
  );
  const indexer = QuoteIndexer.fromSnapshot(snapshot, config.deployment);
  indexer.replayHandoff(queue.drainAll(), snapshot.cursor);
  return indexer;
}

class RecoveryCoordinator {
  /** Currently running recovery shared by every caller. */
  private current?: Promise<void>;

  constructor(
    private readonly indexer: QuoteIndexer,
    private readonly source: ChainDataSource,
    private readonly config: ClientConnectConfig,
    private readonly queue: BoundedUpdateQueue,
    private readonly activity: SourceActivity,
    private readonly signal: AbortSignal,
  ) {}

  /** Starts or joins the sole canonical recovery. */
  run(): Promise<void> {
    this.indexer.markNotReady();
    if (this.current) return this.current;
    const operation = this.recoverUntilReady();
    const tracked = operation.finally(() => {
      if (this.current === tracked) this.current = undefined;
    });
    this.current = tracked;
    return this.current;
  }

  private async recoverUntilReady(): Promise<void> {
    while (!this.signal.aborted) {
      try {
        const active = await waitForActivity(
          "recovery source activity",
          this.activity,
          this.config.sourceOperationTimeoutMilliseconds,
          this.signal,
        );
        if (!active) return;
        const snapshot = await withDeadline(
          "recovery snapshot",
          this.config.sourceOperationTimeoutMilliseconds,
          this.signal,
          () => this.source.snapshot(this.config.deployment),
        );
        this.indexer.installSnapshot(snapshot, this.queue.drainAll());
        return;
      } catch (error) {
        this.indexer.markNotReady();
        if (isPermanentBootstrapError(error)) throw error;
        await delay(this.config.reconnectDelayMilliseconds, this.signal);
      }
    }
  }
}

function remaining(deadline: number): number {
  return Math.max(1, Math.ceil(deadline - monotonicMilliseconds()));
}

function waitForActivity(
  operation: string,
  activity: SourceActivity,
  timeoutMilliseconds: number,
  signal: AbortSignal,
): Promise<boolean> {
  const wait = new AbortController();
  return withDeadline(
    operation,
    timeoutMilliseconds,
    signal,
    () => activity.waitUntilActive(wait.signal),
    () => wait.abort(),
  );
}

function isPermanentBootstrapError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const code = (error as Error & { code?: string }).code;
  return code === "INVALID" || code === "CODE_HASH_MISMATCH";
}
