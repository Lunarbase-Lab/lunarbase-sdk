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
  IndexerCorrectionMetrics,
  IndexerHealth,
  IndexerLifecycleListener,
} from "../model.js";
import { compareCursor } from "../source.js";
import { validateCoreLogIdentity } from "./cursor_policy.js";
import { QuoteIndexer } from "./engine.js";
import { validateFilterTopics } from "./filter.js";
import { pumpSource, SourceActivity } from "./source_task.js";
import { BoundedUpdateQueue } from "./update_queue.js";
import { monotonicMilliseconds, withDeadline } from "./lifecycle.js";
import { RecoveryCoordinator } from "./recovery_coordinator.js";
import {
  DEFAULT_CORRECTION_HISTORY_BLOCKS,
  DEFAULT_CORRECTION_HISTORY_BYTES,
  MAX_CORRECTION_HISTORY_BLOCKS,
  MAX_CORRECTION_HISTORY_BYTES,
} from "./correction_journal.js";

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
  /** Maximum eventful blocks retained for optimistic rollback. */
  readonly correctionHistoryBlockBound?: number;
  /** Maximum compact before-image bytes retained for optimistic rollback. */
  readonly correctionHistoryByteBound?: number;
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
      activity.onInactive(() => indexer.markNotReady());
      const recovery = new RecoveryCoordinator(indexer, source, ownedConfig, queue, activity, sourceController.signal);
      const reducerTask = reduceSource(
        indexer,
        queue,
        recovery,
        reducerController.signal,
        () => sourceController.signal.aborted,
      )
        .catch((error) => {
          recovery.rejectPending(error);
          indexer.markNotReady();
          sourceController.abort();
          reducerController.abort();
          queue.close();
          activity.setActive(false);
        })
        .finally(() => recovery.rejectPending(new IndexerError("SOURCE", "ordered reducer stopped")));
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

  /** Returns compact correction/journal counters for monitoring. */
  correctionMetrics(): IndexerCorrectionMetrics {
    return this.indexer.correctionMetrics();
  }

  /** Observes correction and true-gap notices outside reducer execution. */
  onLifecycle(listener: IndexerLifecycleListener): () => void {
    return this.indexer.onLifecycle(listener);
  }

  /** Returns a checkpoint only while the current state is publishable. */
  checkpoint(): Checkpoint | undefined {
    return !this.stopping && this.indexer.health().ready ? this.indexer.checkpoint() : undefined;
  }

  /** Forces one serialized canonical recovery while subscription buffering continues. */
  recover(): Promise<void> {
    this.requireRunning();
    return this.recovery.request();
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
    correctionHistoryBlockBound: config.correctionHistoryBlockBound ?? DEFAULT_CORRECTION_HISTORY_BLOCKS,
    correctionHistoryByteBound: config.correctionHistoryByteBound ?? DEFAULT_CORRECTION_HISTORY_BYTES,
    reconnectDelayMilliseconds: config.reconnectDelayMilliseconds,
    sourceStallTimeoutMilliseconds: config.sourceStallTimeoutMilliseconds,
    sourceOperationTimeoutMilliseconds: config.sourceOperationTimeoutMilliseconds,
  }))
    if (!Number.isSafeInteger(value) || value <= 0)
      throw new IndexerError("SOURCE", `${name} must be a positive safe integer`);
  if (config.queueByteBound < 1024) throw new IndexerError("SOURCE", "queueByteBound must be at least 1024 bytes");
  if ((config.correctionHistoryByteBound ?? DEFAULT_CORRECTION_HISTORY_BYTES) < 1024)
    throw new IndexerError("SOURCE", "correctionHistoryByteBound must be at least 1024 bytes");
  if ((config.correctionHistoryBlockBound ?? DEFAULT_CORRECTION_HISTORY_BLOCKS) > MAX_CORRECTION_HISTORY_BLOCKS)
    throw new IndexerError("SOURCE", "correctionHistoryBlockBound must be at most 128");
  if ((config.correctionHistoryByteBound ?? DEFAULT_CORRECTION_HISTORY_BYTES) > MAX_CORRECTION_HISTORY_BYTES)
    throw new IndexerError("SOURCE", "correctionHistoryByteBound must be at most 16 MiB");
}

function ownConnectConfig(config: ClientConnectConfig): ClientConnectConfig {
  return Object.freeze({
    ...config,
    correctionHistoryBlockBound: config.correctionHistoryBlockBound ?? DEFAULT_CORRECTION_HISTORY_BLOCKS,
    correctionHistoryByteBound: config.correctionHistoryByteBound ?? DEFAULT_CORRECTION_HISTORY_BYTES,
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
  let consumedSequence = queue.drainedThroughSequence;
  while (!signal.aborted) {
    const requestedBarrier = recovery.requestedBarrier;
    if (requestedBarrier !== undefined && consumedSequence >= requestedBarrier) {
      consumedSequence = Math.max(consumedSequence, await recovery.serviceRequested());
      continue;
    }
    const queued = await queue.nextWithSequence(signal);
    if (!queued) {
      if (signal.aborted || queue.closed) return;
      continue;
    }
    const update = queued.update;
    if (!stateValid) {
      consumedSequence = Math.max(consumedSequence, queued.sequence);
      continue;
    }
    const barrierAfterDequeue = recovery.requestedBarrier;
    if (barrierAfterDequeue !== undefined && queued.sequence > barrierAfterDequeue) {
      consumedSequence = Math.max(consumedSequence, await recovery.serviceRequested(queued));
      continue;
    }
    try {
      indexer.applyCoreUpdate(update);
    } catch {
      if (stopping()) {
        indexer.markNotReady();
        stateValid = false;
      } else {
        const recoveredThrough =
          recovery.requestedBarrier === undefined
            ? await recovery.run(queued)
            : await recovery.serviceRequested(queued);
        consumedSequence = Math.max(consumedSequence, recoveredThrough);
      }
      continue;
    }
    consumedSequence = Math.max(consumedSequence, queued.sequence);
    const barrierAfterApply = recovery.requestedBarrier;
    if (barrierAfterApply !== undefined && consumedSequence >= barrierAfterApply)
      consumedSequence = Math.max(consumedSequence, await recovery.serviceRequested());
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
  const indexer = QuoteIndexer.fromCheckpoint(checkpoint, config.deployment, correctionLimits(config));
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
  const indexer = QuoteIndexer.fromSnapshot(snapshot, config.deployment, correctionLimits(config));
  indexer.replayHandoff(queue.drainAll(), snapshot.cursor);
  return indexer;
}

function correctionLimits(config: ClientConnectConfig) {
  return {
    blockCapacity: config.correctionHistoryBlockBound ?? DEFAULT_CORRECTION_HISTORY_BLOCKS,
    byteCapacity: config.correctionHistoryByteBound ?? DEFAULT_CORRECTION_HISTORY_BYTES,
  };
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
  return code === "INVALID" || code === "INVALID_REQUEST" || code === "CODE_HASH_MISMATCH";
}
