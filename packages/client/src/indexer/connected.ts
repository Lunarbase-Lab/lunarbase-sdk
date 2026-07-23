/** Connected source/reducer lifecycle for embeddable TypeScript clients. */
import type { QuoteRequest } from "@lunarbase/math";
import { checkpointMatchesDeployment, validateDeploymentConfig } from "../bootstrap.js";
import { IndexerError } from "../model.js";
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
import { QuoteIndexer } from "./engine.js";
import { delay, pumpSource, SourceActivity } from "./source_task.js";
import { BoundedUpdateQueue } from "./update_queue.js";

/** Runtime-only configuration; persistence belongs to the Rust indexer. */
export interface ClientConnectConfig {
  /** Immutable network, Core contract, router, and endpoint identity. */
  readonly deployment: DeploymentConfig;
  /** Core address and quote-critical topics accepted by the source. */
  readonly filter: ContractFilter;
  /** Maximum normalized updates waiting for the ordered reducer. */
  readonly queueBound: number;
  /** Delay before reopening a failed realtime subscription. */
  readonly reconnectDelayMilliseconds: number;
  /** Maximum interval without an update before readiness is revoked. */
  readonly sourceStallTimeoutMilliseconds: number;
}

/** High-level embeddable client with one active ordered reducer. */
export class ConnectedQuoteClient {
  private constructor(
    /** Mutable quote state owned by the single ordered reducer. */
    private readonly indexer: QuoteIndexer,
    /** Cooperative cancellation signal for source and reducer loops. */
    private readonly controller: AbortController,
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
    const controller = new AbortController();
    const queue = new BoundedUpdateQueue(config.queueBound);
    const activity = new SourceActivity();
    const pumpTask = pumpSource(
      source,
      config.filter,
      queue,
      activity,
      controller.signal,
      config.reconnectDelayMilliseconds,
      config.sourceStallTimeoutMilliseconds,
    );
    try {
      if (!(await activity.waitUntilActive(controller.signal)))
        throw new IndexerError("SOURCE", "realtime source stopped before handshake");
      const indexer = await bootstrapIndexer(config, source, queue, activity, controller.signal, optionalCheckpoint);
      const recovery = new RecoveryCoordinator(indexer, source, config, queue, activity, controller.signal);
      const reducerTask = reduceSource(indexer, queue, recovery, controller.signal).catch(() => {
        indexer.markNotReady();
        controller.abort();
        queue.close();
        activity.setActive(false);
      });
      return new ConnectedQuoteClient(indexer, controller, queue, activity, recovery, pumpTask, reducerTask);
    } catch (error) {
      controller.abort();
      queue.close();
      await Promise.allSettled([pumpTask]);
      throw error;
    }
  }

  /** Computes one fully offchain quote. */
  quote(request: QuoteRequest): ClientQuote {
    return this.indexer.quote(request);
  }

  /** Computes a batch on one cursor without yielding to the event loop. */
  quoteMany(requests: readonly QuoteRequest[]): ClientBatchQuote {
    return this.indexer.quoteMany(requests);
  }

  /** Returns current readiness and cursor metadata. */
  health(): IndexerHealth {
    return this.indexer.health();
  }

  /** Returns a checkpoint only while the current state is publishable. */
  checkpoint(): Checkpoint | undefined {
    return this.indexer.health().ready ? this.indexer.checkpoint() : undefined;
  }

  /** Forces one serialized canonical recovery while subscription buffering continues. */
  recover(): Promise<void> {
    return this.recovery.run();
  }

  /** Stops all background work and revokes readiness. */
  async shutdown(): Promise<void> {
    this.controller.abort();
    this.queue.close();
    this.activity.setActive(false);
    this.indexer.markNotReady();
    await Promise.allSettled([this.pumpTask, this.reducerTask]);
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
  for (const [name, value] of Object.entries({
    queueBound: config.queueBound,
    reconnectDelayMilliseconds: config.reconnectDelayMilliseconds,
    sourceStallTimeoutMilliseconds: config.sourceStallTimeoutMilliseconds,
  }))
    if (!Number.isSafeInteger(value) || value <= 0)
      throw new IndexerError("SOURCE", `${name} must be a positive safe integer`);
}

async function reduceSource(
  indexer: QuoteIndexer,
  queue: BoundedUpdateQueue,
  recovery: RecoveryCoordinator,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    const update = await queue.next(signal);
    if (!update) return;
    try {
      indexer.applyCoreUpdate(update);
    } catch {
      await recovery.run();
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
  let checkpoint = optionalCheckpoint;
  while (!signal.aborted) {
    if (!(await activity.waitUntilActive(signal))) break;
    try {
      if (
        checkpoint &&
        checkpointMatchesDeployment(checkpoint, config.deployment) &&
        (await source.validateCheckpoint(checkpoint))
      ) {
        try {
          const restored = await restoreCheckpoint(checkpoint, config, source);
          restored.replayHandoff(queue.drainAll(), restored.health().cursor ?? checkpoint.cursor);
          return restored;
        } catch {
          checkpoint = undefined;
        }
      }
      return await snapshotIndexer(config, source, queue);
    } catch (error) {
      if (isPermanentBootstrapError(error)) throw error;
      checkpoint = undefined;
      await delay(config.reconnectDelayMilliseconds, signal);
    }
  }
  throw new IndexerError("SOURCE", "bootstrap cancelled before a coherent state was installed");
}

async function restoreCheckpoint(
  checkpoint: Checkpoint,
  config: ClientConnectConfig,
  source: ChainDataSource,
): Promise<QuoteIndexer> {
  const indexer = QuoteIndexer.fromCheckpoint(checkpoint, config.deployment);
  const head = await source.canonicalHead();
  if (head.chainId !== checkpoint.cursor.chainId || head.blockNumber < checkpoint.cursor.blockNumber)
    throw new IndexerError("GAP", "checkpoint is ahead of canonical head");
  const fromBlock =
    checkpoint.cursor.transactionIndex === undefined && checkpoint.cursor.logIndex === undefined
      ? checkpoint.cursor.blockNumber + 1n
      : checkpoint.cursor.blockNumber;
  if (fromBlock <= head.blockNumber) {
    const logs = [...(await source.backfill({ fromBlock, toBlock: head.blockNumber, filter: config.filter }))]
      .filter((log) => compareCursor(log.cursor, checkpoint.cursor) > 0)
      .sort((left, right) => compareCursor(left.cursor, right.cursor));
    for (const log of logs) indexer.applyCoreUpdate({ kind: "Log", log });
  }
  indexer.applyCoreUpdate({ kind: "Head", cursor: head });
  return indexer;
}

async function snapshotIndexer(
  config: ClientConnectConfig,
  source: ChainDataSource,
  queue: BoundedUpdateQueue,
): Promise<QuoteIndexer> {
  const snapshot = await source.snapshot(config.deployment);
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
      if (!(await this.activity.waitUntilActive(this.signal))) return;
      try {
        const snapshot = await this.source.snapshot(this.config.deployment);
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

function isPermanentBootstrapError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const code = (error as Error & { code?: string }).code;
  return code === "INVALID" || code === "CODE_HASH_MISMATCH";
}
