/** Connected source/reducer lifecycle for embeddable TypeScript clients. */
import type { QuoteRequest } from "@lunarbase/math";
import { checkpointMatchesDeployment, validateDeploymentConfig } from "../bootstrap.js";
import { IndexerError } from "../model.js";
import type {
  ChainDataSource,
  ChainUpdate,
  Checkpoint,
  ClientBatchQuote,
  ClientQuote,
  ContractFilter,
  DeploymentConfig,
  IndexerHealth,
} from "../model.js";
import { compareCursor } from "../source.js";
import { QuoteIndexer } from "./engine.js";

/** Runtime-only configuration; persistence belongs to the Rust indexer. */
export interface ClientConnectConfig {
  readonly deployment: DeploymentConfig;
  readonly filter: ContractFilter;
  readonly queueBound: number;
  readonly reconnectDelayMilliseconds: number;
}

/** High-level embeddable client with one active ordered reducer. */
export class ConnectedQuoteClient {
  private constructor(
    private readonly indexer: QuoteIndexer,
    private readonly source: ChainDataSource,
    private readonly config: ClientConnectConfig,
    private readonly controller: AbortController,
    private readonly queue: BoundedUpdateQueue,
    private readonly pumpTask: Promise<void>,
    private readonly reducerTask: Promise<void>,
  ) {}

  /**
   * Connects a subscription before loading checkpoint/snapshot state.
   *
   * The optional checkpoint is accepted only after local identity and canonical
   * block-hash validation.
   */
  static async connect(
    config: ClientConnectConfig,
    source: ChainDataSource,
    optionalCheckpoint?: Checkpoint,
  ): Promise<ConnectedQuoteClient> {
    validateConnectConfig(config, source);
    const controller = new AbortController();
    const queue = new BoundedUpdateQueue(config.queueBound);
    const pumpTask = pumpSource(source, config.filter, queue, controller.signal, config.reconnectDelayMilliseconds);
    let indexer: QuoteIndexer;
    try {
      const checkpointValid =
        optionalCheckpoint !== undefined &&
        checkpointMatchesDeployment(optionalCheckpoint, config.deployment) &&
        (await source.validateCheckpoint(optionalCheckpoint));
      if (checkpointValid && optionalCheckpoint) {
        try {
          indexer = await restoreCheckpoint(optionalCheckpoint, config, source);
          indexer.replayHandoff(
            queue.drainAll(),
            indexer.health().cursor?.blockNumber ?? optionalCheckpoint.cursor.blockNumber,
          );
        } catch {
          indexer = await snapshotIndexer(config, source, queue);
        }
      } else {
        indexer = await snapshotIndexer(config, source, queue);
      }
    } catch (error) {
      controller.abort();
      queue.close();
      await Promise.allSettled([pumpTask]);
      throw error;
    }
    const reducerTask = reduceSource(indexer, source, config, queue, controller.signal);
    return new ConnectedQuoteClient(indexer, source, config, controller, queue, pumpTask, reducerTask);
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

  /** Returns a deep-cloned v3 checkpoint for external persistence. */
  checkpoint(): Checkpoint | undefined {
    return this.indexer.checkpoint();
  }

  /** Forces an immediate canonical resnapshot while continuing subscription buffering. */
  async recover(): Promise<void> {
    this.indexer.markNotReady();
    const snapshot = await this.source.snapshot(this.config.deployment);
    this.indexer.installSnapshot(snapshot, this.queue.drainAll());
  }

  /** Stops all background work and revokes readiness. */
  async shutdown(): Promise<void> {
    this.controller.abort();
    this.queue.close();
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
  if (
    !Number.isSafeInteger(config.queueBound) ||
    config.queueBound <= 0 ||
    !Number.isSafeInteger(config.reconnectDelayMilliseconds) ||
    config.reconnectDelayMilliseconds <= 0
  )
    throw new IndexerError("SOURCE", "runtime bounds must be positive safe integers");
}

async function pumpSource(
  source: ChainDataSource,
  filter: ContractFilter,
  queue: BoundedUpdateQueue,
  signal: AbortSignal,
  reconnectDelayMilliseconds: number,
): Promise<void> {
  while (!signal.aborted && !queue.closed) {
    try {
      for await (const update of source.subscribe(filter, signal)) {
        if (signal.aborted || queue.closed) return;
        queue.push(update);
        if (update.kind === "Gap") break;
      }
      if (!signal.aborted && !queue.closed)
        queue.push({
          kind: "Gap",
          reason: "source stream ended; canonical recovery required",
        });
    } catch (error) {
      if (signal.aborted || queue.closed) return;
      queue.push({
        kind: "Gap",
        reason: `source failed: ${error instanceof Error ? error.message : String(error)}`,
      });
    }
    await delay(reconnectDelayMilliseconds, signal);
  }
}

async function reduceSource(
  indexer: QuoteIndexer,
  source: ChainDataSource,
  config: ClientConnectConfig,
  queue: BoundedUpdateQueue,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    const update = await queue.next(signal);
    if (!update) return;
    try {
      indexer.applyCoreUpdate(update);
    } catch {
      indexer.markNotReady();
      await recoverUntilReady(indexer, source, config, queue, signal);
    }
  }
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
    const logs = [
      ...(await source.backfill({
        fromBlock,
        toBlock: head.blockNumber,
        filter: config.filter,
      })),
    ].sort((left, right) => compareCursor(left.cursor, right.cursor));
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
  indexer.replayHandoff(queue.drainAll(), snapshot.cursor.blockNumber);
  return indexer;
}

async function recoverUntilReady(
  indexer: QuoteIndexer,
  source: ChainDataSource,
  config: ClientConnectConfig,
  queue: BoundedUpdateQueue,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    try {
      const snapshot = await source.snapshot(config.deployment);
      indexer.installSnapshot(snapshot, queue.drainAll());
      return;
    } catch {
      indexer.markNotReady();
      await delay(config.reconnectDelayMilliseconds, signal);
    }
  }
}

function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(done, milliseconds);
    function done() {
      signal.removeEventListener("abort", done);
      clearTimeout(timer);
      resolve();
    }
    signal.addEventListener("abort", done, { once: true });
  });
}

class BoundedUpdateQueue {
  private readonly values: ChainUpdate[] = [];
  private readonly waiters: Array<(value: ChainUpdate | undefined) => void> = [];
  private ended = false;
  private overflowed = false;

  constructor(private readonly capacity: number) {}

  get closed(): boolean {
    return this.ended;
  }

  push(update: ChainUpdate): void {
    if (this.ended || this.overflowed) return;
    if (this.values.length >= this.capacity) {
      this.values.length = 0;
      this.values.push({
        kind: "Gap",
        reason: "runtime queue overflow; canonical recovery required",
      });
      this.overflowed = true;
    } else {
      this.values.push(update);
    }
    this.resolveWaiters();
  }

  drainAll(): ChainUpdate[] {
    this.overflowed = false;
    return this.values.splice(0);
  }

  close(): void {
    this.ended = true;
    this.resolveWaiters();
  }

  async next(signal: AbortSignal): Promise<ChainUpdate | undefined> {
    const value = this.values.shift();
    if (value) {
      if (this.values.length === 0) this.overflowed = false;
      return value;
    }
    if (this.ended || signal.aborted) return undefined;
    return new Promise((resolve) => {
      const onAbort = () => resolve(undefined);
      signal.addEventListener("abort", onAbort, { once: true });
      this.waiters.push((next) => {
        signal.removeEventListener("abort", onAbort);
        resolve(next);
      });
    });
  }

  private resolveWaiters(): void {
    while (this.waiters.length > 0 && (this.values.length > 0 || this.ended))
      this.waiters.shift()?.(this.values.shift());
  }
}
