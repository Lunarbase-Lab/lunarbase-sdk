import type { QuoteOutcome, QuoteRequest, QuoteState } from "@lunarbase/math";
import type {
  BootstrapSnapshot,
  ChainEventSource,
  ChainUpdate,
  CheckpointStore,
  ClientQuote,
  ContractFilter,
  DeploymentConfig,
  FreshnessPolicy,
  IndexerHealth,
  SnapshotProvider,
} from "../model.js";
import { Commitment, commitmentRank, IndexerError } from "../model.js";
import { QuoteIndexer } from "./engine.js";

export interface ClientConnectConfig {
  readonly deployment: DeploymentConfig;
  readonly filter: ContractFilter;
  readonly laneAssets: readonly import("@lunarbase/math").Address[];
  readonly routers: readonly import("@lunarbase/math").Address[];
  readonly bufferCapacity: number;
  readonly reconnectDelayMilliseconds: number;
  readonly checkpointStore?: CheckpointStore;
}

/** High-level source/reducer lifecycle with bounded handoff and recovery. */
export class ConnectedQuoteClient {
  private constructor(
    private readonly indexer: QuoteIndexer,
    private readonly source: ChainEventSource,
    private readonly filter: ContractFilter,
    private readonly checkpointStore: CheckpointStore | undefined,
    private readonly controller: AbortController,
    private readonly queue: BoundedUpdateQueue,
    private readonly pumpTask: Promise<void>,
    private readonly reducerTask: Promise<void>,
    private readonly handoffBlock: bigint,
  ) {}

  /** Starts source pumping, snapshot handoff, reducer processing, and optional persistence. */
  static async connect(
    provider: SnapshotProvider,
    source: ChainEventSource,
    config: ClientConnectConfig,
  ): Promise<ConnectedQuoteClient> {
    if (config.filter.address.toLowerCase() !== config.deployment.core.toLowerCase())
      throw new IndexerError("SOURCE", "source filter must target deployment Core");
    if (
      config.deployment.chainId <= 0n ||
      config.bufferCapacity <= 0 ||
      !Number.isSafeInteger(config.bufferCapacity) ||
      config.reconnectDelayMilliseconds <= 0 ||
      !Number.isSafeInteger(config.reconnectDelayMilliseconds)
    )
      throw new IndexerError("SOURCE", "client bounds must be positive safe integers");
    if (source.network !== config.deployment.network) throw new IndexerError("SOURCE", "source network mismatch");
    const controller = new AbortController();
    const queue = new BoundedUpdateQueue(config.bufferCapacity);
    const pumpTask = pumpSource(source, config.filter, queue, controller.signal, config.reconnectDelayMilliseconds);
    const initialState: QuoteState = {
      cash: config.deployment.core,
      lanes: new Map(),
      totalPrincipalAmount: new Map(),
      whitelist: new Map(),
      blacklistFeeMultiplier: 0n,
      partnerFeeBps: new Map(),
      stateVersion: 0n,
    };
    const indexer = QuoteIndexer.create(config.deployment.expectedRuntimeCodeHash, initialState);
    let snapshot: BootstrapSnapshot;
    try {
      snapshot = await provider.snapshot(config.deployment, config.laneAssets, config.routers);
      const buffered = queue.drainAll();
      indexer.bootstrapNormalized(snapshot, buffered);
      if (config.checkpointStore) {
        const checkpoint = indexer.checkpoint();
        if (!checkpoint) throw new IndexerError("NO_CURSOR", "bootstrap produced no checkpoint cursor");
        config.checkpointStore.commit(checkpoint, []);
      }
    } catch (error) {
      controller.abort();
      queue.close();
      throw error;
    }
    const reducerTask = reduceSource(
      indexer,
      source,
      config.filter,
      queue,
      controller.signal,
      snapshot.cursor.blockNumber,
      config.checkpointStore,
    );
    return new ConnectedQuoteClient(
      indexer,
      source,
      config.filter,
      config.checkpointStore,
      controller,
      queue,
      pumpTask,
      reducerTask,
      snapshot.cursor.blockNumber,
    );
  }

  /** Waits until readiness reaches a requested commitment or times out. */
  async awaitReady(minimumCommitment: Commitment, timeoutMilliseconds = 30_000): Promise<void> {
    const started = Date.now();
    while (true) {
      const health = this.health();
      if (health.ready && commitmentRank(health.commitment) >= commitmentRank(minimumCommitment)) return;
      if (Date.now() - started >= timeoutMilliseconds)
        throw new IndexerError("FRESHNESS_UNAVAILABLE", "timed out waiting for client readiness");
      await delay(10);
    }
  }

  /** Returns the latest immutable quote state. */
  stateSnapshot(): QuoteState {
    return this.indexer.stateSnapshot();
  }
  /** Computes a quote from the connected reducer. */
  quote(request: QuoteRequest, executionBlockNumber: bigint): QuoteOutcome {
    return this.indexer.quote(request, executionBlockNumber);
  }
  /** Computes a quote after enforcing freshness policy. */
  quoteWithPolicy(request: QuoteRequest, executionBlockNumber: bigint, policy: FreshnessPolicy): ClientQuote {
    return this.indexer.quoteWithPolicy(request, executionBlockNumber, policy);
  }
  /** Computes an exact-input quote. */
  quoteExactIn(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote {
    return this.indexer.quoteExactIn(request, executionBlockNumber);
  }
  /** Computes an exact-output quote. */
  quoteExactOut(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote {
    return this.indexer.quoteExactOut(request, executionBlockNumber);
  }
  /** Returns current background-client health. */
  health(): IndexerHealth {
    return this.indexer.health();
  }
  /** Returns the latest durable checkpoint, if available. */
  checkpoint(): import("../model.js").Checkpoint | undefined {
    return this.indexer.checkpoint();
  }

  /** Canonically resynchronizes the reducer and republishes its checkpoint. */
  async resync(): Promise<void> {
    await this.indexer.recoverFromSource(this.source, this.filter);
    const checkpoint = this.indexer.checkpoint();
    if (checkpoint && this.checkpointStore) this.checkpointStore.commit(checkpoint, []);
  }

  /** Stops background tasks and marks the connected client unavailable. */
  async shutdown(): Promise<void> {
    this.controller.abort();
    this.queue.close();
    this.indexer.shutdown();
    await Promise.allSettled([this.pumpTask, this.reducerTask]);
  }
}

async function pumpSource(
  source: ChainEventSource,
  filter: ContractFilter,
  queue: BoundedUpdateQueue,
  signal: AbortSignal,
  reconnectDelayMilliseconds: number,
): Promise<void> {
  while (!signal.aborted && !queue.closed) {
    let sawGap = false;
    try {
      for await (const update of source.subscribe(filter, signal)) {
        if (signal.aborted || queue.closed) return;
        queue.push(update);
        if (update.kind === "Gap") {
          sawGap = true;
          break;
        }
      }
      if (!signal.aborted && !queue.closed && !sawGap)
        queue.push({ kind: "Gap", reason: "source stream closed; canonical recovery required" });
    } catch (error) {
      if (signal.aborted || queue.closed) return;
      queue.push({
        kind: "Gap",
        reason: `source subscribe failed: ${error instanceof Error ? error.message : String(error)}`,
      });
    }
    await delay(reconnectDelayMilliseconds);
  }
}

async function reduceSource(
  indexer: QuoteIndexer,
  source: ChainEventSource,
  filter: ContractFilter,
  queue: BoundedUpdateQueue,
  signal: AbortSignal,
  handoffBlock: bigint,
  checkpointStore?: CheckpointStore,
): Promise<void> {
  while (!signal.aborted) {
    const update = await queue.next(signal);
    if (!update) return;
    const cursor =
      update.kind === "Log"
        ? update.log.cursor
        : update.kind === "Head"
          ? update.cursor
          : update.kind === "Reorg"
            ? update.newHead
            : update.kind === "Gap"
              ? update.cursor
              : undefined;
    if (
      cursor &&
      ((update.kind === "Log" && cursor.blockNumber <= handoffBlock) ||
        (update.kind === "Head" && cursor.blockNumber < handoffBlock))
    )
      continue;
    try {
      indexer.applyCoreUpdate(update);
      const checkpoint = indexer.checkpoint();
      if (checkpoint && checkpointStore) checkpointStore.commit(checkpoint, [update]);
    } catch {
      try {
        await indexer.recoverFromSource(source, filter);
        const checkpoint = indexer.checkpoint();
        if (checkpoint && checkpointStore) checkpointStore.commit(checkpoint, []);
      } catch {
        indexer.reducer.markNotReady();
      }
    }
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

class BoundedUpdateQueue {
  private readonly values: ChainUpdate[] = [];
  private readonly waiters: Array<(value: ChainUpdate | undefined) => void> = [];
  private ended = false;
  constructor(private readonly capacity: number) {}
  get closed(): boolean {
    return this.ended;
  }
  push(update: ChainUpdate): void {
    if (this.ended) return;
    if (this.values.length >= this.capacity) {
      this.values.length = 0;
      this.values.push({ kind: "Gap", reason: "client update queue overflow; canonical recovery required" });
      this.ended = true;
    } else this.values.push(update);
    this.resolve();
  }
  drainAll(): ChainUpdate[] {
    const values = this.values.splice(0);
    return values;
  }
  close(): void {
    this.ended = true;
    this.resolve();
  }
  async next(signal?: AbortSignal): Promise<ChainUpdate | undefined> {
    const value = this.values.shift();
    if (value) return value;
    if (this.ended || signal?.aborted) return undefined;
    return new Promise((resolve) => {
      const onAbort = () => {
        signal?.removeEventListener("abort", onAbort);
        resolve(undefined);
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      this.waiters.push((next) => {
        signal?.removeEventListener("abort", onAbort);
        resolve(next);
      });
    });
  }
  private resolve(): void {
    while (this.waiters.length > 0 && (this.values.length > 0 || this.ended))
      this.waiters.shift()?.(this.values.shift());
  }
}
