import { quote, type QuoteOutcome, type QuoteRequest, type QuoteState } from "@lunarbase/math";
import { decodeCoreEvent } from "./abi.js";
import { fetchBootstrapSnapshot, updateOrder } from "./bootstrap.js";
import type { BootstrapSnapshot, ChainCursor, ChainEventSource, ChainUpdate, CheckpointStore, ClientQuote, ContractFilter, DeploymentConfig, FreshnessPolicy, IndexerHealth, LogDecoder, SnapshotProvider } from "./model.js";
import { Commitment, commitmentRank, IndexerError, MATH_COMPATIBILITY_VERSION } from "./model.js";
import { QuoteReducer } from "./reducer.js";

export class QuoteIndexer {
  readonly mathCompatibilityVersion = MATH_COMPATIBILITY_VERSION;
  private commitment: Commitment = Commitment.Realtime;
  private lastObservedAt = BigInt(Date.now());
  constructor(readonly expectedRuntimeCodeHash: string, public reducer: QuoteReducer) {}
  static create(expectedCodeHash: string, state: QuoteState): QuoteIndexer { return new QuoteIndexer(expectedCodeHash, new QuoteReducer(state)); }
  static fromCheckpoint(checkpoint: import("./model.js").Checkpoint, expectedCodeHash: string): QuoteIndexer { if (checkpoint.schemaVersion !== 2n || checkpoint.mathCompatibilityVersion !== MATH_COMPATIBILITY_VERSION || checkpoint.expectedRuntimeCodeHash.toLowerCase() !== expectedCodeHash.toLowerCase()) throw new IndexerError("CODE_HASH_MISMATCH", "checkpoint compatibility mismatch"); const indexer = new QuoteIndexer(expectedCodeHash, QuoteReducer.fromCheckpoint(checkpoint)); indexer.commitment = checkpoint.cursor.commitment; indexer.lastObservedAt = BigInt(Date.now()); return indexer; }
  bootstrap(snapshotCursor: ChainCursor): void { this.reducer.bootstrap(snapshotCursor); this.commitment = snapshotCursor.commitment; this.lastObservedAt = BigInt(Date.now()); }
  async bootstrapFromProvider(provider: SnapshotProvider, config: DeploymentConfig, laneAssets: readonly import("@lunarbase/math").Address[], routers: readonly import("@lunarbase/math").Address[], buffered: readonly ChainUpdate[]): Promise<void> { const snapshot = await fetchBootstrapSnapshot(provider, config, laneAssets, routers, this.expectedRuntimeCodeHash); this.bootstrapNormalized(snapshot, [...buffered]); }
  bootstrapNormalized(snapshot: BootstrapSnapshot, buffered: ChainUpdate[]): void { if (snapshot.runtimeCodeHash.toLowerCase() !== this.expectedRuntimeCodeHash.toLowerCase()) throw new IndexerError("CODE_HASH_MISMATCH", "snapshot code hash mismatch"); const block = snapshot.cursor.blockNumber; const chain = snapshot.cursor.chainId; buffered.sort(updateOrder); this.reducer = new QuoteReducer(snapshot.state); this.reducer.bootstrap(snapshot.cursor); this.commitment = snapshot.cursor.commitment; this.lastObservedAt = BigInt(Date.now()); for (const update of buffered) { if (update.kind === "Gap") { this.reducer.markNotReady(); throw new IndexerError("GAP", update.reason); } if (update.kind === "Reorg") { this.reducer.markNotReady(); throw new IndexerError("GAP", "reorg during bootstrap handoff"); } if (update.kind === "SourceHealth") { if (!update.healthy) { this.reducer.markNotReady(); throw new IndexerError("GAP", update.detail); } continue; } const cursor = update.kind === "Log" ? update.log.cursor : update.cursor; if (cursor.chainId !== chain) { this.reducer.markNotReady(); throw new IndexerError("REDUCER", "cursor chain id mismatch"); } if (cursor.blockNumber <= block) continue; this.applyCoreUpdate(update); } this.reducer.publishReady(); }
  async recoverFromSource(source: ChainEventSource, filter: ContractFilter): Promise<void> {
    const checkpoint = this.reducer.cursor();
    if (!checkpoint) throw new IndexerError("NO_CURSOR", "no canonical cursor");
    this.reducer.markNotReady();
    const head = await source.snapshotCursor();
    if (head.chainId !== checkpoint.chainId) throw new IndexerError("REDUCER", "cursor chain id mismatch");
    if (commitmentRank(head.commitment) < commitmentRank(Commitment.Canonical)) throw new IndexerError("FRESHNESS_UNAVAILABLE", "canonical recovery head is not proven");
    if (head.blockNumber < checkpoint.blockNumber) throw new IndexerError("GAP", "canonical source head regressed below checkpoint");
    const fromBlock = checkpoint.transactionIndex === undefined && checkpoint.logIndex === undefined ? checkpoint.blockNumber + 1n : checkpoint.blockNumber;
    if (fromBlock <= head.blockNumber) {
      const logs = [...await source.backfill({ fromBlock, toBlock: head.blockNumber, filter })].sort((left, right) => compareCursor(left.cursor, right.cursor));
      for (const log of logs) this.applyCoreUpdate({ kind: "Log", log });
    }
    this.applyUpdate({ kind: "Head", cursor: head }, () => undefined);
    this.reducer.publishReady();
  }
  applyUpdate(update: ChainUpdate, decodeLog: LogDecoder): void { switch (update.kind) { case "Log": if (update.log.removed) { this.reducer.markNotReady(); throw new IndexerError("GAP", "removed log requires canonical rebuild"); } { const event = decodeLog(update.log); if (event) { try { this.reducer.apply(update.log.cursor, event); } catch (error) { this.reducer.markNotReady(); throw new IndexerError("REDUCER", error instanceof Error ? error.message : "reducer error"); } this.commitment = update.log.cursor.commitment; this.lastObservedAt = BigInt(Date.now()); } } break; case "Head": try { this.reducer.observeHead(update.cursor); } catch (error) { this.reducer.markNotReady(); throw new IndexerError("REDUCER", error instanceof Error ? error.message : "head reducer error"); } this.commitment = this.reducer.cursor()?.commitment ?? update.cursor.commitment; this.lastObservedAt = BigInt(Date.now()); break; case "Reorg": this.reducer.markNotReady(); throw new IndexerError("GAP", "reorg requires canonical backfill"); case "Gap": this.reducer.markNotReady(); throw new IndexerError("GAP", update.reason); case "SourceHealth": if (!update.healthy) this.reducer.markNotReady(); break; } }
  applyCoreUpdate(update: ChainUpdate): void { if (update.kind === "Log") { const event = decodeCoreEvent(update.log); this.applyUpdate(update, () => event); } else this.applyUpdate(update, () => undefined); }
  snapshot(): QuoteState { if (!this.reducer.isReady()) throw new IndexerError("NOT_READY", "indexer is not ready"); return this.reducer.state(); }
  stateSnapshot(): QuoteState { return this.snapshot(); }
  quote(request: QuoteRequest, executionBlockNumber: bigint): QuoteOutcome { const state = this.snapshot(); return quote(request, { cash: state.cash, executionBlockNumber, stateVersion: state.stateVersion }, state); }
  quoteWithPolicy(request: QuoteRequest, executionBlockNumber: bigint, policy: FreshnessPolicy): ClientQuote { const cursor = this.reducer.cursor(); if (!cursor) throw new IndexerError("NO_CURSOR", "no canonical cursor"); const rank = (value: Commitment) => value === Commitment.Realtime ? 0n : value === Commitment.Canonical ? 1n : 2n; if (rank(cursor.commitment) < rank(policy.minimumCommitment)) throw new IndexerError("FRESHNESS_UNAVAILABLE", "requested freshness cannot be proven"); if (policy.maxAgeBlocks !== undefined && executionBlockNumber > cursor.blockNumber && executionBlockNumber - cursor.blockNumber > policy.maxAgeBlocks) throw new IndexerError("FRESHNESS_UNAVAILABLE", "snapshot is too old"); const now = BigInt(Date.now()); return { outcome: this.quote(request, executionBlockNumber), cursor, commitment: cursor.commitment, observedAt: this.lastObservedAt, ageMilliseconds: now >= this.lastObservedAt ? now - this.lastObservedAt : 0n, stale: false, contractCodeHash: this.expectedRuntimeCodeHash, mathCompatibilityVersion: this.mathCompatibilityVersion }; }
  quoteExactIn(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote { return this.quoteWithPolicy({ ...request, mode: "ExactIn" }, executionBlockNumber, { minimumCommitment: Commitment.Realtime }); }
  quoteExactOut(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote { return this.quoteWithPolicy({ ...request, mode: "ExactOut" }, executionBlockNumber, { minimumCommitment: Commitment.Realtime }); }
  checkpoint(): import("./model.js").Checkpoint | undefined { return this.reducer.checkpoint(this.expectedRuntimeCodeHash); }
  currentCommitment(): Commitment { return this.commitment; }
  health(): IndexerHealth { return { ready: this.reducer.isReady(), commitment: this.commitment, cursor: this.reducer.cursor(), contractCodeHash: this.expectedRuntimeCodeHash, mathCompatibilityVersion: this.mathCompatibilityVersion }; }
  shutdown(): void { this.reducer.markNotReady(); }
}

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

  static async connect(provider: SnapshotProvider, source: ChainEventSource, config: ClientConnectConfig): Promise<ConnectedQuoteClient> {
    if (config.filter.address.toLowerCase() !== config.deployment.core.toLowerCase()) throw new IndexerError("SOURCE", "source filter must target deployment Core");
    if (config.deployment.chainId <= 0n || config.bufferCapacity <= 0 || !Number.isSafeInteger(config.bufferCapacity) || config.reconnectDelayMilliseconds <= 0 || !Number.isSafeInteger(config.reconnectDelayMilliseconds)) throw new IndexerError("SOURCE", "client bounds must be positive safe integers");
    if (source.network !== config.deployment.network) throw new IndexerError("SOURCE", "source network mismatch");
    const controller = new AbortController();
    const queue = new BoundedUpdateQueue(config.bufferCapacity);
    const pumpTask = pumpSource(source, config.filter, queue, controller.signal, config.reconnectDelayMilliseconds);
    const initialState: QuoteState = { cash: config.deployment.core, lanes: new Map(), totalPrincipalAmount: new Map(), whitelist: new Map(), blacklistFeeMultiplier: 0n, partnerFeeBps: new Map(), stateVersion: 0n };
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
    const reducerTask = reduceSource(indexer, source, config.filter, queue, controller.signal, snapshot.cursor.blockNumber, config.checkpointStore);
    return new ConnectedQuoteClient(indexer, source, config.filter, config.checkpointStore, controller, queue, pumpTask, reducerTask, snapshot.cursor.blockNumber);
  }

  async awaitReady(minimumCommitment: Commitment, timeoutMilliseconds = 30_000): Promise<void> {
    const started = Date.now();
    while (true) {
      const health = this.health();
      if (health.ready && commitmentRank(health.commitment) >= commitmentRank(minimumCommitment)) return;
      if (Date.now() - started >= timeoutMilliseconds) throw new IndexerError("FRESHNESS_UNAVAILABLE", "timed out waiting for client readiness");
      await delay(10);
    }
  }

  stateSnapshot(): QuoteState { return this.indexer.stateSnapshot(); }
  quote(request: QuoteRequest, executionBlockNumber: bigint): QuoteOutcome { return this.indexer.quote(request, executionBlockNumber); }
  quoteWithPolicy(request: QuoteRequest, executionBlockNumber: bigint, policy: FreshnessPolicy): ClientQuote { return this.indexer.quoteWithPolicy(request, executionBlockNumber, policy); }
  quoteExactIn(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote { return this.indexer.quoteExactIn(request, executionBlockNumber); }
  quoteExactOut(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote { return this.indexer.quoteExactOut(request, executionBlockNumber); }
  health(): IndexerHealth { return this.indexer.health(); }
  checkpoint(): import("./model.js").Checkpoint | undefined { return this.indexer.checkpoint(); }

  async resync(): Promise<void> {
    await this.indexer.recoverFromSource(this.source, this.filter);
    const checkpoint = this.indexer.checkpoint();
    if (checkpoint && this.checkpointStore) this.checkpointStore.commit(checkpoint, []);
  }

  async shutdown(): Promise<void> {
    this.controller.abort();
    this.queue.close();
    this.indexer.shutdown();
    await Promise.allSettled([this.pumpTask, this.reducerTask]);
  }
}

async function pumpSource(source: ChainEventSource, filter: ContractFilter, queue: BoundedUpdateQueue, signal: AbortSignal, reconnectDelayMilliseconds: number): Promise<void> {
  while (!signal.aborted && !queue.closed) {
    let sawGap = false;
    try {
      for await (const update of source.subscribe(filter, signal)) {
        if (signal.aborted || queue.closed) return;
        queue.push(update);
        if (update.kind === "Gap") { sawGap = true; break; }
      }
      if (!signal.aborted && !queue.closed && !sawGap) queue.push({ kind: "Gap", reason: "source stream closed; canonical recovery required" });
    } catch (error) {
      if (signal.aborted || queue.closed) return;
      queue.push({ kind: "Gap", reason: `source subscribe failed: ${error instanceof Error ? error.message : String(error)}` });
    }
    await delay(reconnectDelayMilliseconds);
  }
}

async function reduceSource(indexer: QuoteIndexer, source: ChainEventSource, filter: ContractFilter, queue: BoundedUpdateQueue, signal: AbortSignal, handoffBlock: bigint, checkpointStore?: CheckpointStore): Promise<void> {
  while (!signal.aborted) {
    const update = await queue.next(signal);
    if (!update) return;
    const cursor = update.kind === "Log" ? update.log.cursor : update.kind === "Head" ? update.cursor : update.kind === "Reorg" ? update.newHead : update.kind === "Gap" ? update.cursor : undefined;
    if (cursor && ((update.kind === "Log" && cursor.blockNumber <= handoffBlock) || (update.kind === "Head" && cursor.blockNumber < handoffBlock))) continue;
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

function delay(milliseconds: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, milliseconds)); }

class BoundedUpdateQueue {
  private readonly values: ChainUpdate[] = [];
  private readonly waiters: Array<(value: ChainUpdate | undefined) => void> = [];
  private ended = false;
  constructor(private readonly capacity: number) {}
  get closed(): boolean { return this.ended; }
  push(update: ChainUpdate): void { if (this.ended) return; if (this.values.length >= this.capacity) { this.values.length = 0; this.values.push({ kind: "Gap", reason: "client update queue overflow; canonical recovery required" }); this.ended = true; } else this.values.push(update); this.resolve(); }
  drainAll(): ChainUpdate[] { const values = this.values.splice(0); return values; }
  close(): void { this.ended = true; this.resolve(); }
  async next(signal?: AbortSignal): Promise<ChainUpdate | undefined> { const value = this.values.shift(); if (value) return value; if (this.ended || signal?.aborted) return undefined; return new Promise((resolve) => { const onAbort = () => { signal?.removeEventListener("abort", onAbort); resolve(undefined); }; signal?.addEventListener("abort", onAbort, { once: true }); this.waiters.push((next) => { signal?.removeEventListener("abort", onAbort); resolve(next); }); }); }
  private resolve(): void { while (this.waiters.length > 0 && (this.values.length > 0 || this.ended)) this.waiters.shift()?.(this.values.shift()); }
}

function compareCursor(left: ChainCursor, right: ChainCursor): number { for (const [a, b] of [[left.blockNumber, right.blockNumber], [left.transactionIndex ?? 0n, right.transactionIndex ?? 0n], [left.logIndex ?? 0n, right.logIndex ?? 0n]] as const) { if (a < b) return -1; if (a > b) return 1; } return 0; }
