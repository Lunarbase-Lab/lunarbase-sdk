import { quote, type QuoteOutcome, type QuoteRequest, type QuoteState } from "@lunarbase/math";
import { decodeCoreEvent } from "../protocol/abi.js";
import { fetchBootstrapSnapshot, updateOrder } from "../bootstrap.js";
import type {
  BootstrapSnapshot,
  ChainCursor,
  ChainEventSource,
  ChainUpdate,
  ClientQuote,
  ContractFilter,
  DeploymentConfig,
  FreshnessPolicy,
  IndexerHealth,
  LogDecoder,
  SnapshotProvider,
} from "../model.js";
import { Commitment, commitmentRank, IndexerError, MATH_COMPATIBILITY_VERSION } from "../model.js";
import { QuoteReducer } from "../state/reducer.js";

export class QuoteIndexer {
  readonly mathCompatibilityVersion = MATH_COMPATIBILITY_VERSION;
  private commitment: Commitment = Commitment.Realtime;
  private lastObservedAt = BigInt(Date.now());
  /** Creates an indexer around a supplied reducer and deployment code hash. */
  constructor(
    readonly expectedRuntimeCodeHash: string,
    public reducer: QuoteReducer,
  ) {}
  /** Creates an indexer from an initial quote state. */
  static create(expectedCodeHash: string, state: QuoteState): QuoteIndexer {
    return new QuoteIndexer(expectedCodeHash, new QuoteReducer(state));
  }
  /** Restores a checkpoint only when schema, math version, and code hash match. */
  static fromCheckpoint(checkpoint: import("../model.js").Checkpoint, expectedCodeHash: string): QuoteIndexer {
    if (
      checkpoint.schemaVersion !== 2n ||
      checkpoint.mathCompatibilityVersion !== MATH_COMPATIBILITY_VERSION ||
      checkpoint.expectedRuntimeCodeHash.toLowerCase() !== expectedCodeHash.toLowerCase()
    )
      throw new IndexerError("CODE_HASH_MISMATCH", "checkpoint compatibility mismatch");
    const indexer = new QuoteIndexer(expectedCodeHash, QuoteReducer.fromCheckpoint(checkpoint));
    indexer.commitment = checkpoint.cursor.commitment;
    indexer.lastObservedAt = BigInt(Date.now());
    return indexer;
  }
  /** Establishes the initial snapshot cursor and publishes readiness. */
  bootstrap(snapshotCursor: ChainCursor): void {
    this.reducer.bootstrap(snapshotCursor);
    this.commitment = snapshotCursor.commitment;
    this.lastObservedAt = BigInt(Date.now());
  }
  /** Fetches, validates, and installs a provider snapshot with buffered updates. */
  async bootstrapFromProvider(
    provider: SnapshotProvider,
    config: DeploymentConfig,
    laneAssets: readonly import("@lunarbase/math").Address[],
    routers: readonly import("@lunarbase/math").Address[],
    buffered: readonly ChainUpdate[],
  ): Promise<void> {
    const snapshot = await fetchBootstrapSnapshot(provider, config, laneAssets, routers, this.expectedRuntimeCodeHash);
    this.bootstrapNormalized(snapshot, [...buffered]);
  }
  /** Performs the ordered snapshot-to-realtime handoff and fails closed on gaps. */
  bootstrapNormalized(snapshot: BootstrapSnapshot, buffered: ChainUpdate[]): void {
    if (snapshot.runtimeCodeHash.toLowerCase() !== this.expectedRuntimeCodeHash.toLowerCase())
      throw new IndexerError("CODE_HASH_MISMATCH", "snapshot code hash mismatch");
    const block = snapshot.cursor.blockNumber;
    const chain = snapshot.cursor.chainId;
    buffered.sort(updateOrder);
    this.reducer = new QuoteReducer(snapshot.state);
    this.reducer.bootstrap(snapshot.cursor);
    this.commitment = snapshot.cursor.commitment;
    this.lastObservedAt = BigInt(Date.now());
    for (const update of buffered) {
      if (update.kind === "Gap") {
        this.reducer.markNotReady();
        throw new IndexerError("GAP", update.reason);
      }
      if (update.kind === "Reorg") {
        this.reducer.markNotReady();
        throw new IndexerError("GAP", "reorg during bootstrap handoff");
      }
      if (update.kind === "SourceHealth") {
        if (!update.healthy) {
          this.reducer.markNotReady();
          throw new IndexerError("GAP", update.detail);
        }
        continue;
      }
      const cursor = update.kind === "Log" ? update.log.cursor : update.cursor;
      if (cursor.chainId !== chain) {
        this.reducer.markNotReady();
        throw new IndexerError("REDUCER", "cursor chain id mismatch");
      }
      if (cursor.blockNumber <= block) continue;
      this.applyCoreUpdate(update);
    }
    this.reducer.publishReady();
  }
  /** Recovers the reducer through canonical backfill from its last cursor. */
  async recoverFromSource(source: ChainEventSource, filter: ContractFilter): Promise<void> {
    const checkpoint = this.reducer.cursor();
    if (!checkpoint) throw new IndexerError("NO_CURSOR", "no canonical cursor");
    this.reducer.markNotReady();
    const head = await source.snapshotCursor();
    if (head.chainId !== checkpoint.chainId) throw new IndexerError("REDUCER", "cursor chain id mismatch");
    if (commitmentRank(head.commitment) < commitmentRank(Commitment.Canonical))
      throw new IndexerError("FRESHNESS_UNAVAILABLE", "canonical recovery head is not proven");
    if (head.blockNumber < checkpoint.blockNumber)
      throw new IndexerError("GAP", "canonical source head regressed below checkpoint");
    const fromBlock =
      checkpoint.transactionIndex === undefined && checkpoint.logIndex === undefined
        ? checkpoint.blockNumber + 1n
        : checkpoint.blockNumber;
    if (fromBlock <= head.blockNumber) {
      const logs = [...(await source.backfill({ fromBlock, toBlock: head.blockNumber, filter }))].sort((left, right) =>
        compareCursor(left.cursor, right.cursor),
      );
      for (const log of logs) this.applyCoreUpdate({ kind: "Log", log });
    }
    this.applyUpdate({ kind: "Head", cursor: head }, () => undefined);
    this.reducer.publishReady();
  }
  /** Applies one normalized update with a caller-provided log decoder. */
  applyUpdate(update: ChainUpdate, decodeLog: LogDecoder): void {
    switch (update.kind) {
      case "Log":
        if (update.log.removed) {
          this.reducer.markNotReady();
          throw new IndexerError("GAP", "removed log requires canonical rebuild");
        }
        {
          const event = decodeLog(update.log);
          if (event) {
            try {
              this.reducer.apply(update.log.cursor, event);
            } catch (error) {
              this.reducer.markNotReady();
              throw new IndexerError("REDUCER", error instanceof Error ? error.message : "reducer error");
            }
            this.commitment = update.log.cursor.commitment;
            this.lastObservedAt = BigInt(Date.now());
          }
        }
        break;
      case "Head":
        try {
          this.reducer.observeHead(update.cursor);
        } catch (error) {
          this.reducer.markNotReady();
          throw new IndexerError("REDUCER", error instanceof Error ? error.message : "head reducer error");
        }
        this.commitment = this.reducer.cursor()?.commitment ?? update.cursor.commitment;
        this.lastObservedAt = BigInt(Date.now());
        break;
      case "Reorg":
        this.reducer.markNotReady();
        throw new IndexerError("GAP", "reorg requires canonical backfill");
      case "Gap":
        this.reducer.markNotReady();
        throw new IndexerError("GAP", update.reason);
      case "SourceHealth":
        if (!update.healthy) this.reducer.markNotReady();
        break;
    }
  }
  /** Applies a normalized update through the pinned Core ABI decoder. */
  applyCoreUpdate(update: ChainUpdate): void {
    if (update.kind === "Log") {
      const event = decodeCoreEvent(update.log);
      this.applyUpdate(update, () => event);
    } else this.applyUpdate(update, () => undefined);
  }
  /** Returns a ready immutable state snapshot. */
  snapshot(): QuoteState {
    if (!this.reducer.isReady()) throw new IndexerError("NOT_READY", "indexer is not ready");
    return this.reducer.state();
  }
  /** Returns the current state snapshot under the explicit alias used by clients. */
  stateSnapshot(): QuoteState {
    return this.snapshot();
  }
  /** Computes a quote without a freshness policy. */
  quote(request: QuoteRequest, executionBlockNumber: bigint): QuoteOutcome {
    const state = this.snapshot();
    return quote(request, { cash: state.cash, executionBlockNumber, stateVersion: state.stateVersion }, state);
  }
  /** Computes a quote after commitment and block-age checks. */
  quoteWithPolicy(request: QuoteRequest, executionBlockNumber: bigint, policy: FreshnessPolicy): ClientQuote {
    const cursor = this.reducer.cursor();
    if (!cursor) throw new IndexerError("NO_CURSOR", "no canonical cursor");
    const rank = (value: Commitment) => (value === Commitment.Realtime ? 0n : value === Commitment.Canonical ? 1n : 2n);
    if (rank(cursor.commitment) < rank(policy.minimumCommitment))
      throw new IndexerError("FRESHNESS_UNAVAILABLE", "requested freshness cannot be proven");
    if (
      policy.maxAgeBlocks !== undefined &&
      executionBlockNumber > cursor.blockNumber &&
      executionBlockNumber - cursor.blockNumber > policy.maxAgeBlocks
    )
      throw new IndexerError("FRESHNESS_UNAVAILABLE", "snapshot is too old");
    const now = BigInt(Date.now());
    return {
      outcome: this.quote(request, executionBlockNumber),
      cursor,
      commitment: cursor.commitment,
      observedAt: this.lastObservedAt,
      ageMilliseconds: now >= this.lastObservedAt ? now - this.lastObservedAt : 0n,
      stale: false,
      contractCodeHash: this.expectedRuntimeCodeHash,
      mathCompatibilityVersion: this.mathCompatibilityVersion,
    };
  }
  /** Computes an exact-input quote using realtime freshness as the default. */
  quoteExactIn(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote {
    return this.quoteWithPolicy({ ...request, mode: "ExactIn" }, executionBlockNumber, {
      minimumCommitment: Commitment.Realtime,
    });
  }
  /** Computes an exact-output quote using realtime freshness as the default. */
  quoteExactOut(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote {
    return this.quoteWithPolicy({ ...request, mode: "ExactOut" }, executionBlockNumber, {
      minimumCommitment: Commitment.Realtime,
    });
  }
  /** Returns a durable checkpoint for the current cursor. */
  checkpoint(): import("../model.js").Checkpoint | undefined {
    return this.reducer.checkpoint(this.expectedRuntimeCodeHash);
  }
  /** Returns the latest observed commitment. */
  currentCommitment(): Commitment {
    return this.commitment;
  }
  /** Returns readiness and compatibility metadata. */
  health(): IndexerHealth {
    return {
      ready: this.reducer.isReady(),
      commitment: this.commitment,
      cursor: this.reducer.cursor(),
      contractCodeHash: this.expectedRuntimeCodeHash,
      mathCompatibilityVersion: this.mathCompatibilityVersion,
    };
  }
  /** Marks the reducer unavailable for fresh quotes. */
  shutdown(): void {
    this.reducer.markNotReady();
  }
}

/** Parameters for starting the connected source/reducer lifecycle. */
function compareCursor(left: ChainCursor, right: ChainCursor): number {
  for (const [a, b] of [
    [left.blockNumber, right.blockNumber],
    [left.transactionIndex ?? 0n, right.transactionIndex ?? 0n],
    [left.logIndex ?? 0n, right.logIndex ?? 0n],
  ] as const) {
    if (a < b) return -1;
    if (a > b) return 1;
  }
  return 0;
}
