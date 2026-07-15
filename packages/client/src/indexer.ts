import { quote, type QuoteOutcome, type QuoteRequest, type QuoteState } from "@lunarbase/math";
import { decodeCoreEvent } from "./abi.js";
import { fetchBootstrapSnapshot, updateOrder } from "./bootstrap.js";
import type { BootstrapSnapshot, ChainCursor, ChainEventSource, ChainUpdate, ClientQuote, ContractFilter, DeploymentConfig, FreshnessPolicy, IndexerHealth, LogDecoder, SnapshotProvider } from "./model.js";
import { Commitment, commitmentRank, IndexerError, MATH_COMPATIBILITY_VERSION } from "./model.js";
import { QuoteReducer } from "./reducer.js";

export class QuoteIndexer {
  readonly mathCompatibilityVersion = MATH_COMPATIBILITY_VERSION;
  private commitment: Commitment = Commitment.Realtime;
  constructor(readonly expectedRuntimeCodeHash: string, public reducer: QuoteReducer) {}
  static create(expectedCodeHash: string, state: QuoteState): QuoteIndexer { return new QuoteIndexer(expectedCodeHash, new QuoteReducer(state)); }
  static fromCheckpoint(checkpoint: import("./model.js").Checkpoint, expectedCodeHash: string): QuoteIndexer { if (checkpoint.schemaVersion !== 2n || checkpoint.mathCompatibilityVersion !== MATH_COMPATIBILITY_VERSION || checkpoint.expectedRuntimeCodeHash.toLowerCase() !== expectedCodeHash.toLowerCase()) throw new IndexerError("CODE_HASH_MISMATCH", "checkpoint compatibility mismatch"); const indexer = new QuoteIndexer(expectedCodeHash, QuoteReducer.fromCheckpoint(checkpoint)); indexer.commitment = checkpoint.cursor.commitment; return indexer; }
  bootstrap(snapshotCursor: ChainCursor): void { this.reducer.bootstrap(snapshotCursor); }
  async bootstrapFromProvider(provider: SnapshotProvider, config: DeploymentConfig, laneAssets: readonly import("@lunarbase/math").Address[], routers: readonly import("@lunarbase/math").Address[], buffered: readonly ChainUpdate[]): Promise<void> { const snapshot = await fetchBootstrapSnapshot(provider, config, laneAssets, routers, this.expectedRuntimeCodeHash); this.bootstrapNormalized(snapshot, [...buffered]); }
  bootstrapNormalized(snapshot: BootstrapSnapshot, buffered: ChainUpdate[]): void { if (snapshot.runtimeCodeHash.toLowerCase() !== this.expectedRuntimeCodeHash.toLowerCase()) throw new IndexerError("CODE_HASH_MISMATCH", "snapshot code hash mismatch"); const block = snapshot.cursor.blockNumber; const chain = snapshot.cursor.chainId; buffered.sort(updateOrder); this.reducer = new QuoteReducer(snapshot.state); this.reducer.bootstrap(snapshot.cursor); this.commitment = snapshot.cursor.commitment; for (const update of buffered) { if (update.kind === "Gap") { this.reducer.markNotReady(); throw new IndexerError("GAP", update.reason); } if (update.kind === "Reorg") { this.reducer.markNotReady(); throw new IndexerError("GAP", "reorg during bootstrap handoff"); } if (update.kind === "SourceHealth") { if (!update.healthy) { this.reducer.markNotReady(); throw new IndexerError("GAP", update.detail); } continue; } const cursor = update.kind === "Log" ? update.log.cursor : update.cursor; if (cursor.chainId !== chain) { this.reducer.markNotReady(); throw new IndexerError("REDUCER", "cursor chain id mismatch"); } if (cursor.blockNumber <= block) continue; this.applyCoreUpdate(update); } this.reducer.publishReady(); }
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
  applyUpdate(update: ChainUpdate, decodeLog: LogDecoder): void { switch (update.kind) { case "Log": if (update.log.removed) { this.reducer.markNotReady(); throw new IndexerError("GAP", "removed log requires canonical rebuild"); } { const event = decodeLog(update.log); if (event) { try { this.reducer.apply(update.log.cursor, event); } catch (error) { this.reducer.markNotReady(); throw new IndexerError("REDUCER", error instanceof Error ? error.message : "reducer error"); } this.commitment = update.log.cursor.commitment; } } break; case "Head": try { this.reducer.observeHead(update.cursor); } catch (error) { this.reducer.markNotReady(); throw new IndexerError("REDUCER", error instanceof Error ? error.message : "head reducer error"); } this.commitment = this.reducer.cursor()?.commitment ?? update.cursor.commitment; break; case "Reorg": this.reducer.markNotReady(); throw new IndexerError("GAP", "reorg requires canonical backfill"); case "Gap": this.reducer.markNotReady(); throw new IndexerError("GAP", update.reason); case "SourceHealth": if (!update.healthy) this.reducer.markNotReady(); break; } }
  applyCoreUpdate(update: ChainUpdate): void { if (update.kind === "Log") { const event = decodeCoreEvent(update.log); this.applyUpdate(update, () => event); } else this.applyUpdate(update, () => undefined); }
  snapshot(): QuoteState { if (!this.reducer.isReady()) throw new IndexerError("NOT_READY", "indexer is not ready"); return this.reducer.state(); }
  stateSnapshot(): QuoteState { return this.snapshot(); }
  quote(request: QuoteRequest, executionBlockNumber: bigint): QuoteOutcome { const state = this.snapshot(); return quote(request, { cash: state.cash, executionBlockNumber, stateVersion: state.stateVersion }, state); }
  quoteWithPolicy(request: QuoteRequest, executionBlockNumber: bigint, policy: FreshnessPolicy): ClientQuote { const cursor = this.reducer.cursor(); if (!cursor) throw new IndexerError("NO_CURSOR", "no canonical cursor"); const rank = (value: Commitment) => value === Commitment.Realtime ? 0n : value === Commitment.Canonical ? 1n : 2n; if (rank(cursor.commitment) < rank(policy.minimumCommitment)) throw new IndexerError("FRESHNESS_UNAVAILABLE", "requested freshness cannot be proven"); if (policy.maxAgeBlocks !== undefined && executionBlockNumber > cursor.blockNumber && executionBlockNumber - cursor.blockNumber > policy.maxAgeBlocks) throw new IndexerError("FRESHNESS_UNAVAILABLE", "snapshot is too old"); return { outcome: this.quote(request, executionBlockNumber), cursor, commitment: cursor.commitment, observedAt: BigInt(Date.now()), ageMilliseconds: 0n, stale: false, contractCodeHash: this.expectedRuntimeCodeHash, mathCompatibilityVersion: this.mathCompatibilityVersion }; }
  quoteExactIn(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote { return this.quoteWithPolicy({ ...request, mode: "ExactIn" }, executionBlockNumber, { minimumCommitment: Commitment.Realtime }); }
  quoteExactOut(request: QuoteRequest, executionBlockNumber: bigint): ClientQuote { return this.quoteWithPolicy({ ...request, mode: "ExactOut" }, executionBlockNumber, { minimumCommitment: Commitment.Realtime }); }
  checkpoint(): import("./model.js").Checkpoint | undefined { return this.reducer.checkpoint(this.expectedRuntimeCodeHash); }
  currentCommitment(): Commitment { return this.commitment; }
  health(): IndexerHealth { return { ready: this.reducer.isReady(), commitment: this.commitment, cursor: this.reducer.cursor(), contractCodeHash: this.expectedRuntimeCodeHash, mathCompatibilityVersion: this.mathCompatibilityVersion }; }
  shutdown(): void { this.reducer.markNotReady(); }
}

function compareCursor(left: ChainCursor, right: ChainCursor): number { for (const [a, b] of [[left.blockNumber, right.blockNumber], [left.transactionIndex ?? 0n, right.transactionIndex ?? 0n], [left.logIndex ?? 0n, right.logIndex ?? 0n]] as const) { if (a < b) return -1; if (a > b) return 1; } return 0; }
