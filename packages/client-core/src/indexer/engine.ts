/** Synchronous in-memory quote engine around the ordered reducer. */
import type { QuoteRequest } from "@lunarbase/math";
import { decodeCoreEvent } from "../protocol/abi.js";
import { QuoteReducer } from "../state/reducer.js";
import { Commitment, IndexerError, MATH_COMPATIBILITY_VERSION } from "../model.js";
import type {
  BootstrapSnapshot,
  ChainUpdate,
  Checkpoint,
  ClientBatchQuote,
  ClientQuote,
  DeploymentConfig,
  IndexerHealth,
} from "../model.js";
import { compareCursor, updateCursor } from "../source.js";

/** Provider-neutral quote engine. No method on its hot path performs I/O. */
export class QuoteIndexer {
  private constructor(
    private reducer: QuoteReducer,
    private readonly deployment: DeploymentConfig,
  ) {}

  /** Builds a ready indexer from one coherent source snapshot. */
  static fromSnapshot(snapshot: BootstrapSnapshot, deployment: DeploymentConfig): QuoteIndexer {
    const indexer = new QuoteIndexer(new QuoteReducer(snapshot.state, deployment.router), deployment);
    indexer.verifyCodeHash(snapshot.runtimeCodeHash);
    indexer.reducer.bootstrap(snapshot.cursor);
    return indexer;
  }

  /** Restores an already validated v3 checkpoint. */
  static fromCheckpoint(checkpoint: Checkpoint, deployment: DeploymentConfig): QuoteIndexer {
    return new QuoteIndexer(QuoteReducer.fromCheckpoint(checkpoint), deployment);
  }

  /** Atomically replaces state after snapshot/recovery and replays handoff updates. */
  installSnapshot(snapshot: BootstrapSnapshot, buffered: readonly ChainUpdate[]): void {
    const replacement = QuoteIndexer.fromSnapshot(snapshot, this.deployment);
    replacement.replayHandoff(buffered, snapshot.cursor.blockNumber);
    replacement.reducer.publishReady();
    this.reducer = replacement.reducer;
  }

  /** Applies buffered subscription messages newer than the installed state. */
  replayHandoff(buffered: readonly ChainUpdate[], baseBlock: bigint): void {
    const ordered = [...buffered].sort((left, right) => {
      const a = updateCursor(left);
      const b = updateCursor(right);
      if (!a || !b) return left.kind.localeCompare(right.kind);
      return compareCursor(a, b);
    });
    for (const update of ordered) {
      if (update.kind === "Gap") throw new IndexerError("GAP", update.reason);
      if (update.kind === "Reorg") throw new IndexerError("GAP", "reorg during snapshot handoff");
      const cursor = updateCursor(update);
      if (!cursor) continue;
      if (
        (update.kind === "Log" && cursor.blockNumber <= baseBlock) ||
        (update.kind === "Head" && cursor.blockNumber < baseBlock)
      )
        continue;
      this.applyCoreUpdate(update);
    }
  }

  /** Applies one normalized update through the pinned Core ABI decoder. */
  applyCoreUpdate(update: ChainUpdate): void {
    try {
      switch (update.kind) {
        case "Log": {
          if (update.log.removed) throw new IndexerError("GAP", "removed log requires canonical recovery");
          const event = decodeCoreEvent(update.log);
          if (event) this.reducer.apply(update.log.cursor, event);
          break;
        }
        case "Head":
          this.reducer.observeHead(update.cursor);
          break;
        case "Reorg":
          throw new IndexerError("GAP", "reorg requires canonical recovery");
        case "Gap":
          throw new IndexerError("GAP", update.reason);
      }
    } catch (error) {
      this.reducer.markNotReady();
      if (error instanceof IndexerError) throw error;
      throw new IndexerError("REDUCER", error instanceof Error ? error.message : "reducer transition failed");
    }
  }

  /** Computes one quote from the current cursor without I/O or state cloning. */
  quote(request: QuoteRequest): ClientQuote {
    const cursor = this.requireCursor();
    return {
      outcome: this.reducer.quote(request),
      cursor,
      executionBlockNumber: cursor.executionBlockNumber,
    };
  }

  /** Computes at most 256 quotes from one synchronous state snapshot. */
  quoteMany(requests: readonly QuoteRequest[]): ClientBatchQuote {
    if (requests.length > 256) throw new IndexerError("SOURCE", "quoteMany accepts at most 256 requests");
    const cursor = this.requireCursor();
    return {
      cursor,
      executionBlockNumber: cursor.executionBlockNumber,
      results: this.reducer.quoteMany(requests),
    };
  }

  /** Returns readiness and compatibility metadata. */
  health(): IndexerHealth {
    const cursor = this.reducer.cursor();
    return {
      ready: this.reducer.isReady(),
      cursor,
      commitment: cursor?.commitment ?? Commitment.Realtime,
      contractCodeHash: this.deployment.expectedRuntimeCodeHash,
      mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    };
  }

  /** Produces a deep-cloned checkpoint outside the quote path. */
  checkpoint(): Checkpoint | undefined {
    return this.reducer.checkpoint(this.deployment);
  }

  /** Marks quotes unavailable during shutdown or recovery. */
  markNotReady(): void {
    this.reducer.markNotReady();
  }

  private requireCursor() {
    const cursor = this.reducer.cursor();
    if (!cursor) throw new IndexerError("NO_CURSOR", "indexer has no cursor");
    return cursor;
  }

  private verifyCodeHash(actual: string): void {
    const expected = this.deployment.expectedRuntimeCodeHash;
    if (expected !== "" && !/^0x0+$/.test(expected) && actual.toLowerCase() !== expected.toLowerCase())
      throw new IndexerError("CODE_HASH_MISMATCH", "snapshot code hash mismatch");
  }
}
