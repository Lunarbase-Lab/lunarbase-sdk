/** Synchronous in-memory quote engine around the ordered reducer. */
import { BPS, parseAddress, type Address, type QuoteRequest } from "@lunarbase-lab/pmm-v2-math";
import { checkpointMatchesDeployment } from "../bootstrap.js";
import { ownDeploymentConfig } from "../ownership.js";
import { decodeCoreEvent } from "../protocol/abi.js";
import { QuoteReducer } from "../state/reducer.js";
import { Commitment, IndexerError, MATH_COMPATIBILITY_VERSION } from "../model.js";
import type {
  BootstrapSnapshot,
  ChainCursor,
  ChainUpdate,
  Checkpoint,
  ClientBatchQuote,
  ClientQuote,
  DeploymentConfig,
  ContractLog,
  IndexerHealth,
} from "../model.js";
import { compareCursor, updateCursor } from "../source.js";

/** Provider-neutral quote engine. No method on its hot path performs I/O. */
export class QuoteIndexer {
  /** Canonical Core identity reused without per-log string allocation. */
  private readonly coreAddress: Address;
  private constructor(
    /** Ordered owner of current quote-critical state and cursor. */
    private reducer: QuoteReducer,
    /** Immutable deployment identity used for compatibility checks. */
    deployment: DeploymentConfig,
    /** Canonical state boundary already represented by the reducer. */
    private canonicalFloor?: ChainCursor,
  ) {
    this.deployment = ownDeploymentConfig(deployment);
    this.coreAddress = this.deployment.core;
  }

  /** Owned immutable deployment identity used for compatibility checks. */
  private readonly deployment: DeploymentConfig;

  /** Builds a ready indexer from one coherent source snapshot. */
  static fromSnapshot(snapshot: BootstrapSnapshot, deployment: DeploymentConfig): QuoteIndexer {
    if (snapshot.cursor.chainId !== deployment.chainId)
      throw new IndexerError("SOURCE", "snapshot cursor chain id mismatch");
    validateVerifiedRouterSnapshot(snapshot, deployment);
    const indexer = new QuoteIndexer(
      new QuoteReducer(snapshot.state, deployment.feeClass, snapshot.verifiedRouter),
      deployment,
      {
        ...snapshot.cursor,
      },
    );
    indexer.verifyImplementation(snapshot);
    indexer.reducer.bootstrap(snapshot.cursor);
    return indexer;
  }

  /** Restores a compatible checkpoint. */
  static fromCheckpoint(checkpoint: Checkpoint, deployment: DeploymentConfig): QuoteIndexer {
    if (!checkpointMatchesDeployment(checkpoint, deployment))
      throw new IndexerError("CODE_HASH_MISMATCH", "checkpoint deployment or state mismatch");
    if (deployment.verifiedRouter !== undefined)
      throw new IndexerError("INVALID_REQUEST", "verified-router mode requires a fresh chain snapshot");
    return new QuoteIndexer(QuoteReducer.fromCheckpoint(checkpoint, deployment.feeClass), deployment, {
      ...checkpoint.cursor,
    });
  }

  /** Atomically replaces state after snapshot/recovery and replays handoff updates. */
  installSnapshot(snapshot: BootstrapSnapshot, buffered: readonly ChainUpdate[]): void {
    const replacement = QuoteIndexer.fromSnapshot(snapshot, this.deployment);
    replacement.replayHandoff(buffered, snapshot.cursor);
    replacement.reducer.publishReady();
    this.reducer = replacement.reducer;
    this.canonicalFloor = replacement.canonicalFloor ? { ...replacement.canonicalFloor } : undefined;
  }

  /** Applies buffered subscription messages newer than the installed state. */
  replayHandoff(buffered: readonly ChainUpdate[], snapshotCursor: import("../model.js").ChainCursor): void {
    const ordered = [...buffered].sort((left, right) => {
      const a = updateCursor(left);
      const b = updateCursor(right);
      if (!a || !b) return left.kind.localeCompare(right.kind);
      return compareCursor(a, b);
    });
    for (const update of ordered) {
      try {
        if (update.kind === "Gap") throw new IndexerError("GAP", update.reason);
        if (update.kind === "Reorg") throw new IndexerError("GAP", "reorg during snapshot handoff");
        this.validateCoreLogIdentity(update);
        const cursor = updateCursor(update);
        if (!cursor) continue;
        if (snapshotCovers(cursor, snapshotCursor)) continue;
        this.applyCoreUpdate(update);
      } catch (error) {
        this.reducer.markNotReady();
        throw error;
      }
    }
  }

  /** Records a completed canonical recovery range. */
  setCanonicalFloor(cursor: ChainCursor): void {
    if (cursor.chainId !== this.deployment.chainId) {
      this.reducer.markNotReady();
      throw new IndexerError("REDUCER", "canonical floor chain id mismatch");
    }
    this.canonicalFloor = { ...cursor };
  }

  /** Applies one normalized update through the pinned Core ABI decoder. */
  applyCoreUpdate(update: ChainUpdate): void {
    this.validateCoreLogIdentity(update);
    try {
      switch (update.kind) {
        case "Log": {
          if (update.log.removed) throw new IndexerError("GAP", "removed log requires canonical recovery");
          if (this.canonicalFloor && canonicalFloorCoversLog(update.log.cursor, this.canonicalFloor)) break;
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
      implementationCodeHash: this.deployment.expectedImplementationCodeHash,
      mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
      feeClass: this.deployment.feeClass,
      verifiedRouter: this.reducer.verifiedRouter(),
    };
  }

  /** Computes at most 256 quotes from one synchronous state snapshot. */
  quoteMany(requests: readonly QuoteRequest[]): ClientBatchQuote {
    if (requests.length > 256) throw new IndexerError("INVALID_REQUEST", "quoteMany accepts at most 256 requests");
    const cursor = this.requireCursor();
    return {
      cursor,
      executionBlockNumber: cursor.executionBlockNumber,
      results: this.reducer.quoteMany(requests),
      implementationCodeHash: this.deployment.expectedImplementationCodeHash,
      mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
      feeClass: this.deployment.feeClass,
      verifiedRouter: this.reducer.verifiedRouter(),
    };
  }

  /** Returns readiness and compatibility metadata. */
  health(): IndexerHealth {
    const cursor = this.reducer.cursor();
    return {
      ready: this.reducer.isReady(),
      cursor,
      commitment: cursor?.commitment ?? Commitment.Realtime,
      implementationCodeHash: this.deployment.expectedImplementationCodeHash,
      mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
      feeClass: this.deployment.feeClass,
      verifiedRouter: this.reducer.verifiedRouter(),
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

  private validateCoreLogIdentity(update: ChainUpdate): void {
    if (update.kind !== "Log") return;
    try {
      validateCoreLogIdentity(update.log, this.coreAddress, this.deployment.chainId);
    } catch (error) {
      this.reducer.markNotReady();
      throw error;
    }
  }

  private requireCursor() {
    const cursor = this.reducer.cursor();
    if (!cursor) throw new IndexerError("NO_CURSOR", "indexer has no cursor");
    return cursor;
  }

  private verifyImplementation(snapshot: BootstrapSnapshot): void {
    if (
      snapshot.implementation.toLowerCase() !== this.deployment.expectedImplementation.toLowerCase() ||
      snapshot.implementationCodeHash.toLowerCase() !== this.deployment.expectedImplementationCodeHash.toLowerCase()
    )
      throw new IndexerError("CODE_HASH_MISMATCH", "snapshot implementation identity mismatch");
  }
}

function validateVerifiedRouterSnapshot(snapshot: BootstrapSnapshot, deployment: DeploymentConfig): void {
  const expected = deployment.verifiedRouter;
  const actual = snapshot.verifiedRouter;
  if (expected === undefined && actual === undefined) return;
  try {
    if (
      expected !== undefined &&
      actual !== undefined &&
      parseAddress(expected) === parseAddress(actual.router) &&
      actual.partnerFeeBps.size <= snapshot.state.lanes.size + 1 &&
      [...actual.partnerFeeBps.entries()].every(([asset, fee]) => {
        const parsedAsset = parseAddress(asset);
        return (
          (parsedAsset === parseAddress(snapshot.state.cash) || snapshot.state.lanes.has(parsedAsset)) &&
          Number.isInteger(fee) &&
          fee >= 0 &&
          BigInt(fee) <= BPS
        );
      })
    )
      return;
  } catch {
    // Normalize malformed snapshot primitives to the stable source error below.
  }
  throw new IndexerError("SOURCE", "snapshot verified-router policy does not match deployment");
}
/** Validates normalized log identity before any ordering shortcut or decode. */
export function validateCoreLogIdentity(log: ContractLog, expectedCore: Address, expectedChainId: bigint): void {
  if (log.address !== expectedCore)
    throw new IndexerError("REDUCER", "contract log address does not match deployment Core");
  if (log.cursor.chainId !== expectedChainId)
    throw new IndexerError("REDUCER", "contract log cursor chain id mismatch");
}

function snapshotCovers(update: ChainCursor, snapshot: ChainCursor): boolean {
  if (update.chainId !== snapshot.chainId) throw new IndexerError("REDUCER", "cursor chain id mismatch");
  if (update.blockNumber < snapshot.blockNumber) return true;
  if (update.blockNumber > snapshot.blockNumber) return false;
  if (update.blockHash === undefined || snapshot.blockHash === undefined)
    throw new IndexerError("GAP", "same-block handoff has no hash identity; canonical recovery required");
  return update.blockHash.toLowerCase() === snapshot.blockHash.toLowerCase();
}

function canonicalFloorCoversLog(update: ChainCursor, floor: ChainCursor): boolean {
  if (update.chainId !== floor.chainId) throw new IndexerError("REDUCER", "cursor chain id mismatch");
  if (update.blockNumber < floor.blockNumber) return true;
  if (update.blockNumber > floor.blockNumber) return false;
  if (update.blockHash === undefined || floor.blockHash === undefined)
    throw new IndexerError("GAP", "same-block realtime log has no canonical hash identity");
  if (update.blockHash.toLowerCase() !== floor.blockHash.toLowerCase())
    throw new IndexerError("REDUCER", "block hash mismatch");
  const floorIsBlockComplete = floor.transactionIndex === undefined && floor.logIndex === undefined;
  return floorIsBlockComplete || compareCursor(update, floor) <= 0;
}
