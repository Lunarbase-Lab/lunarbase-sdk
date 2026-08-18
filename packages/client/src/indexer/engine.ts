/** Synchronous in-memory quote engine around the ordered reducer. */
import { BPS, parseAddress, type Address, type QuoteRequest } from "@lunarbase-lab/pmm-v2-math";
import { checkpointMatchesDeployment } from "../bootstrap.js";
import { ownDeploymentConfig } from "../ownership.js";
import { decodeCoreEvent } from "../protocol/abi.js";
import { QuoteReducer } from "../state/reducer.js";
import { Commitment, IndexerError, MATH_COMPATIBILITY_VERSION } from "../model.js";
import type {
  BlockRef,
  BootstrapSnapshot,
  ChainCorrection,
  ChainCursor,
  ChainUpdate,
  Checkpoint,
  ClientBatchQuote,
  ClientQuote,
  DeploymentConfig,
  ContractLog,
  IndexerCorrectionMetrics,
  IndexerHealth,
  IndexerLifecycleEvent,
  IndexerLifecycleListener,
} from "../model.js";
import { updateCursor } from "../source.js";
import { correctionFingerprint } from "./correction_fingerprint.js";
import {
  CorrectionJournal,
  DEFAULT_CORRECTION_HISTORY_BLOCKS,
  DEFAULT_CORRECTION_HISTORY_BYTES,
  type CorrectionJournalLimits,
} from "./correction_journal.js";
import { validateCorrectionEnvelope, validateCorrectionState } from "./correction_validation.js";
import {
  canonicalFloorCoversLog,
  canonicalFloorMatchesCurrent,
  cursorCoversCorrectionTip,
  cursorHasIdentity,
  sameCursorIdentity,
  snapshotCovers,
  validateCoreLogIdentity,
  validateSnapshotCursor,
} from "./cursor_policy.js";
import { orderHandoffUpdates } from "./handoff_order.js";
import { LifecyclePublisher } from "./lifecycle_publisher.js";
import { FinalityGuard } from "./finality_guard.js";

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
    /** Count and byte budget for compact optimistic before-images. */
    correctionLimits: CorrectionJournalLimits = defaultCorrectionLimits(),
  ) {
    this.deployment = ownDeploymentConfig(deployment);
    this.correctionLimits = ownCorrectionLimits(correctionLimits);
    this.coreAddress = this.deployment.core;
    const floor = canonicalFloor ?? reducer.cursor();
    if (!floor) throw new IndexerError("NO_CURSOR", "correction journal requires an initial cursor");
    this.correctionJournal = new CorrectionJournal({ cursor: { ...floor } }, this.correctionLimits);
  }

  /** Owned immutable deployment identity used for compatibility checks. */
  private readonly deployment: DeploymentConfig;
  /** Owned immutable correction budgets reused by recovery snapshots. */
  private readonly correctionLimits: CorrectionJournalLimits;

  /** Compact state before-images retained for bounded optimistic rollback. */
  private correctionJournal: CorrectionJournal;
  /** Canonical state owner used to avoid persisting an incomplete optimistic head. */
  private stableReducer?: QuoteReducer;
  /** Bounded asynchronous observer surface outside reducer execution. */
  private readonly lifecycle = new LifecyclePublisher();
  /** Successful corrections since construction. */
  private appliedCorrections = 0;
  /** Completed journal evictions retained across canonical snapshot replacement. */
  private journalEvictionOffset = 0;
  /** Exact last correction identity retained without its payload. */
  private lastAppliedCorrectionFingerprint?: string;
  /** New-tip identity paired with the compact retry fingerprint. */
  private lastAppliedCorrectionTip?: ChainCursor;
  /** Highest immutable finalized identity that corrections may not cross. */
  private finality = new FinalityGuard();

  /** Builds a ready indexer from one coherent source snapshot. */
  static fromSnapshot(
    snapshot: BootstrapSnapshot,
    deployment: DeploymentConfig,
    correctionLimits = defaultCorrectionLimits(),
  ): QuoteIndexer {
    validateSnapshotCursor(snapshot.cursor, deployment.chainId);
    validateVerifiedRouterSnapshot(snapshot, deployment);
    const indexer = new QuoteIndexer(
      new QuoteReducer(snapshot.state, deployment.feeClass, snapshot.verifiedRouter),
      deployment,
      {
        ...snapshot.cursor,
      },
      correctionLimits,
    );
    indexer.verifyImplementation(snapshot);
    indexer.reducer.bootstrap(snapshot.cursor);
    indexer.stableReducer = indexer.reducer.fork();
    indexer.finality.observe(snapshot.cursor);
    return indexer;
  }

  /** Restores a compatible checkpoint. */
  static fromCheckpoint(
    checkpoint: Checkpoint,
    deployment: DeploymentConfig,
    correctionLimits = defaultCorrectionLimits(),
  ): QuoteIndexer {
    if (!checkpointMatchesDeployment(checkpoint, deployment))
      throw new IndexerError("CODE_HASH_MISMATCH", "checkpoint deployment or state mismatch");
    if (deployment.verifiedRouter !== undefined)
      throw new IndexerError("INVALID_REQUEST", "verified-router mode requires a fresh chain snapshot");
    const indexer = new QuoteIndexer(
      QuoteReducer.fromCheckpoint(checkpoint, deployment.feeClass),
      deployment,
      { ...checkpoint.cursor },
      correctionLimits,
    );
    indexer.stableReducer = indexer.reducer.fork();
    indexer.finality.observe(checkpoint.cursor);
    return indexer;
  }

  /** Atomically replaces state after snapshot/recovery and replays handoff updates. */
  installSnapshot(snapshot: BootstrapSnapshot, buffered: readonly ChainUpdate[]): void {
    this.finality.validateSnapshot(snapshot.cursor);
    const priorJournalEvictions = saturatingCounter(this.journalEvictionOffset, this.correctionJournal.evictionCount);
    const replacement = QuoteIndexer.fromSnapshot(snapshot, this.deployment, this.correctionLimits);
    replacement.finality.retain(this.finality.cursor());
    if (
      this.lastAppliedCorrectionFingerprint &&
      this.lastAppliedCorrectionTip &&
      snapshotCovers(this.lastAppliedCorrectionTip, snapshot.cursor)
    ) {
      replacement.lastAppliedCorrectionFingerprint = this.lastAppliedCorrectionFingerprint;
      replacement.lastAppliedCorrectionTip = { ...this.lastAppliedCorrectionTip };
    }
    const stagedLifecycle = new LifecyclePublisher();
    replacement.replayHandoff(buffered, snapshot.cursor, stagedLifecycle);
    replacement.reducer.publishReady();
    this.reducer = replacement.reducer;
    this.correctionJournal = replacement.correctionJournal;
    this.journalEvictionOffset = saturatingCounter(priorJournalEvictions, replacement.journalEvictionOffset);
    this.appliedCorrections = saturatingCounter(this.appliedCorrections, replacement.appliedCorrections);
    this.stableReducer = replacement.stableReducer;
    this.lastAppliedCorrectionFingerprint = replacement.lastAppliedCorrectionFingerprint;
    this.lastAppliedCorrectionTip = replacement.lastAppliedCorrectionTip
      ? { ...replacement.lastAppliedCorrectionTip }
      : undefined;
    this.finality = replacement.finality;
    this.canonicalFloor = replacement.canonicalFloor ? { ...replacement.canonicalFloor } : undefined;
    stagedLifecycle.flushInto(this.lifecycle);
  }

  /** Applies buffered subscription messages newer than the installed state. */
  replayHandoff(
    buffered: readonly ChainUpdate[],
    snapshotCursor: import("../model.js").ChainCursor,
    stagedLifecycle?: LifecyclePublisher,
  ): void {
    const ordered = orderHandoffUpdates(buffered);
    for (const update of ordered) {
      try {
        if (update.kind === "Gap") throw new IndexerError("GAP", update.reason);
        if (update.kind === "Reorg") throw new IndexerError("GAP", "reorg during snapshot handoff");
        if (update.kind === "Log") this.validateHandoffLog(update.log);
        if (update.kind === "Correction") {
          this.validateHandoffCorrection(update.correction);
          if (snapshotCovers(update.correction.newTip.cursor, snapshotCursor)) {
            const notice = this.observeCoveredCorrection(update.correction);
            if (notice) {
              if (stagedLifecycle) stagedLifecycle.stage(notice);
              else this.lifecycle.publish(notice);
            }
            continue;
          }
          const notice = this.applyCorrection(update.correction, true);
          if (notice) {
            if (stagedLifecycle) stagedLifecycle.stage(notice);
            else this.lifecycle.publish(notice);
          }
          continue;
        }
        const cursor = updateCursor(update);
        if (!cursor) continue;
        if (snapshotCovers(cursor, snapshotCursor)) {
          this.observeFinalized(cursor, false);
          continue;
        }
        this.applyCoreUpdate(update);
      } catch (error) {
        this.reducer.markNotReady();
        throw error;
      }
    }
  }

  /** Records a completed canonical recovery range. */
  setCanonicalFloor(cursor: ChainCursor): void {
    const current = this.reducer.cursor();
    if (!current || !canonicalFloorMatchesCurrent(cursor, current)) {
      this.reducer.markNotReady();
      throw new IndexerError("GAP", "canonical floor does not match current reducer state");
    }
    this.canonicalFloor = { ...cursor };
    this.stableReducer = this.reducer.fork();
    this.lastAppliedCorrectionFingerprint = undefined;
    this.lastAppliedCorrectionTip = undefined;
    this.journalEvictionOffset = saturatingCounter(this.journalEvictionOffset, this.correctionJournal.evictionCount);
    this.correctionJournal = new CorrectionJournal({ cursor: { ...cursor } }, this.correctionLimits);
  }

  /** Applies one normalized update through the pinned Core ABI decoder. */
  applyCoreUpdate(update: ChainUpdate): void {
    try {
      switch (update.kind) {
        case "Log":
          if (this.applyLog(this.reducer, this.correctionJournal, update.log, true)) {
            this.lastAppliedCorrectionFingerprint = undefined;
            this.lastAppliedCorrectionTip = undefined;
          }
          if (update.log.cursor.commitment === Commitment.Finalized) this.reducer.observeHead(update.log.cursor);
          this.observeFinalized(update.log.cursor, false);
          break;
        case "Head":
          this.reducer.observeHead(update.head.cursor);
          if (cursorHasIdentity(this.reducer.cursor(), update.head.cursor)) {
            this.correctionJournal.observe(update.head.cursor);
            this.observeFinalized(update.head.cursor, true);
          } else {
            this.observeFinalized(update.head.cursor, false);
          }
          break;
        case "Correction":
          {
            const notice = this.applyCorrection(update.correction);
            if (notice) this.lifecycle.publish(notice);
          }
          break;
        case "Reorg":
          throw new IndexerError("GAP", "reorg requires canonical recovery");
        case "Gap":
          throw new IndexerError("GAP", update.reason);
      }
    } catch (error) {
      this.reducer.markNotReady();
      const normalized =
        error instanceof IndexerError
          ? error
          : new IndexerError("REDUCER", error instanceof Error ? error.message : "reducer transition failed");
      this.lifecycle.publish({ kind: "Gap", cursor: updateCursor(update), reason: normalized.message });
      throw normalized;
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

  /** Returns compact correction counters for load and memory monitoring. */
  correctionMetrics(): IndexerCorrectionMetrics {
    return {
      appliedCorrections: this.appliedCorrections,
      journalBlocks: this.correctionJournal.blockCount,
      journalRetainedBytes: this.correctionJournal.retainedBytes,
      journalEvictions: saturatingCounter(this.journalEvictionOffset, this.correctionJournal.evictionCount),
    };
  }

  /** Observes correction/gap lifecycle asynchronously outside reducer work. */
  onLifecycle(listener: IndexerLifecycleListener): () => void {
    return this.lifecycle.subscribe(listener);
  }

  /** Produces only the latest complete canonical checkpoint. */
  checkpoint(): Checkpoint | undefined {
    return this.stableReducer?.checkpoint(this.deployment);
  }

  /** Identity-only checkpoint used to prove the monotonic finalized floor. */
  finalizedCheckpoint(): Checkpoint | undefined {
    return this.finality.checkpoint(this.checkpoint());
  }

  /** Marks quotes unavailable during shutdown or recovery. */
  markNotReady(): void {
    this.reducer.markNotReady();
  }

  private applyLog(
    reducer: QuoteReducer,
    journal: CorrectionJournal,
    log: ContractLog,
    respectCanonicalFloor: boolean,
  ): boolean {
    validateCoreLogIdentity(log, this.coreAddress, this.deployment.chainId);
    if (log.removed) throw new IndexerError("GAP", "removed log requires a resolved correction");
    if (respectCanonicalFloor && this.canonicalFloor && canonicalFloorCoversLog(log.cursor, this.canonicalFloor))
      return false;
    const event = decodeCoreEvent(log);
    if (!event) return false;
    if (respectCanonicalFloor) journal.validateMutation(log.cursor);
    const undo = reducer.apply(log.cursor, event);
    if (undo) journal.record(log.cursor, undo);
    return undo !== undefined;
  }

  private applyCorrection(correction: ChainCorrection, envelopeValidated = false): IndexerLifecycleEvent | undefined {
    if (!envelopeValidated) validateCorrectionEnvelope(correction, this.coreAddress, this.deployment.chainId);
    if (!this.reducer.isReady())
      throw new IndexerError("GAP", "correction cannot repair invalid quote state; snapshot recovery required");
    const fingerprint = correctionFingerprint(correction);
    const current = this.reducer.cursor();
    if (
      this.lastAppliedCorrectionFingerprint === fingerprint &&
      this.lastAppliedCorrectionTip &&
      sameCursorIdentity(this.lastAppliedCorrectionTip, correction.newTip.cursor) &&
      current &&
      cursorCoversCorrectionTip(current, correction.newTip.cursor)
    )
      return undefined;
    this.finality.validateCorrection(correction);
    const shouldApply = validateCorrectionState(correction, current);
    if (!shouldApply) {
      if (this.lastAppliedCorrectionFingerprint === fingerprint) return undefined;
      throw new IndexerError("GAP", "correction new tip has no matching applied envelope");
    }
    const candidate = this.correctionJournal.candidate(this.reducer, correction.commonAncestor, correction.oldBranch);
    const replacement = candidate.reducer;
    const journal = candidate.journal;
    for (const log of correction.replacementLogs) this.applyLog(replacement, journal, log, false);
    replacement.observeHead(correction.newTip.cursor);
    for (const block of correction.newBranch) journal.observe(block.cursor);
    this.reducer = replacement;
    this.correctionJournal = journal;
    this.lastAppliedCorrectionFingerprint = fingerprint;
    this.lastAppliedCorrectionTip = { ...correction.newTip.cursor };
    this.observeFinalized(correction.newTip.cursor, true);
    this.appliedCorrections = Math.min(Number.MAX_SAFE_INTEGER, this.appliedCorrections + 1);
    return correctionNotice(correction);
  }

  private observeCoveredCorrection(correction: ChainCorrection): IndexerLifecycleEvent | undefined {
    const fingerprint = correctionFingerprint(correction);
    if (this.lastAppliedCorrectionTip && sameCursorIdentity(this.lastAppliedCorrectionTip, correction.newTip.cursor)) {
      if (this.lastAppliedCorrectionFingerprint === fingerprint) return undefined;
      throw new IndexerError("GAP", "snapshot-covered correction conflicts with the retained correction identity");
    }
    if (
      correction.oldTip.cursor.commitment === Commitment.Finalized ||
      correction.oldBranch.some((block) => block.cursor.commitment === Commitment.Finalized)
    )
      throw new IndexerError("GAP", "snapshot-covered correction cannot replace finalized branch state");
    this.observeFinalized(correction.newTip.cursor, true);
    this.lastAppliedCorrectionFingerprint = fingerprint;
    this.lastAppliedCorrectionTip = { ...correction.newTip.cursor };
    this.appliedCorrections = Math.min(Number.MAX_SAFE_INTEGER, this.appliedCorrections + 1);
    return correctionNotice(correction);
  }

  private validateHandoffLog(log: ContractLog): void {
    try {
      validateCoreLogIdentity(log, this.coreAddress, this.deployment.chainId);
    } catch (error) {
      throw permanentHandoffError(error);
    }
  }

  private validateHandoffCorrection(correction: ChainCorrection): void {
    try {
      validateCorrectionEnvelope(correction, this.coreAddress, this.deployment.chainId);
    } catch (error) {
      throw permanentHandoffError(error);
    }
  }

  private observeFinalized(cursor: ChainCursor, blockComplete: boolean): void {
    if (!this.finality.observe(cursor) || !blockComplete || !cursorHasIdentity(this.reducer.cursor(), cursor)) return;
    this.stableReducer = this.reducer.fork();
    this.canonicalFloor = { ...cursor };
    this.journalEvictionOffset = saturatingCounter(this.journalEvictionOffset, this.correctionJournal.evictionCount);
    this.correctionJournal = new CorrectionJournal({ cursor: { ...cursor } }, this.correctionLimits);
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

function defaultCorrectionLimits(): CorrectionJournalLimits {
  return {
    blockCapacity: DEFAULT_CORRECTION_HISTORY_BLOCKS,
    byteCapacity: DEFAULT_CORRECTION_HISTORY_BYTES,
  };
}

function ownCorrectionLimits(limits: CorrectionJournalLimits): CorrectionJournalLimits {
  return Object.freeze({ blockCapacity: limits.blockCapacity, byteCapacity: limits.byteCapacity });
}

function saturatingCounter(left: number, right: number): number {
  return left >= Number.MAX_SAFE_INTEGER - right ? Number.MAX_SAFE_INTEGER : left + right;
}

function cloneBlock(block: BlockRef): BlockRef {
  return { cursor: { ...block.cursor }, parentHash: block.parentHash };
}

function correctionNotice(correction: ChainCorrection): IndexerLifecycleEvent {
  return {
    kind: "CorrectionApplied",
    commonAncestor: cloneBlock(correction.commonAncestor),
    oldTip: cloneBlock(correction.oldTip),
    newTip: cloneBlock(correction.newTip),
    replacementLogCount: correction.replacementLogs.length,
  };
}

function permanentHandoffError(error: unknown): IndexerError {
  return new IndexerError(
    "INVALID_REQUEST",
    error instanceof Error ? error.message : "snapshot handoff contains an invalid update",
  );
}
