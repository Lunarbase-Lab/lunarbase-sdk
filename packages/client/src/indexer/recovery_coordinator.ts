/** Reducer-owned canonical recovery with bounded transactional handoff staging. */
import {
  Commitment,
  IndexerError,
  commitmentRank,
  type ChainCursor,
  type ChainDataSource,
  type Checkpoint,
} from "../model.js";
import { compareCursor, updateCursor } from "../source.js";
import type { ClientConnectConfig } from "./connected.js";
import { validateCorrectionEnvelope } from "./correction_validation.js";
import { validateCoreLogIdentity } from "./cursor_policy.js";
import { QuoteIndexer } from "./engine.js";
import { delay, SourceActivity } from "./source_task.js";
import { BoundedUpdateQueue, type DequeuedUpdate } from "./update_queue.js";
import { withDeadline } from "./lifecycle.js";

const RECOVERY_IDENTITY_FIXED_SLOTS = 8;

/** Serializes explicit and fault-triggered recovery through the sole reducer owner. */
export class RecoveryCoordinator {
  private current?: Promise<number>;
  private requested?: ManualRecoveryRequest;

  constructor(
    private readonly indexer: QuoteIndexer,
    private readonly source: ChainDataSource,
    private readonly config: ClientConnectConfig,
    private readonly queue: BoundedUpdateQueue,
    private readonly activity: SourceActivity,
    private readonly signal: AbortSignal,
  ) {}

  request(): Promise<void> {
    if (this.queue.closed || this.signal.aborted)
      return Promise.reject(new IndexerError("SOURCE", "ordered reducer is not available for recovery"));
    if (this.current) return this.current.then(() => undefined);
    if (this.requested) return this.requested.promise;
    const coverage = this.indexer.health().cursor;
    this.indexer.markNotReady();
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<void>((accept, fail) => {
      resolve = accept;
      reject = fail;
    });
    this.requested = {
      barrier: this.queue.acceptedSequence,
      coverage: coverage && { ...coverage },
      promise,
      resolve,
      reject,
    };
    this.queue.wakeConsumer();
    return promise;
  }

  get requestedBarrier(): number | undefined {
    return this.requested?.barrier;
  }

  async serviceRequested(failed?: DequeuedUpdate): Promise<number> {
    const requested = this.requested;
    if (!requested) return this.queue.acceptedSequence;
    this.requested = undefined;
    try {
      const through = await this.run(failed, requested.coverage);
      requested.resolve();
      return through;
    } catch (error) {
      requested.reject(error);
      throw error;
    }
  }

  run(failed?: DequeuedUpdate, requiredCoverage?: ChainCursor): Promise<number> {
    this.indexer.markNotReady();
    if (this.current) return this.current;
    const anchors: ChainCursor[] = [];
    if (requiredCoverage) anchors.push({ ...requiredCoverage });
    const currentCoverage = this.indexer.health().cursor;
    if (currentCoverage) anchors.push({ ...currentCoverage });
    let coverage = highestCoverage(requiredCoverage, currentCoverage);
    coverage = highestCoverage(coverage, failed && updateCursor(failed.update));
    this.queue.beginRecovery(failed);
    const operation = this.recoverUntilReady(coverage, this.indexer.finalizedCheckpoint(), anchors);
    const tracked = operation.finally(() => {
      if (this.current === tracked) this.current = undefined;
    });
    this.current = tracked;
    return tracked;
  }

  rejectPending(error: unknown): void {
    const requested = this.requested;
    this.requested = undefined;
    requested?.reject(error);
    if (this.signal.aborted || this.queue.closed) this.queue.abortRecovery();
  }

  private async recoverUntilReady(
    initialCoverage: ChainCursor | undefined,
    finalizedCheckpoint: Checkpoint | undefined,
    anchors: readonly ChainCursor[],
  ): Promise<number> {
    let requiredCoverage = initialCoverage && { ...initialCoverage };
    let cursorlessCoverage: ChainCursor | undefined;
    let cursorlessThrough = 0;
    const finalizedProof = finalizedCheckpoint;
    let finalizedValidated = finalizedProof === undefined;
    while (!this.signal.aborted) {
      try {
        const bindings = new RecoveryHashBindings(recoveryIdentityCapacity(this.config.queueBound));
        for (const cursor of anchors) bindings.observe(cursor);
        bindings.observe(this.indexer.health().cursor);
        bindings.observe(requiredCoverage);
        bindings.observe(finalizedProof?.cursor);
        bindings.observe(cursorlessCoverage);
        const activityLease = await waitForActivityLease(
          this.activity,
          this.config.sourceOperationTimeoutMilliseconds,
          this.signal,
        );
        if (activityLease === undefined) throw new IndexerError("SOURCE", "recovery source became inactive");
        if (!finalizedValidated && finalizedProof) {
          const valid = await boundedSourceOperation(
            "recovery finalized checkpoint validation",
            this.config.sourceOperationTimeoutMilliseconds,
            this.signal,
            () => this.source.validateCheckpoint(finalizedProof),
          );
          if (!valid) throw new IndexerError("INVALID_REQUEST", "recovery finalized checkpoint is not canonical");
          finalizedValidated = true;
        }
        this.queue.stageQueuedForRecovery();
        let staged = this.queue.recoveryEntries();
        validateRecoveryEntries(staged, this.config, bindings);
        if (
          staged.some(
            (entry) => entry.sequence > cursorlessThrough && entry.update.kind === "Gap" && !entry.update.cursor,
          )
        ) {
          const coverageThrough = this.queue.acceptedSequence;
          const head = await boundedSourceOperation(
            "recovery canonical coverage",
            this.config.sourceOperationTimeoutMilliseconds,
            this.signal,
            () => this.source.canonicalHead(),
          );
          if (head.chainId !== this.config.deployment.chainId)
            throw new IndexerError("SOURCE", "recovery canonical coverage chain id mismatch");
          bindings.observe(head);
          cursorlessCoverage = { ...head };
          cursorlessThrough = coverageThrough;
          requiredCoverage = highestCoverage(requiredCoverage, head);
        }
        requiredCoverage = highestCoverage(requiredCoverage, barrierCoverage(staged));
        const snapshot = await boundedSourceOperation(
          "recovery snapshot",
          this.config.sourceOperationTimeoutMilliseconds,
          this.signal,
          () => this.source.snapshot(this.config.deployment),
        );
        bindings.observe(snapshot.cursor);
        if (requiredCoverage && !snapshotCoversCoverage(snapshot.cursor, requiredCoverage)) {
          this.indexer.markNotReady();
          await delay(this.config.reconnectDelayMilliseconds, this.signal);
          continue;
        }
        this.queue.stageQueuedForRecovery();
        staged = this.queue.recoveryEntries();
        validateRecoveryEntries(staged, this.config, bindings);
        const buffered = staged
          .filter((entry) => !snapshotCoversUpdate(snapshot.cursor, entry, cursorlessCoverage, cursorlessThrough))
          .map(({ update }) => update);
        try {
          if (!this.activity.isCurrent(activityLease))
            throw new IndexerError("SOURCE", "recovery source activity changed during snapshot");
          this.indexer.installSnapshot(snapshot, buffered);
        } catch (error) {
          requiredCoverage = highestCoverage(requiredCoverage, entriesCoverage(staged));
          throw error;
        }
        return this.queue.completeRecovery();
      } catch (error) {
        this.indexer.markNotReady();
        if (this.signal.aborted)
          throw new IndexerError("SOURCE", "recovery aborted before a state candidate was installed");
        if (isPermanentBootstrapError(error)) {
          this.queue.abortRecovery();
          throw error;
        }
        await delay(this.config.reconnectDelayMilliseconds, this.signal);
      }
    }
    this.queue.abortRecovery();
    throw new IndexerError("SOURCE", "recovery ended before a state candidate was installed");
  }
}

interface ManualRecoveryRequest {
  readonly barrier: number;
  readonly coverage?: ChainCursor;
  readonly promise: Promise<void>;
  readonly resolve: () => void;
  readonly reject: (error: unknown) => void;
}

class RecoveryHashBindings {
  private readonly values = new Map<string, { readonly blockNumber: bigint; readonly executionBlockNumber: bigint }>();

  constructor(private readonly capacity: number) {}

  observe(cursor: ChainCursor | undefined): void {
    if (!cursor || !isNonzeroHash(cursor.blockHash)) return;
    const hash = cursor.blockHash.toLowerCase();
    const existing = this.values.get(hash);
    if (existing) {
      if (existing.blockNumber !== cursor.blockNumber || existing.executionBlockNumber !== cursor.executionBlockNumber)
        throw new IndexerError("INVALID_REQUEST", "recovery reuses block hash with conflicting identity");
      return;
    }
    if (this.values.size >= this.capacity)
      throw new IndexerError("INVALID_REQUEST", "recovery block identity budget exceeded");
    this.values.set(hash, {
      blockNumber: cursor.blockNumber,
      executionBlockNumber: cursor.executionBlockNumber,
    });
  }
}

function recoveryIdentityCapacity(queueBound: number): number {
  const queueLimit = Math.floor((Number.MAX_SAFE_INTEGER - RECOVERY_IDENTITY_FIXED_SLOTS) / 2);
  return queueBound > queueLimit ? Number.MAX_SAFE_INTEGER : queueBound * 2 + RECOVERY_IDENTITY_FIXED_SLOTS;
}

function snapshotCoversUpdate(
  snapshot: ChainCursor,
  entry: DequeuedUpdate,
  cursorlessCoverage: ChainCursor | undefined,
  cursorlessThrough: number,
): boolean {
  if (entry.update.kind !== "Gap" && entry.update.kind !== "Reorg") return false;
  const cursor = updateCursor(entry.update);
  if (cursor) return snapshotCoversCoverage(snapshot, cursor);
  return (
    entry.update.kind === "Gap" &&
    entry.sequence <= cursorlessThrough &&
    cursorlessCoverage !== undefined &&
    snapshotCoversCoverage(snapshot, cursorlessCoverage)
  );
}

function snapshotCoversCoverage(snapshot: ChainCursor, required: ChainCursor): boolean {
  if (snapshot.chainId !== required.chainId)
    throw new IndexerError("INVALID_REQUEST", "recovery coverage chain id mismatch");
  assertImmutableHashBinding(snapshot, required);
  if (snapshot.blockNumber !== required.blockNumber) return snapshot.blockNumber > required.blockNumber;
  const snapshotRank = commitmentRank(snapshot.commitment);
  const requiredRank = commitmentRank(required.commitment);
  if (sameBlockIdentity(snapshot, required)) return snapshotRank >= requiredRank;
  if (required.commitment === Commitment.Finalized)
    throw new IndexerError("INVALID_REQUEST", "recovery snapshot conflicts with finalized block identity");
  return (
    snapshot.commitment !== Commitment.Realtime && snapshotRank >= requiredRank && isNonzeroHash(snapshot.blockHash)
  );
}

function barrierCoverage(entries: readonly DequeuedUpdate[]): ChainCursor | undefined {
  let highest: ChainCursor | undefined;
  for (const { update } of entries)
    if (update.kind === "Gap" || update.kind === "Reorg") highest = highestCoverage(highest, updateCursor(update));
  return highest;
}

function entriesCoverage(entries: readonly DequeuedUpdate[]): ChainCursor | undefined {
  let highest: ChainCursor | undefined;
  for (const { update } of entries) highest = highestCoverage(highest, updateCursor(update));
  return highest;
}

function highestCoverage(left: ChainCursor | undefined, right: ChainCursor | undefined): ChainCursor | undefined {
  if (!left) return right && { ...right };
  if (!right) return { ...left };
  if (left.chainId !== right.chainId) throw new IndexerError("INVALID_REQUEST", "recovery coverage chain id mismatch");
  assertImmutableHashBinding(left, right);
  if (left.blockNumber === right.blockNumber) {
    const sameIdentity = sameBlockIdentity(left, right);
    if (!sameIdentity && (left.commitment === Commitment.Finalized || right.commitment === Commitment.Finalized))
      throw new IndexerError("INVALID_REQUEST", "recovery coverage conflicts with finalized block identity");
    if (sameIdentity && commitmentRank(left.commitment) !== commitmentRank(right.commitment))
      return { ...(commitmentRank(left.commitment) > commitmentRank(right.commitment) ? left : right) };
  }
  return { ...(compareCursor(left, right) >= 0 ? left : right) };
}

function sameBlockIdentity(left: ChainCursor, right: ChainCursor): boolean {
  return (
    left.executionBlockNumber === right.executionBlockNumber &&
    isNonzeroHash(left.blockHash) &&
    isNonzeroHash(right.blockHash) &&
    left.blockHash.toLowerCase() === right.blockHash.toLowerCase()
  );
}

function assertImmutableHashBinding(left: ChainCursor, right: ChainCursor): void {
  if (
    sameHash(left, right) &&
    (left.blockNumber !== right.blockNumber || left.executionBlockNumber !== right.executionBlockNumber)
  )
    throw new IndexerError("INVALID_REQUEST", "recovery reuses block hash with conflicting identity");
}

function sameHash(left: ChainCursor, right: ChainCursor): boolean {
  return (
    isNonzeroHash(left.blockHash) &&
    isNonzeroHash(right.blockHash) &&
    left.blockHash.toLowerCase() === right.blockHash.toLowerCase()
  );
}

function isNonzeroHash(hash: string | undefined): hash is string {
  return hash !== undefined && /^0x[0-9a-fA-F]{64}$/.test(hash) && !/^0x0{64}$/i.test(hash);
}

function waitForActivityLease(
  activity: SourceActivity,
  timeoutMilliseconds: number,
  signal: AbortSignal,
): Promise<number | undefined> {
  const wait = new AbortController();
  return withDeadline(
    "recovery source activity",
    timeoutMilliseconds,
    signal,
    () => activity.waitForLease(wait.signal),
    () => wait.abort(),
  );
}

async function boundedSourceOperation<T>(
  name: string,
  timeoutMilliseconds: number,
  signal: AbortSignal,
  start: () => Promise<T>,
): Promise<T> {
  const operation = Promise.resolve().then(start);
  try {
    return await withDeadline(name, timeoutMilliseconds, signal, () => operation);
  } catch (error) {
    if (!isDeadlineError(error)) throw error;
    return waitForUnderlying(operation, signal, name);
  }
}

function waitForUnderlying<T>(operation: Promise<T>, signal: AbortSignal, name: string): Promise<T> {
  if (signal.aborted) return Promise.reject(new IndexerError("SOURCE", `${name} cancelled`));
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", onAbort);
      callback();
    };
    const onAbort = () => finish(() => reject(new IndexerError("SOURCE", `${name} cancelled`)));
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      (value) => finish(() => resolve(value)),
      (error: unknown) => finish(() => reject(error)),
    );
    if (signal.aborted) onAbort();
  });
}

function isDeadlineError(error: unknown): boolean {
  return (
    error instanceof Error && (error as Error & { code?: string }).code === "SOURCE" && /deadline/i.test(error.message)
  );
}

function isPermanentBootstrapError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const code = (error as Error & { code?: string }).code;
  return code === "INVALID_REQUEST" || code === "CODE_HASH_MISMATCH";
}

function validateRecoveryEntries(
  entries: readonly DequeuedUpdate[],
  config: ClientConnectConfig,
  bindings?: RecoveryHashBindings,
): void {
  try {
    for (const { update } of entries) {
      if (update.kind === "Log") {
        validateCoreLogIdentity(update.log, config.deployment.core, config.deployment.chainId);
      } else if (update.kind === "Correction") {
        validateCorrectionEnvelope(update.correction, config.deployment.core, config.deployment.chainId);
      }
      bindings?.observe(updateCursor(update));
    }
  } catch (error) {
    throw new IndexerError(
      "INVALID_REQUEST",
      error instanceof Error ? error.message : "recovery handoff contains an invalid source update",
    );
  }
}
