import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { Address, QuoteState } from "@lunarbase-lab/pmm-v2-math";
import type { Hex } from "ox/Hex";
import {
  Commitment,
  ConnectedQuoteClient,
  CORE_EVENT_TOPICS,
  MATH_COMPATIBILITY_VERSION,
  Network,
  QuoteIndexer,
  type BackfillRequest,
  type BlockRef,
  type BootstrapSnapshot,
  type ChainCorrection,
  type ChainCursor,
  type ChainDataSource,
  type ChainUpdate,
  type Checkpoint,
  type ClientConnectConfig,
  type ContractFilter,
  type ContractLog,
  type DeploymentConfig,
  type IndexerLifecycleEvent,
} from "./index.js";
import { RecoveryCoordinator } from "./indexer/recovery_coordinator.js";
import { SourceActivity } from "./indexer/source_task.js";
import { BoundedUpdateQueue } from "./indexer/update_queue.js";

const CASH = "0x0000000000000000000000000000000000000001" as Address;
const CORE = "0x0000000000000000000000000000000000000002" as Address;
const IMPLEMENTATION = "0x0000000000000000000000000000000000000003" as Address;
const HASH = `0x${"11".repeat(32)}` as Hex;

test("overflow recovery replays updates received while its snapshot is pending", async () => {
  const source = new DelayedRecoverySource();
  const client = await ConnectedQuoteClient.connect({ ...connectConfig(1_000), queueByteBound: 1_024 }, source);
  source.blockNextSnapshot();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await source.publishAndWaitYielded({
    kind: "Log",
    log: {
      address: CORE,
      topics: [],
      data: `0x${"00".repeat(2_048)}` as Hex,
      removed: false,
      cursor: { ...nextCursor(), transactionIndex: 0n, logIndex: 0n },
    },
  });
  assert.equal(client.health().ready, false);
  await waitUntil(() => source.subscribeCalls >= 2);
  await waitUntil(() => source.snapshotCalls === 2 && !client.health().ready);
  source.publish({ kind: "Head", head: { cursor: nextCursor() } });
  source.advanceSnapshot(nextCursor());
  source.releaseSnapshot({
    ...snapshot(),
    cursor: { ...nextCursor(), blockHash: `0x${"44".repeat(32)}` as Hex },
  });

  await waitUntil(() => client.health().ready && client.health().cursor?.blockNumber === 101n);
  assert.ok(source.snapshotCalls >= 3);
  assert.equal(client.health().cursor?.blockHash, nextCursor().blockHash);
  await client.shutdown();
});

test("a resolved correction keeps the acknowledged source stream active", async () => {
  const source = new DelayedRecoverySource();
  const client = await ConnectedQuoteClient.connect(connectConfig(1_000), source);
  const correction = recoveryCorrection();
  await source.publishAndWait({ kind: "Head", head: correction.oldTip });
  await waitUntil(() => client.health().cursor?.blockHash === correction.oldTip.cursor.blockHash);
  await source.publishAndWait({ kind: "Correction", correction });
  await waitUntil(() => client.health().cursor?.blockHash === correction.newTip.cursor.blockHash);

  assert.equal(client.health().ready, true);
  assert.equal(source.subscribeCalls, 1);
  await client.shutdown();
});

test("source inactivity revokes readiness before canonical recovery installs", async () => {
  const source = new DelayedRecoverySource();
  const client = await ConnectedQuoteClient.connect(connectConfig(1_000), source);
  source.blockNextSnapshot();
  await source.publishAndWaitYielded({ kind: "Gap", reason: "source disconnected" });
  assert.equal(client.health().ready, false);
  source.releaseSnapshot();
  await waitUntil(() => client.health().ready);
  assert.equal(client.health().ready, true);
  await client.shutdown();
});

test("failed recovery candidate retains the bounded suffix after its gap", async () => {
  const source = new DelayedRecoverySource();
  const client = await ConnectedQuoteClient.connect(connectConfig(1_000), source);
  source.blockNextSnapshot();
  const recovery = client.recover();
  await waitUntil(() => source.snapshotCalls === 2 && !client.health().ready);

  await source.publishAndWaitYielded({ kind: "Gap", reason: "candidate barrier" });
  await source.publishAndWaitYielded({ kind: "Head", head: { cursor: nextCursor() } });
  source.advanceSnapshot(nextCursor());
  source.releaseSnapshot();
  await recovery;

  assert.ok(source.snapshotCalls >= 3);
  assert.equal(client.health().ready, true);
  assert.equal(client.health().cursor?.blockNumber, 101n);
  assert.equal(client.health().cursor?.blockHash, nextCursor().blockHash);
  await client.shutdown();
});

test("recovery publishes a buffered correction only after its candidate swap", async () => {
  const source = new DelayedRecoverySource();
  const client = await ConnectedQuoteClient.connect(connectConfig(1_000), source);
  const notices: IndexerLifecycleEvent[] = [];
  let observedHash: Hex | undefined;
  let reentrantRecovery: Promise<void> | undefined;
  client.onLifecycle((event) => {
    notices.push(event);
    if (event.kind === "CorrectionApplied") {
      observedHash = client.health().cursor?.blockHash;
      reentrantRecovery ??= client.recover();
    }
  });
  source.blockNextSnapshot();
  const recovery = client.recover();
  await waitUntil(() => source.snapshotCalls === 2 && !client.health().ready);
  const correction = recoveryCorrection();
  const expectedHash = correction.newTip.cursor.blockHash;
  await source.publishAndWait({ kind: "Head", head: correction.oldTip });
  await source.publishAndWait({ kind: "Log", log: blacklistMultiplierLog(correction.oldTip.cursor, 2n) });
  const aliased: ChainUpdate = { kind: "Correction", correction };
  await source.publishAndWait(aliased);
  (correction.newTip.cursor as { blockHash?: Hex }).blockHash = `0x${"44".repeat(32)}` as Hex;
  assert.equal(notices.filter((event) => event.kind === "CorrectionApplied").length, 0);

  source.advanceSnapshot(correction.newTip.cursor);
  source.releaseSnapshot();
  await recovery;
  await new Promise((resolve) => setTimeout(resolve, 0));
  await reentrantRecovery;
  assert.equal(client.health().ready, true);
  assert.equal(client.health().cursor?.blockHash, expectedHash);
  assert.equal(client.correctionMetrics().appliedCorrections, 1);
  assert.equal(notices.filter((event) => event.kind === "CorrectionApplied").length, 1);
  assert.equal(observedHash, expectedHash);
  await client.shutdown();
});

test("failed recovery candidate emits none of its staged correction notices", async () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const notices: IndexerLifecycleEvent[] = [];
  indexer.onLifecycle((event) => notices.push(event));
  const correction = recoveryCorrection();

  assert.throws(
    () =>
      indexer.installSnapshot(snapshot(), [
        { kind: "Head", head: correction.oldTip },
        { kind: "Correction", correction },
        { kind: "Gap", reason: "candidate handoff failed" },
      ]),
    { code: "GAP" },
  );
  await Promise.resolve();
  assert.equal(indexer.health().ready, true);
  assert.equal(indexer.health().cursor?.blockHash, HASH);
  assert.equal(indexer.correctionMetrics().appliedCorrections, 0);
  assert.deepEqual(notices, []);
});

test("snapshot-covered corrections publish once and retain exact retry identity", async () => {
  const correction = recoveryCorrection();
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const notices: IndexerLifecycleEvent[] = [];
  indexer.onLifecycle((event) => notices.push(event));
  indexer.installSnapshot({ ...snapshot(), cursor: correction.newTip.cursor }, [{ kind: "Correction", correction }]);

  await Promise.resolve();
  assert.equal(notices.filter((event) => event.kind === "CorrectionApplied").length, 1);
  assert.equal(indexer.correctionMetrics().appliedCorrections, 1);
  indexer.applyCoreUpdate({ kind: "Correction", correction });
  await Promise.resolve();
  assert.equal(indexer.health().ready, true);
  assert.equal(notices.filter((event) => event.kind === "CorrectionApplied").length, 1);
  assert.equal(indexer.correctionMetrics().appliedCorrections, 1);
});

test("manual recovery covers reducer progress accepted before its barrier", async () => {
  const source = new DelayedRecoverySource();
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const { recovery } = recoveryHarness(indexer, source, 1_000);
  source.blockNextSnapshot();
  const requested = recovery.request();
  indexer.applyCoreUpdate({ kind: "Log", log: blacklistMultiplierLog(nextCursor(), 2n) });
  const servicing = recovery.serviceRequested();
  await waitUntil(() => source.snapshotCalls === 1);

  source.advanceSnapshot(nextCursor(), 2n);
  source.releaseSnapshot();
  await Promise.all([requested, servicing]);
  assert.ok(source.snapshotCalls >= 2);
  assert.equal(indexer.health().cursor?.blockNumber, 101n);
  assert.equal(indexer.checkpoint()?.state.blacklistFeeMultiplier, 2n);
});

test("manual recovery rejects a conflicting finalized snapshot identity", async () => {
  const source = new DelayedRecoverySource();
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const { recovery } = recoveryHarness(indexer, source, 1_000);
  source.blockNextSnapshot();
  const requestedFailure = assert.rejects(recovery.request(), /conflicts with finalized block identity/);
  const servicingFailure = assert.rejects(recovery.serviceRequested(), /conflicts with finalized block identity/);
  await waitUntil(() => source.snapshotCalls === 1);
  source.releaseSnapshot({
    ...snapshot(),
    cursor: { ...cursor(), blockHash: `0x${"44".repeat(32)}` as Hex },
  });

  await Promise.all([requestedFailure, servicingFailure]);
  assert.equal(indexer.health().ready, false);
});

test("manual recovery rejects a provisional fork that conflicts with finalized coverage", async () => {
  const source = new DelayedRecoverySource();
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const { recovery } = recoveryHarness(indexer, source, 1_000);
  const requestedFailure = assert.rejects(recovery.request(), /conflicts with finalized block identity/);
  const servicingFailure = assert.rejects(
    recovery.serviceRequested({
      update: {
        kind: "Head",
        head: {
          cursor: { ...cursor(), blockHash: `0x${"44".repeat(32)}` as Hex, commitment: Commitment.Realtime },
        },
      },
      sequence: 1,
      chargedBytes: 384,
    }),
    /conflicts with finalized block identity/,
  );

  await Promise.all([requestedFailure, servicingFailure]);
  assert.equal(source.snapshotCalls, 0);
  assert.equal(indexer.health().ready, false);
});

test("recovery coverage rejects a reused hash with another execution context", async () => {
  const source = new DelayedRecoverySource();
  const initial = { ...snapshot(), cursor: { ...cursor(), commitment: Commitment.Canonical } };
  const indexer = QuoteIndexer.fromSnapshot(initial, deployment());
  const { recovery } = recoveryHarness(indexer, source, 1_000);
  const requestedFailure = assert.rejects(recovery.request(), /reuses block hash with conflicting identity/);
  const conflictingCursor = {
    ...initial.cursor,
    executionBlockNumber: initial.cursor.executionBlockNumber + 1n,
  };
  const servicingFailure = assert.rejects(
    recovery.serviceRequested({
      update: { kind: "Log", log: blacklistMultiplierLog(conflictingCursor, 2n) },
      sequence: 1,
      chargedBytes: 384,
    }),
    /reuses block hash with conflicting identity/,
  );

  await Promise.all([requestedFailure, servicingFailure]);
  assert.equal(source.snapshotCalls, 0);
  assert.equal(indexer.health().ready, false);
});

test("recovery snapshot cannot reuse a published hash at another block height", async () => {
  const source = new DelayedRecoverySource();
  const initial = { ...snapshot(), cursor: { ...cursor(), commitment: Commitment.Canonical } };
  const indexer = QuoteIndexer.fromSnapshot(initial, deployment());
  const { recovery } = recoveryHarness(indexer, source, 1_000);
  source.advanceSnapshot({
    ...initial.cursor,
    blockNumber: initial.cursor.blockNumber + 1n,
    executionBlockNumber: initial.cursor.executionBlockNumber + 1n,
  });
  const requestedFailure = assert.rejects(recovery.request(), /reuses block hash with conflicting identity/);
  const servicingFailure = assert.rejects(recovery.serviceRequested(), /reuses block hash with conflicting identity/);

  await Promise.all([requestedFailure, servicingFailure]);
  assert.equal(source.snapshotCalls, 1);
  assert.equal(indexer.health().ready, false);
});

test("recovery never publishes above an invalid retained finalized checkpoint", async () => {
  const source = new DelayedRecoverySource();
  source.checkpointValid = false;
  source.advanceSnapshot(nextCursor());
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const { recovery } = recoveryHarness(indexer, source, 1_000);
  const requestedFailure = assert.rejects(recovery.request(), /finalized checkpoint is not canonical/);
  const servicingFailure = assert.rejects(recovery.serviceRequested(), /finalized checkpoint is not canonical/);

  await Promise.all([requestedFailure, servicingFailure]);
  assert.equal(indexer.health().ready, false);
  assert.equal(indexer.health().cursor?.blockNumber, 100n);
  assert.equal(source.snapshotCalls, 0);
});

test("timed-out recovery retains only one underlying snapshot operation", async () => {
  const source = new DelayedRecoverySource();
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const { recovery, controller } = recoveryHarness(indexer, source, 20);
  source.blockNextSnapshot();
  const requestedFailure = assert.rejects(recovery.request(), /recovery aborted/);
  const servicingFailure = assert.rejects(recovery.serviceRequested(), /recovery aborted/);
  await waitUntil(() => source.snapshotCalls === 1);
  await new Promise((resolve) => setTimeout(resolve, 60));
  assert.equal(source.snapshotCalls, 1);

  controller.abort();
  await Promise.all([requestedFailure, servicingFailure]);
  assert.equal(indexer.health().ready, false);
});

test("recovery discards a snapshot when source activity changes generation", async () => {
  const source = new DelayedRecoverySource();
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const { recovery, activity } = recoveryHarness(indexer, source, 1_000);
  source.blockNextSnapshot();
  const requested = recovery.request();
  const servicing = recovery.serviceRequested();
  await waitUntil(() => source.snapshotCalls === 1);
  activity.setActive(false);
  activity.setActive(true);
  source.releaseSnapshot();
  await Promise.all([requested, servicing]);
  assert.ok(source.snapshotCalls >= 2);
  assert.equal(indexer.health().ready, true);
});

class DelayedRecoverySource implements ChainDataSource {
  readonly network = Network.Base;
  snapshotCalls = 0;
  subscribeCalls = 0;
  checkpointValid = true;
  private readonly updates: ChainUpdate[] = [];
  private enqueuedUpdates = 0;
  private yieldedUpdates = 0;
  private deliveredUpdates = 0;
  private wake?: () => void;
  private blockedSnapshot?: Promise<BootstrapSnapshot>;
  private resolveBlockedSnapshot?: (value: BootstrapSnapshot) => void;
  private steadySnapshot = snapshot();

  snapshot(): Promise<BootstrapSnapshot> {
    this.snapshotCalls += 1;
    return this.blockedSnapshot ?? Promise.resolve(this.steadySnapshot);
  }

  blockNextSnapshot(): void {
    this.blockedSnapshot = new Promise((resolve) => {
      this.resolveBlockedSnapshot = resolve;
    });
  }

  releaseSnapshot(replacement = snapshot()): void {
    this.resolveBlockedSnapshot?.(replacement);
    this.blockedSnapshot = undefined;
    this.resolveBlockedSnapshot = undefined;
  }

  advanceSnapshot(next: ChainCursor, blacklistFeeMultiplier = 1n): void {
    const value = snapshot();
    this.steadySnapshot = {
      ...value,
      state: { ...value.state, blacklistFeeMultiplier },
      cursor: { ...next },
    };
  }

  backfill(_request: BackfillRequest): Promise<readonly ContractLog[]> {
    return Promise.resolve([]);
  }

  canonicalHead(): Promise<ChainCursor> {
    return Promise.resolve(cursor());
  }

  validateCheckpoint(_checkpoint: Checkpoint): Promise<boolean> {
    return Promise.resolve(this.checkpointValid);
  }

  async subscribe(_filter: ContractFilter, signal?: AbortSignal): Promise<AsyncIterable<ChainUpdate>> {
    this.subscribeCalls += 1;
    const empty = () => this.updates.length === 0;
    const take = () => this.updates.shift();
    const delivered = () => (this.deliveredUpdates += 1);
    const yielded = () => (this.yieldedUpdates += 1);
    const wait = () =>
      new Promise<void>((resolve) => {
        const done = () => {
          signal?.removeEventListener("abort", done);
          this.wake = undefined;
          resolve();
        };
        this.wake = done;
        signal?.addEventListener("abort", done, { once: true });
      });
    return {
      async *[Symbol.asyncIterator]() {
        while (!signal?.aborted) {
          if (empty()) await wait();
          const update = take();
          if (update) {
            yielded();
            yield update;
            delivered();
          }
        }
      },
    };
  }

  publish(update: ChainUpdate): void {
    this.enqueuedUpdates += 1;
    this.updates.push(update);
    this.wake?.();
  }

  async publishAndWait(update: ChainUpdate): Promise<void> {
    this.publish(update);
    const target = this.enqueuedUpdates;
    await waitUntil(() => this.deliveredUpdates >= target);
  }

  async publishAndWaitYielded(update: ChainUpdate): Promise<void> {
    this.publish(update);
    const target = this.enqueuedUpdates;
    await waitUntil(() => this.yieldedUpdates >= target);
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

function recoveryHarness(indexer: QuoteIndexer, source: ChainDataSource, timeout: number) {
  const activity = new SourceActivity();
  const controller = new AbortController();
  activity.setActive(true);
  return {
    activity,
    controller,
    recovery: new RecoveryCoordinator(
      indexer,
      source,
      connectConfig(timeout),
      new BoundedUpdateQueue(16, 1024 * 1024),
      activity,
      controller.signal,
    ),
  };
}

function connectConfig(timeout: number): ClientConnectConfig {
  return {
    deployment: deployment(),
    filter: { address: CORE, topics: [] },
    queueBound: 16,
    queueByteBound: 1024 * 1024,
    reconnectDelayMilliseconds: 5,
    sourceStallTimeoutMilliseconds: 1_000,
    sourceOperationTimeoutMilliseconds: timeout,
  };
}

function deployment(): DeploymentConfig {
  return {
    network: Network.Base,
    chainId: 8453n,
    core: CORE,
    feeClass: "Whitelisted",
    verifiedRouter: undefined,
    deploymentBlock: 1n,
    expectedImplementation: IMPLEMENTATION,
    expectedImplementationCodeHash: HASH,
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    explicitLaneAssets: [],
  };
}

function snapshot(): BootstrapSnapshot {
  const state: QuoteState = { cash: CASH, cashReserve: 1_000_000n, lanes: new Map(), blacklistFeeMultiplier: 1n };
  return { state, cursor: cursor(), implementation: IMPLEMENTATION, implementationCodeHash: HASH };
}

function cursor(): ChainCursor {
  return {
    chainId: 8453n,
    blockNumber: 100n,
    executionBlockNumber: 100n,
    blockHash: HASH,
    commitment: Commitment.Finalized,
  };
}

function nextCursor(): ChainCursor {
  return {
    ...cursor(),
    blockNumber: 101n,
    executionBlockNumber: 101n,
    blockHash: `0x${"22".repeat(32)}` as Hex,
    commitment: Commitment.Realtime,
  };
}

function recoveryCorrection(): ChainCorrection {
  const ancestor: BlockRef = { cursor: cursor(), parentHash: `0x${"99".repeat(32)}` as Hex };
  const oldTip: BlockRef = { cursor: { ...nextCursor(), sourceSequence: 1n }, parentHash: HASH };
  const newTip: BlockRef = {
    cursor: { ...nextCursor(), blockHash: `0x${"33".repeat(32)}` as Hex, sourceSequence: 2n },
    parentHash: HASH,
  };
  return {
    commonAncestor: ancestor,
    oldTip,
    newTip,
    oldBranch: [oldTip],
    newBranch: [newTip],
    replacementLogs: [],
  };
}

function blacklistMultiplierLog(block: ChainCursor, value: bigint): ContractLog {
  return {
    address: CORE,
    topics: [CORE_EVENT_TOPICS.BlacklistFeeMultiplierSet],
    data: `0x${value.toString(16).padStart(64, "0")}` as Hex,
    removed: false,
    cursor: { ...block, transactionIndex: 0n, logIndex: 0n },
  };
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, "condition timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
