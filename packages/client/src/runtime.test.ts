import { strict as assert } from "node:assert";
import { test } from "node:test";
import {
  WAD,
  createLaneState,
  decodeLaneSlot0,
  encodeLaneSlot0,
  type Address,
  type QuoteRequest,
  type QuoteState,
} from "@lunarbase-lab/pmm-v2-math";
import * as HexValue from "ox/Hex";
import type { Hex } from "ox/Hex";
import {
  Commitment,
  ConnectedQuoteClient,
  CORE_EVENT_TOPICS,
  MATH_COMPATIBILITY_VERSION,
  Network,
  QuoteIndexer,
  QuoteReducer,
  SCHEMA_VERSION,
  checkpointMatchesDeployment,
  validateDeploymentConfig,
  type BackfillRequest,
  type BootstrapSnapshot,
  type ChainCursor,
  type ChainDataSource,
  type ChainUpdate,
  type Checkpoint,
  type ClientConnectConfig,
  type ContractFilter,
  type ContractLog,
  type DeploymentConfig,
} from "./index.js";
import { CursorReorderBuffer } from "./state/ordering.js";

const CASH = "0x0000000000000000000000000000000000000001" as Address;
const ASSET = "0x0000000000000000000000000000000000000002" as Address;
const ROUTER = "0x0000000000000000000000000000000000000003" as Address;
const CORE = "0x0000000000000000000000000000000000000004" as Address;
const HASH = `0x${"11".repeat(32)}` as Hex;

class MockSource implements ChainDataSource {
  readonly network = Network.Base;
  snapshotCalls = 0;
  backfillCalls = 0;
  subscribeCalls = 0;
  canonicalCalls = 0;
  validateCalls = 0;
  checkpointValid = true;
  snapshotFailures = 0;
  private readonly updates: ChainUpdate[] = [];
  private wake?: () => void;

  async snapshot(): Promise<BootstrapSnapshot> {
    this.snapshotCalls += 1;
    if (this.snapshotFailures > 0) {
      this.snapshotFailures -= 1;
      throw new Error("intentional snapshot failure");
    }
    return snapshot();
  }

  async backfill(_request: BackfillRequest): Promise<readonly ContractLog[]> {
    this.backfillCalls += 1;
    return [];
  }

  async subscribe(_filter: ContractFilter, signal?: AbortSignal): Promise<AsyncIterable<ChainUpdate>> {
    this.subscribeCalls += 1;
    return this.stream(signal);
  }

  private async *stream(signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    while (!signal?.aborted) {
      if (this.updates.length === 0)
        await new Promise<void>((resolve) => {
          const done = () => {
            signal?.removeEventListener("abort", done);
            this.wake = undefined;
            resolve();
          };
          this.wake = done;
          signal?.addEventListener("abort", done, { once: true });
        });
      const update = this.updates.shift();
      if (update) yield update;
    }
  }

  async canonicalHead(): Promise<ChainCursor> {
    this.canonicalCalls += 1;
    return cursor();
  }

  async validateCheckpoint(_checkpoint: Checkpoint): Promise<boolean> {
    this.validateCalls += 1;
    return this.checkpointValid;
  }

  publish(update: ChainUpdate): void {
    this.updates.push(update);
    this.wake?.();
  }
}

test("quote and quoteMany use one in-memory snapshot without source I/O", async () => {
  const source = new MockSource();
  const client = await ConnectedQuoteClient.connect(connectConfig(), source);
  const calls = sourceCalls(source);
  const single = client.quote(request());
  const batch = client.quoteMany([request(), request(), request()]);
  assert.deepEqual(batch.cursor, single.cursor);
  assert.deepEqual(batch.results, [single.outcome, single.outcome, single.outcome]);
  assert.deepEqual(sourceCalls(source), calls);
  assert.throws(() => client.quoteMany(Array.from({ length: 257 }, request)), {
    code: "INVALID_REQUEST",
  });
  await client.shutdown();
});

test("checkpoint is deployment-bound and reusable", async () => {
  const source = new MockSource();
  const first = await ConnectedQuoteClient.connect(connectConfig(), source);
  const checkpoint = first.checkpoint();
  assert.equal(checkpoint?.schemaVersion, SCHEMA_VERSION);
  assert.equal(checkpoint?.network, Network.Base);
  assert.equal(checkpoint?.deploymentBlock, 1n);
  assert.equal(checkpoint?.expectWhitelisted, true);
  assert.deepEqual(checkpoint?.explicitLaneAssets, [ASSET]);
  assert.equal(checkpoint?.router, ROUTER);
  await first.shutdown();

  const restoredSource = new MockSource();
  const restored = await ConnectedQuoteClient.connect(connectConfig(), restoredSource, checkpoint);
  assert.equal(restoredSource.snapshotCalls, 0);
  assert.equal(restoredSource.validateCalls, 1);
  assert.equal(restoredSource.canonicalCalls, 1);
  await restored.shutdown();
});

test("forked or incompatible checkpoints fall back to a full snapshot", async () => {
  const source = new MockSource();
  const first = await ConnectedQuoteClient.connect(connectConfig(), source);
  const checkpoint = first.checkpoint();
  await first.shutdown();
  assert.ok(checkpoint);
  if (!checkpoint) throw new Error("checkpoint was not produced");

  const forkedSource = new MockSource();
  forkedSource.checkpointValid = false;
  const forked = await ConnectedQuoteClient.connect(connectConfig(), forkedSource, checkpoint);
  assert.equal(forkedSource.snapshotCalls, 1);
  await forked.shutdown();

  const incompatible = { ...checkpoint, router: ASSET };
  const incompatibleSource = new MockSource();
  const rejected = await ConnectedQuoteClient.connect(connectConfig(), incompatibleSource, incompatible);
  assert.equal(incompatibleSource.validateCalls, 0);
  assert.equal(incompatibleSource.snapshotCalls, 1);
  await rejected.shutdown();
});

test("checkpoint policy and structural state mismatches fail closed", () => {
  const checkpoint = QuoteIndexer.fromSnapshot(snapshot(), deployment()).checkpoint();
  assert.ok(checkpoint);
  if (!checkpoint) throw new Error("checkpoint was not produced");

  assert.equal(checkpointMatchesDeployment({ ...checkpoint, network: Network.Monad }, deployment()), false);
  assert.equal(
    checkpointMatchesDeployment({ ...checkpoint, deploymentBlock: checkpoint.deploymentBlock + 1n }, deployment()),
    false,
  );
  assert.equal(
    checkpointMatchesDeployment({ ...checkpoint, expectWhitelisted: !checkpoint.expectWhitelisted }, deployment()),
    false,
  );
  assert.equal(checkpointMatchesDeployment({ ...checkpoint, explicitLaneAssets: [CASH] }, deployment()), false);

  const invalidCash: Checkpoint = {
    ...checkpoint,
    state: { ...checkpoint.state, cash: "0x0000000000000000000000000000000000000000" },
  };
  assert.equal(checkpointMatchesDeployment(invalidCash, deployment()), false);

  const invalidFee: Checkpoint = {
    ...checkpoint,
    state: {
      ...checkpoint.state,
      feeProfile: {
        ...checkpoint.state.feeProfile,
        partnerFeeBps: new Map([[ASSET, 1_000_001]]),
      },
    },
  };
  assert.equal(checkpointMatchesDeployment(invalidFee, deployment()), false);

  const invalidWhitelist: Checkpoint = {
    ...checkpoint,
    state: {
      ...checkpoint.state,
      feeProfile: {
        ...checkpoint.state.feeProfile,
        whitelisted: !checkpoint.expectWhitelisted,
      },
    },
  };
  assert.equal(checkpointMatchesDeployment(invalidWhitelist, deployment()), false);
});

test("deployment and checkpoint integers match Rust widths", () => {
  const overU64 = 1n << 64n;
  const overU32 = 1n << 32n;
  const invalidDeployments: DeploymentConfig[] = [
    { ...deployment(), chainId: overU64 },
    { ...deployment(), deploymentBlock: -1n },
    { ...deployment(), deploymentBlock: overU64 },
    { ...deployment(), network: "Unsupported" as Network },
    { ...deployment(), expectWhitelisted: 1 as unknown as boolean },
    { ...deployment(), explicitLaneAssets: undefined as unknown as readonly Address[] },
  ];
  for (const invalid of invalidDeployments) assert.throws(() => validateDeploymentConfig(invalid), { code: "SOURCE" });

  const checkpoint = QuoteIndexer.fromSnapshot(snapshot(), deployment()).checkpoint();
  assert.ok(checkpoint);
  if (!checkpoint) throw new Error("checkpoint was not produced");
  const invalidCursors: ChainCursor[] = [
    { ...checkpoint.cursor, blockNumber: overU64 },
    { ...checkpoint.cursor, executionBlockNumber: overU64 },
    { ...checkpoint.cursor, sourceSequence: overU64 },
    { ...checkpoint.cursor, transactionIndex: overU32, logIndex: 0n },
    { ...checkpoint.cursor, transactionIndex: 0n, logIndex: overU32 },
    { ...checkpoint.cursor, sourceSequence: 0n, sourceSubIndex: overU32 },
  ];
  for (const invalidCursor of invalidCursors)
    assert.equal(checkpointMatchesDeployment({ ...checkpoint, cursor: invalidCursor }, deployment()), false);
});

test("gap stays fail-closed until retrying snapshot recovery succeeds", async () => {
  const source = new MockSource();
  const client = await ConnectedQuoteClient.connect(connectConfig(), source);
  await waitUntil(() => source.subscribeCalls > 0);
  source.snapshotFailures = 1;
  source.publish({ kind: "Gap", reason: "intentional gap" });
  await waitUntil(() => !client.health().ready);
  await waitUntil(() => source.snapshotCalls >= 3 && client.health().ready);
  assert.doesNotThrow(() => client.quote(request()));
  await client.shutdown();
});

test("canonical block floor ignores a late covered log", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  indexer.applyCoreUpdate({ kind: "Log", log: depositLog(eventCursor(1n), 7n) });

  assert.equal(indexer.checkpoint()?.state.lanes.get(ASSET)?.totalPrincipalAmount, 0n);
  assert.equal(indexer.health().ready, true);
});

test("installSnapshot replaces the canonical floor atomically", () => {
  const previous = snapshot();
  const indexer = QuoteIndexer.fromSnapshot(
    {
      ...previous,
      cursor: {
        ...previous.cursor,
        blockNumber: previous.cursor.blockNumber - 1n,
        executionBlockNumber: previous.cursor.executionBlockNumber - 1n,
      },
    },
    deployment(),
  );
  indexer.installSnapshot(snapshot(), []);
  indexer.applyCoreUpdate({ kind: "Log", log: depositLog(eventCursor(1n), 7n) });

  assert.equal(indexer.checkpoint()?.state.lanes.get(ASSET)?.totalPrincipalAmount, 0n);
});

test("canonical floor never hides a removed log", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const removed = { ...depositLog(eventCursor(1n), 7n), removed: true };

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Log", log: removed }), { code: "GAP" });
  assert.equal(indexer.health().ready, false);
});

test("same-block canonical floor rejects a conflicting hash", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const conflicting = eventCursor(1n);
  conflicting.blockHash = `0x${"22".repeat(32)}` as Hex;

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Log", log: depositLog(conflicting, 7n) }), {
    code: "REDUCER",
  });
  assert.equal(indexer.health().ready, false);
});

test("canonical log identity ignores transport sequence for duplicate detection", () => {
  const firstCursor = { ...eventCursor(3n), sourceSequence: 1n };
  const duplicateCursor = { ...firstCursor, sourceSequence: 2n };
  const duplicateBuffer = new CursorReorderBuffer(2);
  duplicateBuffer.push({ kind: "Log", log: depositLog(firstCursor, 7n) });
  assert.throws(() => duplicateBuffer.push({ kind: "Log", log: depositLog(duplicateCursor, 7n) }), {
    message: "multiple updates share one cursor",
  });
  assert.equal(duplicateBuffer.isPoisoned(), true);

  const conflictBuffer = new CursorReorderBuffer(2);
  conflictBuffer.push({ kind: "Log", log: depositLog(firstCursor, 7n) });
  assert.throws(() => conflictBuffer.push({ kind: "Log", log: depositLog(duplicateCursor, 8n) }), {
    message: "multiple updates share one cursor",
  });
  assert.equal(conflictBuffer.isPoisoned(), true);
});

test("event-level checkpoint floor covers only events through its cursor", () => {
  const checkpoint = QuoteIndexer.fromSnapshot(snapshot(), deployment()).checkpoint();
  assert.ok(checkpoint);
  if (!checkpoint) throw new Error("checkpoint was not produced");
  checkpoint.cursor = {
    ...checkpoint.cursor,
    transactionIndex: 2n,
    logIndex: 3n,
    commitment: Commitment.Canonical,
  };
  const indexer = QuoteIndexer.fromCheckpoint(checkpoint, deployment());

  indexer.applyCoreUpdate({ kind: "Log", log: depositLog(eventCursor(2n), 5n) });
  assert.equal(indexer.checkpoint()?.state.lanes.get(ASSET)?.totalPrincipalAmount, 0n);

  indexer.applyCoreUpdate({ kind: "Log", log: depositLog(eventCursor(4n), 5n) });
  assert.equal(indexer.checkpoint()?.state.lanes.get(ASSET)?.totalPrincipalAmount, 5n);
});

test("handoff never covers an older update from another chain", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const foreign: ChainCursor = {
    ...cursor(),
    chainId: 1n,
    blockNumber: cursor().blockNumber - 1n,
    executionBlockNumber: cursor().executionBlockNumber - 1n,
  };

  assert.throws(() => indexer.replayHandoff([{ kind: "Head", cursor: foreign }], cursor()), {
    code: "REDUCER",
  });
  assert.equal(indexer.health().ready, false);
});

test("same-height handoff with another block hash fails closed", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const conflicting: ChainCursor = {
    ...cursor(),
    blockHash: `0x${"22".repeat(32)}` as Hex,
    commitment: Commitment.Realtime,
    sourceSequence: 1n,
  };

  assert.throws(() => indexer.replayHandoff([{ kind: "Head", cursor: conflicting }], cursor()), {
    code: "REDUCER",
  });
  assert.equal(indexer.health().ready, false);
});

test("reducer applies packed controls and Sync, then rejects Upgraded", () => {
  const reducer = new QuoteReducer(snapshot().state, ROUTER);
  reducer.bootstrap(cursor());
  const eventCursor = (logIndex: bigint): ChainCursor => ({
    ...cursor(),
    transactionIndex: 0n,
    logIndex,
  });
  reducer.apply(eventCursor(0n), { kind: "LaneAdded", asset: ASSET });
  reducer.apply(eventCursor(1n), { kind: "LanePausedSet", asset: ASSET, paused: false });
  reducer.apply(eventCursor(2n), { kind: "Sync", asset: ASSET, assetReserve: 11n, cashReserve: 12n });
  reducer.apply(eventCursor(3n), { kind: "SlippageKSet", asset: ASSET, newK: 1_000 });
  reducer.apply(eventCursor(4n), { kind: "BlockDelaySet", asset: ASSET, blockDelay: 7 });
  reducer.apply(eventCursor(5n), {
    kind: "PricePushThresholdSet",
    asset: ASSET,
    pricePushThreshold: 17,
    enabled: true,
  });
  reducer.apply(eventCursor(6n), { kind: "LanePausedSet", asset: ASSET, paused: true });

  const checkpoint = reducer.checkpoint(deployment());
  assert.ok(checkpoint);
  const lane = checkpoint?.state.lanes.get(ASSET);
  assert.ok(lane);
  assert.equal(checkpoint?.state.cashReserve, 12n);
  assert.equal(lane?.assetReserve, 11n);
  assert.equal(lane?.totalPrincipalAmount, 0n);
  if (!lane) return;
  const fields = decodeLaneSlot0(lane.slot0);
  assert.equal(fields.price, WAD);
  assert.equal(fields.pricePushThreshold, 17n);
  assert.equal(fields.thresholdEnabled, true);
  assert.equal(fields.slippageKBps, 1_000);
  assert.equal(fields.blockDelay, 7);
  assert.equal(fields.paused, true);
  assert.throws(
    () =>
      reducer.apply(eventCursor(7n), {
        kind: "ImplementationUpgraded",
        implementation: "0x9999999999999999999999999999999999999999",
      }),
    { code: "IMPLEMENTATION_UPGRADED" },
  );
  assert.throws(
    () =>
      reducer.apply(eventCursor(8n), {
        kind: "LaneUpdated",
        asset: "0x9999999999999999999999999999999999999999",
        slot0: 0n,
      }),
    { code: "UNKNOWN_LANE" },
  );
});

function connectConfig(): ClientConnectConfig {
  return {
    deployment: deployment(),
    filter: { address: CORE, topics: [] },
    queueBound: 16,
    reconnectDelayMilliseconds: 10,
    sourceStallTimeoutMilliseconds: 1_000,
  };
}

function deployment(): DeploymentConfig {
  return {
    network: Network.Base,
    chainId: 8453n,
    core: CORE,
    router: ROUTER,
    expectWhitelisted: true,
    deploymentBlock: 1n,
    expectedImplementation: "0x8888888888888888888888888888888888888888",
    expectedImplementationCodeHash: HASH,
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    explicitLaneAssets: [ASSET],
  };
}

function snapshot(): BootstrapSnapshot {
  const state: QuoteState = {
    cash: CASH,
    cashReserve: 1_000_000n,
    lanes: new Map([
      [
        ASSET,
        createLaneState(
          encodeLaneSlot0({
            price: WAD,
            askFeeBps: 0n,
            bidFeeBps: 0n,
            pricePushThreshold: 0n,
            thresholdEnabled: false,
            latestUpdateBlock: 0n,
            exists: true,
            paused: false,
            blockDelay: 0,
            slippageKBps: 0,
            reservedHighBits: 0n,
          }),
          1_000_000n,
          0n,
        ),
      ],
    ]),
    feeProfile: {
      whitelisted: true,
      blacklistFeeMultiplier: 1n,
      partnerFeeBps: new Map(),
    },
  };
  return {
    state,
    cursor: cursor(),
    implementation: "0x8888888888888888888888888888888888888888",
    implementationCodeHash: HASH,
  };
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

function eventCursor(logIndex: bigint): ChainCursor {
  return {
    ...cursor(),
    transactionIndex: 2n,
    logIndex,
    commitment: Commitment.Realtime,
  };
}

function depositLog(eventCursor: ChainCursor, principal: bigint): ContractLog {
  return {
    address: CORE,
    topics: [
      CORE_EVENT_TOPICS.DepositExecuted,
      HexValue.fromNumber(1, { size: 32 }),
      HexValue.padLeft(ROUTER, 32),
      HexValue.padLeft(ASSET, 32),
    ],
    data: HexValue.fromNumber(principal, { size: 32 }),
    removed: false,
    cursor: eventCursor,
  };
}

function request(): QuoteRequest {
  return {
    assetIn: CASH,
    assetOut: ASSET,
    amount: 1_000n,
    mode: "ExactIn",
  };
}

function sourceCalls(source: MockSource): readonly number[] {
  return [
    source.snapshotCalls,
    source.backfillCalls,
    source.subscribeCalls,
    source.canonicalCalls,
    source.validateCalls,
  ];
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, "condition timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
