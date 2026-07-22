import { strict as assert } from "node:assert";
import { test } from "node:test";
import {
  WAD,
  createLaneState,
  encodeLaneSlot0,
  type Address,
  type QuoteRequest,
  type QuoteState,
} from "@lunarbase/math";
import type { Hex } from "ox/Hex";
import {
  Commitment,
  ConnectedQuoteClient,
  MATH_COMPATIBILITY_VERSION,
  Network,
  QuoteIndexer,
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

test("v3 checkpoint is deployment-bound and reusable", async () => {
  const source = new MockSource();
  const first = await ConnectedQuoteClient.connect(connectConfig(), source);
  const checkpoint = first.checkpoint();
  assert.equal(checkpoint?.schemaVersion, 3);
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
    expectedRuntimeCodeHash: HASH,
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    httpRpcUrl: "http://unused",
    realtimeSource: "ws://unused",
    explicitLaneAssets: [ASSET],
  };
}

function snapshot(): BootstrapSnapshot {
  const state: QuoteState = {
    cash: CASH,
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
            reservedHighBits: 0n,
          }),
          1_000_000n,
          0,
          0,
          true,
          false,
        ),
      ],
    ]),
    feeProfile: {
      whitelisted: true,
      blacklistFeeMultiplier: 1n,
      partnerFeeBps: new Map(),
    },
  };
  return { state, cursor: cursor(), runtimeCodeHash: HASH };
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
