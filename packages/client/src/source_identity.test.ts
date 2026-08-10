import { strict as assert } from "node:assert";
import { test } from "node:test";
import { WAD, createLaneState, type Address, type QuoteState } from "@lunarbase-lab/pmm-v2-math";
import { encodeLaneSlot0 } from "@lunarbase-lab/pmm-v2-math/slot0";
import * as HexValue from "ox/Hex";
import type { Hex } from "ox/Hex";
import {
  Commitment,
  ConnectedQuoteClient,
  CORE_EVENT_TOPICS,
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
  snapshotFailures = 0;
  private readonly updates: ChainUpdate[] = [];
  backfillCalls = 0;
  backfillLogs: readonly ContractLog[] = [];
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
    return this.backfillLogs;
  }

  async subscribe(_filter: ContractFilter, signal?: AbortSignal): Promise<AsyncIterable<ChainUpdate>> {
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
    return cursor(100n, Commitment.Finalized);
  }

  async validateCheckpoint(_checkpoint: Checkpoint): Promise<boolean> {
    return true;
  }

  publish(update: ChainUpdate): void {
    this.updates.push(update);
    this.wake?.();
  }
}

test("foreign valid Core log is rejected without state or cursor mutation", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const before = indexer.checkpoint();

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Log", log: foreignLaneRemovedLog(101n, false) }), {
    code: "REDUCER",
    message: "contract log address does not match deployment Core",
  });
  const after = indexer.checkpoint();
  assert.deepEqual(after?.cursor, before?.cursor);
  assert.deepEqual(after?.state, before?.state);
  assert.equal(indexer.health().ready, false);
});

test("foreign removed and covered malformed logs are rejected by address first", () => {
  const logs = [
    foreignLaneRemovedLog(101n, true),
    { ...foreignLaneRemovedLog(100n, false), topics: [CORE_EVENT_TOPICS.LaneRemoved] },
  ];
  for (const log of logs) {
    const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
    assert.throws(() => indexer.applyCoreUpdate({ kind: "Log", log }), {
      code: "REDUCER",
      message: "contract log address does not match deployment Core",
    });
    assert.equal(indexer.health().ready, false);
  }
});

test("correct-Core foreign-chain removed log is rejected before removed handling", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const sourceLog = foreignLaneRemovedLog(101n, true);
  const log = {
    ...sourceLog,
    address: CORE,
    cursor: { ...sourceLog.cursor, chainId: 1n },
  };

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Log", log }), {
    code: "REDUCER",
    message: "contract log cursor chain id mismatch",
  });
  assert.equal(indexer.health().ready, false);
});

test("connected reducer recovers without applying a foreign Core log", async () => {
  const source = new MockSource();
  const client = await ConnectedQuoteClient.connect(connectConfig(), source);
  source.snapshotFailures = 1;

  source.publish({ kind: "Log", log: foreignLaneRemovedLog(101n, false) });
  await waitUntil(() => !client.health().ready);
  await waitUntil(() => source.snapshotCalls >= 3 && client.health().ready);

  assert.equal(client.checkpoint()?.state.lanes.has(ASSET), true);
  await client.shutdown();
});

test("checkpoint recovery validates foreign-chain backfill before cursor skip", async () => {
  const restored = QuoteIndexer.fromSnapshot(snapshot(), deployment()).checkpoint();
  assert.ok(restored);
  if (!restored) throw new Error("checkpoint was not produced");
  const checkpoint: Checkpoint = {
    ...restored,
    cursor: {
      ...restored.cursor,
      transactionIndex: 2n,
      logIndex: 3n,
      commitment: Commitment.Canonical,
    },
  };
  const source = new MockSource();
  const sourceLog = foreignLaneRemovedLog(100n, false);
  source.backfillLogs = [
    {
      ...sourceLog,
      address: CORE,
      cursor: { ...sourceLog.cursor, chainId: 1n, commitment: Commitment.Canonical },
    },
  ];

  const client = await ConnectedQuoteClient.connect(connectConfig(), source, checkpoint);

  assert.equal(source.backfillCalls, 1);
  assert.equal(source.snapshotCalls, 1);
  await client.shutdown();
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
    lanes: new Map([[ASSET, createLaneState(laneSlot0(), 1_000_000n, 0n)]]),
    feeProfile: {
      whitelisted: true,
      blacklistFeeMultiplier: 1n,
      partnerFeeBps: new Map(),
    },
  };
  return {
    state,
    cursor: cursor(100n, Commitment.Finalized),
    implementation: "0x8888888888888888888888888888888888888888",
    implementationCodeHash: HASH,
  };
}

function laneSlot0(): bigint {
  return encodeLaneSlot0({
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
  });
}

function cursor(blockNumber: bigint, commitment: Commitment): ChainCursor {
  return {
    chainId: 8453n,
    blockNumber,
    executionBlockNumber: blockNumber,
    blockHash: HASH,
    commitment,
  };
}

function foreignLaneRemovedLog(blockNumber: bigint, removed: boolean): ContractLog {
  return {
    address: ROUTER,
    topics: [CORE_EVENT_TOPICS.LaneRemoved, HexValue.padLeft(ASSET, 32)],
    data: "0x",
    removed,
    cursor: {
      ...cursor(blockNumber, Commitment.Realtime),
      transactionIndex: 0n,
      logIndex: 0n,
    },
  };
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, "condition timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
