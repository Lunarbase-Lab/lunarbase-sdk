import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { Address, QuoteState } from "@lunarbase-lab/pmm-v2-math";
import type { Hex } from "ox/Hex";
import {
  Commitment,
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
import { RecoveryCoordinator } from "./indexer/recovery_coordinator.js";
import { SourceActivity } from "./indexer/source_task.js";
import { BoundedUpdateQueue } from "./indexer/update_queue.js";

const CASH = "0x0000000000000000000000000000000000000001" as Address;
const CORE = "0x0000000000000000000000000000000000000002" as Address;
const IMPLEMENTATION = "0x0000000000000000000000000000000000000003" as Address;
const C = hash("11");
const A = hash("22");
const B = hash("33");

test("recovery binds every staged hash identity before choosing a coverage winner", async () => {
  const source = new IdentitySource([snapshot(cursor(103n, A))]);
  const indexer = QuoteIndexer.fromSnapshot(snapshot(cursor(100n, C)), deployment());
  const { queue, recovery } = harness(indexer, source);
  queue.push(head(101n, A));
  queue.push(head(102n, B));
  queue.push(head(103n, A));
  const requestedFailure = assert.rejects(recovery.request(), /reuses block hash with conflicting identity/);
  const servicingFailure = assert.rejects(recovery.serviceRequested(), /reuses block hash with conflicting identity/);

  await Promise.all([requestedFailure, servicingFailure]);
  assert.equal(source.snapshotCalls, 0);
  assert.equal(indexer.health().ready, false);
});

test("recovery rebuild retains staged hash bindings across retry attempts", async () => {
  const source = new IdentitySource([snapshot(cursor(100n, C)), snapshot(cursor(103n, A))]);
  const indexer = QuoteIndexer.fromSnapshot(snapshot(cursor(100n, C)), deployment());
  const { queue, recovery } = harness(indexer, source);
  queue.push(head(101n, A));
  queue.push({ kind: "Gap", cursor: cursor(102n, B), reason: "force a newer coverage watermark" });
  const requestedFailure = assert.rejects(recovery.request(), /reuses block hash with conflicting identity/);
  const servicingFailure = assert.rejects(recovery.serviceRequested(), /reuses block hash with conflicting identity/);
  await waitUntil(() => source.snapshotCalls === 1);
  queue.push(head(103n, A));

  await Promise.all([requestedFailure, servicingFailure]);
  assert.equal(source.snapshotCalls, 1);
  assert.equal(indexer.health().ready, false);
});

test("recovery identity budget covers bounded pre and post-snapshot overflow sets", async () => {
  const target = snapshot(cursor(300n, hash("aa")));
  const source = new IdentitySource([target]);
  source.blockNextSnapshot();
  const indexer = QuoteIndexer.fromSnapshot(snapshot(cursor(100n, C)), deployment());
  const { queue, recovery } = harness(indexer, source);
  for (let offset = 0; offset < 8; offset += 1)
    queue.push(head(101n + BigInt(offset), hash((0x40 + offset).toString(16))));
  const requested = recovery.request();
  const servicing = recovery.serviceRequested();
  await waitUntil(() => source.snapshotCalls === 1);
  for (let offset = 0; offset < 8; offset += 1)
    queue.push(head(201n + BigInt(offset), hash((0x60 + offset).toString(16))));
  source.releaseSnapshot(target);

  await Promise.all([requested, servicing]);
  assert.equal(indexer.health().ready, true);
  assert.equal(indexer.health().cursor?.blockNumber, 300n);
});

class IdentitySource implements ChainDataSource {
  readonly network = Network.Base;
  snapshotCalls = 0;
  private blockedSnapshot?: Promise<BootstrapSnapshot>;
  private resolveBlockedSnapshot?: (value: BootstrapSnapshot) => void;

  constructor(private readonly snapshots: readonly BootstrapSnapshot[]) {}

  blockNextSnapshot(): void {
    this.blockedSnapshot = new Promise((resolve) => {
      this.resolveBlockedSnapshot = resolve;
    });
  }

  releaseSnapshot(value: BootstrapSnapshot): void {
    this.resolveBlockedSnapshot?.(value);
    this.blockedSnapshot = undefined;
    this.resolveBlockedSnapshot = undefined;
  }

  snapshot(): Promise<BootstrapSnapshot> {
    const index = Math.min(this.snapshotCalls, this.snapshots.length - 1);
    const result = this.blockedSnapshot ?? Promise.resolve(this.snapshots[index]!);
    this.snapshotCalls += 1;
    return result;
  }

  backfill(_request: BackfillRequest): Promise<readonly ContractLog[]> {
    return Promise.resolve([]);
  }

  async subscribe(_filter: ContractFilter, _signal?: AbortSignal): Promise<AsyncIterable<ChainUpdate>> {
    return { async *[Symbol.asyncIterator]() {} };
  }

  canonicalHead(): Promise<ChainCursor> {
    return Promise.resolve(this.snapshots.at(-1)!.cursor);
  }

  validateCheckpoint(_checkpoint: Checkpoint): Promise<boolean> {
    return Promise.resolve(true);
  }
}

function harness(indexer: QuoteIndexer, source: ChainDataSource) {
  const activity = new SourceActivity();
  const queue = new BoundedUpdateQueue(8, 1024 * 1024);
  activity.setActive(true);
  return {
    queue,
    recovery: new RecoveryCoordinator(indexer, source, config(), queue, activity, new AbortController().signal),
  };
}

function config(): ClientConnectConfig {
  return {
    deployment: deployment(),
    filter: { address: CORE, topics: [] },
    queueBound: 8,
    queueByteBound: 1024 * 1024,
    reconnectDelayMilliseconds: 20,
    sourceStallTimeoutMilliseconds: 1_000,
    sourceOperationTimeoutMilliseconds: 1_000,
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
    expectedImplementationCodeHash: C,
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    explicitLaneAssets: [],
  };
}

function snapshot(at: ChainCursor): BootstrapSnapshot {
  const state: QuoteState = { cash: CASH, cashReserve: 1_000_000n, lanes: new Map(), blacklistFeeMultiplier: 1n };
  return { state, cursor: at, implementation: IMPLEMENTATION, implementationCodeHash: C };
}

function cursor(blockNumber: bigint, blockHash: Hex): ChainCursor {
  return {
    chainId: 8453n,
    blockNumber,
    executionBlockNumber: blockNumber,
    blockHash,
    commitment: Commitment.Canonical,
  };
}

function head(blockNumber: bigint, blockHash: Hex): ChainUpdate {
  return { kind: "Head", head: { cursor: cursor(blockNumber, blockHash) } };
}

function hash(byte: string): Hex {
  return `0x${byte.repeat(32)}` as Hex;
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, "condition timed out");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}
