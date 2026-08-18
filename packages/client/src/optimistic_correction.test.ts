import { strict as assert } from "node:assert";
import { test } from "node:test";
import { WAD, createLaneState, type Address, type QuoteRequest, type QuoteState } from "@lunarbase-lab/pmm-v2-math";
import { encodeLaneSlot0 } from "@lunarbase-lab/pmm-v2-math/slot0";
import * as HexValue from "ox/Hex";
import type { Hex } from "ox/Hex";
import {
  Commitment,
  CORE_EVENT_TOPICS,
  MATH_COMPATIBILITY_VERSION,
  Network,
  QuoteIndexer,
  QuoteReducer,
  type BlockRef,
  type BootstrapSnapshot,
  type ChainCorrection,
  type ChainCursor,
  type ContractLog,
  type DeploymentConfig,
  type IndexerLifecycleEvent,
} from "./index.js";

const CASH = "0x0000000000000000000000000000000000000001" as Address;
const ASSET = "0x0000000000000000000000000000000000000002" as Address;
const CORE = "0x0000000000000000000000000000000000000004" as Address;
const IMPLEMENTATION = "0x8888888888888888888888888888888888888888" as Address;
const H100 = hash("10");

test("resolved correction swaps state atomically, stays ready, and notifies asynchronously", async () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const baseline = indexer.quote(request()).outcome;
  const checkpoint = indexer.checkpoint();
  const old101 = block(101n, hash("11"), H100, 1n);
  const next101 = block(101n, hash("21"), H100, 3n);
  const notices: IndexerLifecycleEvent[] = [];
  let listenerReady = false;
  indexer.onLifecycle((event) => {
    notices.push(event);
    listenerReady = indexer.health().ready;
  });

  indexer.applyCoreUpdate({ kind: "Head", head: old101 });
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old101, 0n, true, 2n) });
  assert.throws(() => assert.deepEqual(indexer.quote(request()).outcome, baseline));
  assert.deepEqual(indexer.checkpoint(), checkpoint, "optimistic head must not replace stable checkpoint");

  const correction = fork(H100, old101, next101, [old101], [next101], []);
  indexer.applyCoreUpdate({ kind: "Correction", correction });
  assert.equal(indexer.health().ready, true);
  assert.deepEqual(indexer.quote(request()).outcome, baseline);
  assert.deepEqual(indexer.correctionMetrics(), {
    appliedCorrections: 1,
    journalBlocks: 0,
    journalEvictions: 0,
    journalRetainedBytes: 0,
  });
  assert.equal(notices.length, 0, "user callbacks must not run inside correction reduction");
  await Promise.resolve();
  assert.equal(notices[0]?.kind, "CorrectionApplied");
  assert.equal(listenerReady, true);

  indexer.applyCoreUpdate({ kind: "Correction", correction });
  assert.equal(indexer.health().ready, true);
  assert.equal(indexer.correctionMetrics().appliedCorrections, 1, "duplicate correction must be idempotent");
  await Promise.resolve();
  assert.equal(notices.length, 1);
});

test("corrections preserve the stable checkpoint and support a later deeper fork", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const initial = indexer.quote(request()).outcome;
  const stableCheckpoint = indexer.checkpoint();
  const ancestor = block(100n, H100, hash("09"), 0n, Commitment.Finalized);
  const shared101 = block(101n, hash("11"), H100, 1n);
  const old102 = block(102n, hash("12"), hash("11"), 2n);
  const next102 = block(102n, hash("22"), hash("11"), 3n);
  const next101 = block(101n, hash("31"), H100, 4n);

  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(shared101, 0n, true, 1n) });
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old102, 0n, false, 2n) });
  indexer.applyCoreUpdate({
    kind: "Correction",
    correction: {
      commonAncestor: shared101,
      oldTip: old102,
      newTip: next102,
      oldBranch: [old102],
      newBranch: [next102],
      replacementLogs: [],
    },
  });
  assert.equal(indexer.health().ready, true);
  assert.throws(() => assert.deepEqual(indexer.quote(request()).outcome, initial));
  assert.deepEqual(indexer.checkpoint(), stableCheckpoint);

  indexer.applyCoreUpdate({
    kind: "Correction",
    correction: {
      commonAncestor: ancestor,
      oldTip: next102,
      newTip: next101,
      oldBranch: [shared101, next102],
      newBranch: [next101],
      replacementLogs: [],
    },
  });
  assert.equal(indexer.health().ready, true);
  assert.deepEqual(indexer.quote(request()).outcome, initial);
  assert.deepEqual(indexer.checkpoint(), stableCheckpoint);
  assert.equal(indexer.correctionMetrics().appliedCorrections, 2);
  assert.equal(indexer.correctionMetrics().journalBlocks, 0);

  const restarted = QuoteIndexer.fromCheckpoint(indexer.checkpoint()!, deployment());
  assert.deepEqual(restarted.quote(request()).outcome, initial);
});

test("corrections cannot cross a finalized floor behind a newer realtime head", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const old101 = block(101n, hash("11"), H100, 1n);
  const old102 = block(102n, hash("12"), hash("11"), 2n);
  const old103 = block(103n, hash("13"), hash("12"), 3n);
  const old104 = block(104n, hash("14"), hash("13"), 4n);
  const old105 = block(105n, hash("15"), hash("14"), 5n);
  const finalized105 = block(105n, hash("15"), hash("14"), 6n, Commitment.Finalized);
  const old106 = block(106n, hash("16"), hash("15"), 7n);
  for (const head of [old101, old102, old103, old104, old105, finalized105, old106])
    indexer.applyCoreUpdate({ kind: "Head", head });

  const next104 = block(104n, hash("24"), hash("13"), 8n);
  const next105 = block(105n, hash("25"), hash("24"), 9n);
  const next106 = block(106n, hash("26"), hash("25"), 10n);
  const correction: ChainCorrection = {
    commonAncestor: old103,
    oldTip: old106,
    newTip: next106,
    oldBranch: [old104, old105, old106],
    newBranch: [next104, next105, next106],
    replacementLogs: [],
  };

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Correction", correction }), {
    code: "GAP",
    message: "correction would roll back finalized state",
  });
  assert.equal(indexer.health().ready, false);
});

test("late log after a newer head mutates state without cursor regression", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const head102 = block(102n, hash("12"), hash("11"), 1n);
  const log101 = block(101n, hash("11"), H100, 2n);

  indexer.applyCoreUpdate({ kind: "Head", head: head102 });
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(log101, 0n, true, 2n) });

  const health = indexer.health();
  assert.equal(health.ready, true);
  assert.equal(health.cursor?.blockNumber, 102n);
  assert.equal(health.cursor?.executionBlockNumber, 102n);
  assert.equal(health.cursor?.sourceSequence, 2n, "state revision must advance even when head height stays newer");
  assert.throws(() =>
    assert.deepEqual(
      indexer.quote(request()).outcome,
      QuoteIndexer.fromSnapshot(snapshot(), deployment()).quote(request()).outcome,
    ),
  );
});

test("head cannot relabel mutated same-height state with another hash", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const old101 = block(101n, hash("11"), H100, 1n);
  const conflicting = block(101n, hash("21"), H100, 2n);
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old101, 0n, true, 1n) });

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Head", head: conflicting }), { code: "REDUCER" });
  assert.equal(indexer.health().ready, false);

  const headOnly = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  headOnly.applyCoreUpdate({ kind: "Head", head: old101 });
  headOnly.applyCoreUpdate({ kind: "Head", head: conflicting });
  assert.equal(headOnly.health().ready, true);
  assert.equal(headOnly.health().cursor?.blockHash, conflicting.cursor.blockHash);
  const conflictingExecution = {
    ...conflicting,
    cursor: { ...conflicting.cursor, executionBlockNumber: 102n, sourceSequence: 3n },
  };
  assert.throws(() => headOnly.applyCoreUpdate({ kind: "Head", head: conflictingExecution }), { code: "REDUCER" });
  assert.equal(headOnly.health().ready, false);
});

test("mutated same-height state requires stable hash and execution identity", () => {
  const reducer = new QuoteReducer(snapshot().state, "Whitelisted");
  reducer.bootstrap(snapshot().cursor);
  const noHash: ChainCursor = {
    chainId: 8453n,
    blockNumber: 101n,
    executionBlockNumber: 101n,
    transactionIndex: 0n,
    logIndex: 0n,
    sourceSequence: 1n,
    commitment: Commitment.Realtime,
  };
  reducer.apply(noHash, { kind: "LanePausedSet", asset: ASSET, paused: true });
  const hashedHead: ChainCursor = {
    chainId: 8453n,
    blockNumber: 101n,
    executionBlockNumber: 101n,
    blockHash: hash("11"),
    sourceSequence: 2n,
    commitment: Commitment.Realtime,
  };

  assert.throws(() => reducer.observeHead(hashedHead), { code: "BLOCK_HASH_MISMATCH" });
  assert.throws(
    () =>
      reducer.apply(
        { ...noHash, blockHash: hash("11"), logIndex: 1n },
        { kind: "LanePausedSet", asset: ASSET, paused: false },
      ),
    { code: "BLOCK_HASH_MISMATCH" },
  );
  reducer.apply(
    { ...noHash, logIndex: 1n, sourceSequence: 2n },
    { kind: "LanePausedSet", asset: ASSET, paused: false },
  );
  assert.equal(reducer.isReady(), true, "both-missing transport ordering remains valid");
  const eventReducer = new QuoteReducer(snapshot().state, "Whitelisted");
  eventReducer.bootstrap(snapshot().cursor);
  const eventCursor = { ...hashedHead, transactionIndex: 0n, logIndex: 0n };
  eventReducer.apply(eventCursor, { kind: "LanePausedSet", asset: ASSET, paused: true });
  const wrongExecutionEvent = { ...eventCursor, executionBlockNumber: 102n, logIndex: 1n, sourceSequence: 3n };
  assert.throws(() => eventReducer.apply(wrongExecutionEvent, { kind: "LanePausedSet", asset: ASSET, paused: false }), {
    code: "BLOCK_HASH_MISMATCH",
  });
  assert.throws(() => eventReducer.observeHead({ ...hashedHead, executionBlockNumber: 102n }), {
    code: "BLOCK_HASH_MISMATCH",
  });
  const headEventReducer = new QuoteReducer(snapshot().state, "Whitelisted");
  headEventReducer.bootstrap(hashedHead);
  assert.throws(
    () => headEventReducer.apply(wrongExecutionEvent, { kind: "LanePausedSet", asset: ASSET, paused: true }),
    {
      code: "BLOCK_HASH_MISMATCH",
    },
  );
});

test("full journal accepts multiple logs in its current block", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment(), {
    blockCapacity: 1,
    byteCapacity: 4_096,
  });
  const old101 = block(101n, hash("11"), H100, 1n);
  const next101 = block(101n, hash("21"), H100, 4n);
  const baseline = QuoteIndexer.fromSnapshot(snapshot(), deployment()).quote(request()).outcome;

  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old101, 0n, true, 1n) });
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old101, 1n, false, 2n) });
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old101, 2n, true, 3n) });
  assert.equal(indexer.correctionMetrics().journalBlocks, 1);
  assert.equal(indexer.health().ready, true);

  indexer.applyCoreUpdate({
    kind: "Correction",
    correction: fork(H100, old101, next101, [old101], [next101], []),
  });
  assert.equal(indexer.health().ready, true);
  assert.deepEqual(indexer.quote(request()).outcome, baseline);
});

test("late mutation fails closed after its block identity leaves the bounded header window", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment()),
    heads = chain(129, "40");
  for (const head of heads) indexer.applyCoreUpdate({ kind: "Head", head });

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(heads[0]!, 0n, true, 130n) }), {
    code: "GAP",
    message: "optimistic mutation is outside retained block identity history",
  });
  assert.equal(indexer.health().ready, false);
});

test("correction beyond retained history becomes a true observable gap", async () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment(), {
    blockCapacity: 1,
    byteCapacity: 4_096,
  });
  const old101 = block(101n, hash("11"), H100, 1n);
  const old102 = block(102n, hash("12"), hash("11"), 2n);
  const next101 = block(101n, hash("21"), H100, 3n);
  const next102 = block(102n, hash("22"), hash("21"), 4n);
  const notices: IndexerLifecycleEvent[] = [];
  indexer.onLifecycle((event) => notices.push(event));
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old101, 0n, true, 1n) });
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old102, 0n, false, 2n) });
  assert.equal(indexer.correctionMetrics().journalEvictions, 1);

  assert.throws(
    () =>
      indexer.applyCoreUpdate({
        kind: "Correction",
        correction: fork(H100, old102, next102, [old101, old102], [next101, next102], []),
      }),
    { code: "GAP", message: "correction exceeds retained optimistic history" },
  );
  assert.equal(indexer.health().ready, false);
  await Promise.resolve();
  assert.equal(notices.at(-1)?.kind, "Gap");
});

test("retained block identities must match every block in the old correction branch", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const old101 = block(101n, hash("11"), H100, 1n);
  const old102 = block(102n, hash("12"), hash("11"), 2n);
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old101, 0n, true, 1n) });
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old102, 0n, false, 2n) });
  const wrong101 = { ...old101, cursor: { ...old101.cursor, executionBlockNumber: 999n, sourceSequence: 3n } };
  const declaredOld102 = { ...old102, cursor: { ...old102.cursor, sourceSequence: 4n } };
  const next101 = block(101n, hash("21"), H100, 5n);

  assert.throws(
    () =>
      indexer.applyCoreUpdate({
        kind: "Correction",
        correction: fork(H100, declaredOld102, next101, [wrong101, declaredOld102], [next101], []),
      }),
    { code: "GAP", message: "old correction branch conflicts with retained optimistic state" },
  );
  assert.equal(indexer.health().ready, false);
});

test("handoff ordering never moves logs across a correction barrier", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const old101 = block(101n, hash("11"), H100, 1n);
  const old102 = block(102n, hash("12"), hash("11"), 2n);
  const next101 = block(101n, hash("21"), H100, 3n);
  const next102 = block(102n, hash("22"), hash("21"), 4n);
  const correction = fork(H100, old102, next101, [old101, old102], [next101], []);

  indexer.replayHandoff(
    [
      { kind: "Log", log: pauseLog(old102, 0n, true, 2n) },
      { kind: "Correction", correction },
      { kind: "Log", log: pauseLog(next102, 0n, true, 4n) },
    ],
    snapshot().cursor,
  );

  assert.equal(indexer.health().ready, true);
  assert.equal(indexer.health().cursor?.blockHash, next102.cursor.blockHash);
  assert.equal(indexer.correctionMetrics().appliedCorrections, 1);
});

test("snapshot-covered correction barriers are validated and observed during handoff", () => {
  const later = { ...snapshot(), cursor: block(102n, hash("32"), hash("31"), 10n).cursor };
  const indexer = QuoteIndexer.fromSnapshot(later, deployment());
  const old101 = block(101n, hash("11"), H100, 1n);
  const next101 = block(101n, hash("21"), H100, 2n);

  indexer.replayHandoff(
    [{ kind: "Correction", correction: fork(H100, old101, next101, [old101], [next101], []) }],
    later.cursor,
  );

  assert.equal(indexer.health().cursor?.blockNumber, 102n);
  assert.equal(indexer.correctionMetrics().appliedCorrections, 1);
});

test("source-sequence replacement logs replay through the full client correction path", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const baseline = indexer.quote(request()).outcome;
  const old101 = block(101n, hash("11"), H100, 1n);
  const next101 = block(101n, hash("21"), H100, 4n);
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old101, 0n, true, 1n) });

  indexer.applyCoreUpdate({
    kind: "Correction",
    correction: fork(H100, old101, next101, [old101], [next101], [sourcePauseLog(next101, false, 3n)]),
  });

  assert.equal(indexer.health().ready, true);
  assert.deepEqual(indexer.quote(request()).outcome, baseline);
});

test("replacement logs must be strictly ordered and remain inside new branch", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const old101 = block(101n, hash("11"), H100, 1n);
  const next101 = block(101n, hash("21"), H100, 2n);
  indexer.applyCoreUpdate({ kind: "Log", log: pauseLog(old101, 0n, true, 1n) });
  const logs = [pauseLog(next101, 1n, false, 3n), pauseLog(next101, 0n, true, 2n)];

  assert.throws(
    () =>
      indexer.applyCoreUpdate({
        kind: "Correction",
        correction: fork(H100, old101, next101, [old101], [next101], logs),
      }),
    { code: "GAP", message: "correction replacement logs are not strictly ordered" },
  );
  assert.equal(indexer.health().ready, false);
});

test("correction protocol count caps fail closed before candidate replay", () => {
  const branch = chain(129, "40");
  const branchLimited = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  branchLimited.applyCoreUpdate({ kind: "Head", head: branch.at(-1)! });
  assert.throws(
    () =>
      branchLimited.applyCoreUpdate({
        kind: "Correction",
        correction: fork(H100, branch.at(-1)!, block(100n, H100, hash("09"), 200n), branch, [], []),
      }),
    { code: "GAP", message: "correction branch exceeds 128-block protocol limit" },
  );

  const logLimited = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const ancestor = block(100n, H100, hash("09"), 0n, Commitment.Finalized);
  const excessLogs = new Array<ContractLog>(8_193).fill(pauseLog(ancestor, 0n, true, 1n));
  assert.throws(
    () =>
      logLimited.applyCoreUpdate({
        kind: "Correction",
        correction: fork(H100, ancestor, ancestor, [], [], excessLogs),
      }),
    { code: "GAP", message: "correction exceeds 8192-log protocol limit" },
  );
});

function fork(
  ancestorHash: Hex,
  oldTip: BlockRef,
  newTip: BlockRef,
  oldBranch: readonly BlockRef[],
  newBranch: readonly BlockRef[],
  replacementLogs: readonly ContractLog[],
): ChainCorrection {
  return {
    commonAncestor: block(100n, ancestorHash, hash("09"), 0n, Commitment.Finalized),
    oldTip,
    newTip,
    oldBranch,
    newBranch,
    replacementLogs,
  };
}

function block(
  blockNumber: bigint,
  blockHash: Hex,
  parentHash: Hex,
  sourceSequence: bigint,
  commitment = Commitment.Realtime,
): BlockRef {
  return {
    cursor: { chainId: 8453n, blockNumber, executionBlockNumber: blockNumber, blockHash, sourceSequence, commitment },
    parentHash,
  };
}

function pauseLog(blockRef: BlockRef, logIndex: bigint, paused: boolean, sourceSequence: bigint): ContractLog {
  return {
    address: CORE,
    topics: [CORE_EVENT_TOPICS.LanePausedSet, HexValue.padLeft(ASSET, 32)],
    data: HexValue.concat(
      HexValue.fromNumber(paused ? 0 : 1, { size: 32 }),
      HexValue.fromNumber(paused ? 1 : 0, { size: 32 }),
    ),
    removed: false,
    cursor: { ...blockRef.cursor, transactionIndex: 0n, logIndex, sourceSequence },
  };
}

function sourcePauseLog(blockRef: BlockRef, paused: boolean, sourceSequence: bigint): ContractLog {
  const log = pauseLog(blockRef, 0n, paused, sourceSequence);
  return {
    ...log,
    cursor: {
      chainId: blockRef.cursor.chainId,
      blockNumber: blockRef.cursor.blockNumber,
      executionBlockNumber: blockRef.cursor.executionBlockNumber,
      blockHash: blockRef.cursor.blockHash!,
      sourceSequence,
      commitment: blockRef.cursor.commitment,
    },
  };
}

function snapshot(): BootstrapSnapshot {
  const state: QuoteState = {
    cash: CASH,
    cashReserve: 1_000_000n,
    lanes: new Map([[ASSET, createLaneState(laneSlot0(), 1_000_000n, 0n)]]),
    blacklistFeeMultiplier: 1n,
  };
  return {
    state,
    cursor: block(100n, H100, hash("09"), 0n, Commitment.Finalized).cursor,
    implementation: IMPLEMENTATION,
    implementationCodeHash: hash("88"),
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
    expectedImplementationCodeHash: hash("88"),
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    explicitLaneAssets: [ASSET],
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

function request(): QuoteRequest {
  return { assetIn: CASH, assetOut: ASSET, amount: 1_000n, mode: "ExactIn" };
}

function hash(byte: string): Hex {
  return `0x${byte.repeat(32)}` as Hex;
}

function chain(count: number, firstByte: string): BlockRef[] {
  const blocks: BlockRef[] = [];
  let parent = H100;
  for (let offset = 0; offset < count; offset += 1) {
    const value = (Number.parseInt(firstByte, 16) + offset).toString(16).padStart(2, "0").slice(-2);
    const blockHash = hash(value);
    blocks.push(block(101n + BigInt(offset), blockHash, parent, BigInt(offset + 1)));
    parent = blockHash;
  }
  return blocks;
}
