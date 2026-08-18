import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { Address, QuoteState } from "@lunarbase-lab/pmm-v2-math";
import * as HexValue from "ox/Hex";
import type { Hex } from "ox/Hex";
import {
  Commitment,
  CORE_EVENT_TOPICS,
  MATH_COMPATIBILITY_VERSION,
  Network,
  QuoteIndexer,
  type BlockRef,
  type BootstrapSnapshot,
  type ChainCorrection,
  type ContractLog,
  type DeploymentConfig,
} from "./index.js";

const CASH = "0x0000000000000000000000000000000000000001" as Address;
const CORE = "0x0000000000000000000000000000000000000004" as Address;
const IMPLEMENTATION = "0x8888888888888888888888888888888888888888" as Address;
const H100 = hash("10");

test("head at a correction new tip is not proof that its payload was applied", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const value = correction();
  indexer.applyCoreUpdate({ kind: "Head", head: value.newTip });

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Correction", correction: value }), {
    code: "GAP",
    message: "correction new tip has no matching applied envelope",
  });
  assert.equal(indexer.health().ready, false);
});

test("only the exact semantic correction retry is idempotent", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const value = correction();
  indexer.applyCoreUpdate({ kind: "Head", head: value.oldTip });
  indexer.applyCoreUpdate({ kind: "Correction", correction: value });

  const retryOld = transport(value.oldTip, 11n, Commitment.Canonical);
  const retryNew = transport(value.newTip, 12n, Commitment.Canonical);
  const semanticRetry: ChainCorrection = {
    commonAncestor: transport(value.commonAncestor, 10n, Commitment.Finalized),
    oldTip: retryOld,
    newTip: retryNew,
    oldBranch: [retryOld],
    newBranch: [retryNew],
    replacementLogs: [],
  };
  indexer.applyCoreUpdate({ kind: "Log", log: unknownLog(value.newTip) });
  indexer.applyCoreUpdate({ kind: "Correction", correction: semanticRetry });

  assert.equal(indexer.health().ready, true);
  assert.equal(indexer.correctionMetrics().appliedCorrections, 1);

  const altered: ChainCorrection = {
    ...value,
    replacementLogs: [unknownLog(value.newTip)],
  };
  assert.throws(() => indexer.applyCoreUpdate({ kind: "Correction", correction: altered }), {
    code: "GAP",
    message: "correction new tip has no matching applied envelope",
  });
  assert.equal(indexer.health().ready, false);
});

test("a correction cannot resurrect state after a true gap", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const value = correction();
  indexer.applyCoreUpdate({ kind: "Head", head: value.oldTip });
  assert.throws(() => indexer.applyCoreUpdate({ kind: "Gap", reason: "lost lifecycle delta" }), { code: "GAP" });

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Correction", correction: value }), {
    code: "GAP",
    message: "correction cannot repair invalid quote state; snapshot recovery required",
  });
  assert.equal(indexer.health().ready, false);
});

test("an exact correction retry remains idempotent after new-tip finalization", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const value = correction();
  indexer.applyCoreUpdate({ kind: "Head", head: value.oldTip });
  indexer.applyCoreUpdate({ kind: "Correction", correction: value });
  indexer.applyCoreUpdate({ kind: "Head", head: transport(value.newTip, 20n, Commitment.Finalized) });

  indexer.applyCoreUpdate({ kind: "Correction", correction: value });
  assert.equal(indexer.health().ready, true);
  assert.equal(indexer.health().commitment, Commitment.Finalized);
  assert.equal(indexer.correctionMetrics().appliedCorrections, 1);

  const altered = { ...value, replacementLogs: [unknownLog(value.newTip)] };
  assert.throws(() => indexer.applyCoreUpdate({ kind: "Correction", correction: altered }), {
    code: "GAP",
    message: "correction would roll back finalized state",
  });
  assert.equal(indexer.health().ready, false);
});

test("a correction cannot roll back a finalized old tip", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const value = correction();
  indexer.applyCoreUpdate({ kind: "Head", head: value.oldTip });
  indexer.applyCoreUpdate({ kind: "Head", head: transport(value.oldTip, 2n, Commitment.Finalized) });
  indexer.applyCoreUpdate({ kind: "Log", log: multiplierLog(value.oldTip) });
  assert.equal(indexer.health().commitment, Commitment.Finalized);

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Correction", correction: value }), {
    code: "GAP",
    message: "correction would roll back finalized state",
  });
  assert.equal(indexer.health().ready, false);
});

test("a later quote-critical event invalidates the correction retry marker", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const value = correction();
  indexer.applyCoreUpdate({ kind: "Head", head: value.oldTip });
  indexer.applyCoreUpdate({ kind: "Correction", correction: value });
  indexer.applyCoreUpdate({ kind: "Log", log: multiplierLog(value.newTip) });
  assert.equal(indexer.health().ready, true);

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Correction", correction: value }), {
    code: "GAP",
    message: "correction new tip has no matching applied envelope",
  });
  assert.equal(indexer.health().ready, false);
});

test("same positioned event requires an identical decoded payload", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const ref = block(101n, hash("41"), H100, 1n);
  const first = multiplierLog(ref);
  indexer.applyCoreUpdate({ kind: "Log", log: first });
  const metrics = indexer.correctionMetrics();

  indexer.applyCoreUpdate({
    kind: "Log",
    log: {
      ...first,
      cursor: { ...first.cursor, sourceSequence: 99n, sourceSubIndex: 77n },
    },
  });
  assert.equal(indexer.health().ready, true);
  assert.deepEqual(indexer.correctionMetrics(), metrics);

  assert.throws(
    () =>
      indexer.applyCoreUpdate({
        kind: "Log",
        log: { ...first, data: HexValue.fromNumber(3n, { size: 32 }) },
      }),
    { code: "REDUCER", message: "conflicting event payload at the same cursor" },
  );
  assert.equal(indexer.health().ready, false);
});

test("snapshot-covered corrections still require matching execution context", () => {
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const ancestor = block(99n, hash("09"), hash("08"), 0n, Commitment.Finalized);
  const oldTip = block(100n, hash("31"), hash("09"), 1n);
  const canonical = block(100n, H100, hash("09"), 2n);
  const conflictingExecution = {
    ...canonical,
    cursor: { ...canonical.cursor, executionBlockNumber: 999n },
  };
  const value: ChainCorrection = {
    commonAncestor: ancestor,
    oldTip,
    newTip: conflictingExecution,
    oldBranch: [oldTip],
    newBranch: [conflictingExecution],
    replacementLogs: [],
  };

  assert.throws(() => indexer.replayHandoff([{ kind: "Correction", correction: value }], snapshot().cursor), {
    code: "GAP",
    message: "same-block handoff execution context mismatch; canonical recovery required",
  });
  assert.equal(indexer.health().ready, false);
});

test("snapshot boundaries require identity and never weaken published finality", () => {
  const invalid: BootstrapSnapshot = {
    ...snapshot(),
    cursor: { ...snapshot().cursor, blockHash: undefined },
  };
  assert.throws(() => QuoteIndexer.fromSnapshot(invalid, deployment()), {
    code: "SOURCE",
    message: "snapshot cursor requires a non-zero block hash",
  });

  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  const weaker = { ...snapshot(), cursor: { ...snapshot().cursor, commitment: Commitment.Realtime } };
  assert.throws(() => indexer.installSnapshot(weaker, []), {
    code: "GAP",
    message: "recovery snapshot weakens finalized commitment",
  });
  assert.equal(indexer.health().ready, true);
  assert.equal(indexer.health().commitment, Commitment.Finalized);
});

test("same-block handoff promotes rather than downgrades commitment", () => {
  const provisional = { ...snapshot(), cursor: { ...snapshot().cursor, commitment: Commitment.Realtime } };
  const indexer = QuoteIndexer.fromSnapshot(provisional, deployment());
  const finalized = block(100n, H100, hash("09"), 1n, Commitment.Finalized);
  indexer.replayHandoff([{ kind: "Head", head: finalized }], provisional.cursor);
  assert.equal(indexer.health().commitment, Commitment.Finalized);
  assert.equal(indexer.health().ready, true);
});

test("correction ancestor must have a retained block identity", () => {
  const ancestor = block(101n, hash("41"), H100, 1n);
  const oldTip = block(102n, hash("42"), ancestor.cursor.blockHash!, 2n);
  const newTip = block(102n, hash("52"), ancestor.cursor.blockHash!, 3n);
  const value: ChainCorrection = {
    commonAncestor: ancestor,
    oldTip,
    newTip,
    oldBranch: [oldTip],
    newBranch: [newTip],
    replacementLogs: [],
  };
  const unknown = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  unknown.applyCoreUpdate({ kind: "Head", head: oldTip });
  assert.throws(() => unknown.applyCoreUpdate({ kind: "Correction", correction: value }), {
    code: "GAP",
    message: "correction ancestor identity is not retained",
  });

  const observed = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  observed.applyCoreUpdate({ kind: "Head", head: ancestor });
  observed.applyCoreUpdate({ kind: "Head", head: oldTip });
  observed.applyCoreUpdate({ kind: "Correction", correction: value });
  assert.equal(observed.health().ready, true);
  assert.equal(observed.health().cursor?.blockHash, newTip.cursor.blockHash);
});

test("a stale ignored head cannot relabel an observed correction ancestor", () => {
  const ancestor = block(101n, hash("61"), H100, 1n);
  const oldTip = block(102n, hash("62"), ancestor.cursor.blockHash!, 2n);
  const falseAncestor = block(101n, hash("71"), H100, 3n);
  const declaredOldTip = { ...oldTip, parentHash: falseAncestor.cursor.blockHash };
  const newTip = block(102n, hash("72"), falseAncestor.cursor.blockHash!, 4n);
  const value: ChainCorrection = {
    commonAncestor: falseAncestor,
    oldTip: declaredOldTip,
    newTip,
    oldBranch: [declaredOldTip],
    newBranch: [newTip],
    replacementLogs: [],
  };
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), deployment());
  indexer.applyCoreUpdate({ kind: "Head", head: ancestor });
  indexer.applyCoreUpdate({ kind: "Head", head: oldTip });
  indexer.applyCoreUpdate({ kind: "Head", head: falseAncestor });

  assert.throws(() => indexer.applyCoreUpdate({ kind: "Correction", correction: value }), {
    code: "GAP",
    message: "correction ancestor conflicts with observed block history",
  });
  assert.equal(indexer.health().ready, false);
});

function correction(): ChainCorrection {
  const commonAncestor = block(100n, H100, hash("09"), 0n, Commitment.Finalized);
  const oldTip = block(101n, hash("11"), H100, 1n);
  const newTip = block(101n, hash("21"), H100, 2n);
  return {
    commonAncestor,
    oldTip,
    newTip,
    oldBranch: [oldTip],
    newBranch: [newTip],
    replacementLogs: [],
  };
}

function unknownLog(ref: BlockRef): ContractLog {
  return {
    address: CORE,
    topics: [hash("ff")],
    data: "0x",
    removed: false,
    cursor: { ...ref.cursor, sourceSequence: 3n },
  };
}

function multiplierLog(ref: BlockRef): ContractLog {
  return {
    address: CORE,
    topics: [CORE_EVENT_TOPICS.BlacklistFeeMultiplierSet],
    data: HexValue.fromNumber(2n, { size: 32 }),
    removed: false,
    cursor: { ...ref.cursor, transactionIndex: 0n, logIndex: 0n, sourceSequence: 3n },
  };
}

function transport(ref: BlockRef, sourceSequence: bigint, commitment: Commitment): BlockRef {
  return { ...ref, cursor: { ...ref.cursor, sourceSequence, commitment } };
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

function snapshot(): BootstrapSnapshot {
  const state: QuoteState = {
    cash: CASH,
    cashReserve: 1_000_000n,
    lanes: new Map(),
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
    explicitLaneAssets: [],
  };
}

function hash(byte: string): Hex {
  return `0x${byte.repeat(32)}` as Hex;
}
