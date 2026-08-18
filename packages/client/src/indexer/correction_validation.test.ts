import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { Address } from "@lunarbase-lab/pmm-v2-math";
import type { Hex } from "ox/Hex";
import {
  chainCorrectionProtocolBytes,
  chainUpdateRetainedBytes,
  Commitment,
  type BlockRef,
  type ChainCorrection,
  type ContractLog,
} from "../model.js";
import { validateCorrection, validateCorrectionEnvelope } from "./correction_validation.js";

const CORE = "0x0000000000000000000000000000000000000004" as Address;
const H100 = hash("10");

test("source-sequence replacement logs provide provider-neutral deterministic order", () => {
  const value = correction();
  const withLogs: ChainCorrection = {
    ...value,
    replacementLogs: [sourceLog(value.newTip, 3n, 0n), sourceLog(value.newTip, 3n, 1n)],
  };

  assert.equal(validateCorrection(withLogs, value.oldTip.cursor, CORE, 8453n), true);
});

test("protocol byte cap counts decoded payload while runtime queues remain V8-conservative", () => {
  const value = correction();
  const payload = `0x${"ab".repeat(10 * 1024 * 1024)}` as Hex;
  const nearCap: ChainCorrection = {
    ...value,
    replacementLogs: [{ ...sourceLog(value.newTip, 3n, 0n), data: payload }],
  };

  assert.ok(chainUpdateRetainedBytes({ kind: "Correction", correction: nearCap }) > 16 * 1024 * 1024);
  assert.ok(chainCorrectionProtocolBytes(nearCap) < 16 * 1024 * 1024);
  assert.equal(validateCorrection(nearCap, value.oldTip.cursor, CORE, 8453n), true);
});

test("sourceSubIndex without sourceSequence is not an ordering identity", () => {
  const value = correction();
  const invalid: ChainCorrection = { ...value, replacementLogs: [unsequencedLog(value.newTip)] };

  assert.throws(() => validateCorrectionEnvelope(invalid, CORE, 8453n), {
    code: "GAP",
    message: "correction replacement log has no deterministic ordering identity",
  });
});

test("zero block hashes are rejected before correction replay", () => {
  const value = correction();
  const invalid: ChainCorrection = {
    ...value,
    commonAncestor: { ...value.commonAncestor, cursor: { ...value.commonAncestor.cursor, blockHash: hash("00") } },
  };

  assert.throws(() => validateCorrectionEnvelope(invalid, CORE, 8453n), {
    code: "GAP",
    message: "correction block has an invalid hash identity",
  });
});

test("declared branch tip must exactly match execution and lifecycle metadata", () => {
  const value = correction();
  const invalid: ChainCorrection = {
    ...value,
    newTip: {
      ...value.newTip,
      cursor: { ...value.newTip.cursor, executionBlockNumber: value.newTip.cursor.executionBlockNumber + 1n },
    },
  };

  assert.throws(() => validateCorrectionEnvelope(invalid, CORE, 8453n), {
    code: "GAP",
    message: "correction branch does not end at its declared tip",
  });
});

test("a correction must replace a distinct immutable tip identity", () => {
  const value = correction();
  const invalid: ChainCorrection = {
    ...value,
    newTip: value.oldTip,
    newBranch: value.oldBranch,
  };

  assert.throws(() => validateCorrectionEnvelope(invalid, CORE, 8453n), {
    code: "GAP",
    message: "correction old and new tips have the same block identity",
  });
});

test("same hash and height cannot disguise a changed execution context", () => {
  const value = correction();
  const reused = {
    ...value.oldTip,
    cursor: {
      ...value.oldTip.cursor,
      executionBlockNumber: value.oldTip.cursor.executionBlockNumber + 1n,
      sourceSequence: 2n,
    },
  };
  const invalid: ChainCorrection = { ...value, newTip: reused, newBranch: [reused] };

  assert.throws(() => validateCorrectionEnvelope(invalid, CORE, 8453n), {
    code: "GAP",
    message: "correction old and new tips have the same block identity",
  });
});

test("a block hash cannot be rebound to another height, execution, or parent", () => {
  const value = correction();
  const reused = block(101n, H100, H100, 2n);
  const invalid: ChainCorrection = { ...value, newTip: reused, newBranch: [reused] };

  assert.throws(() => validateCorrectionEnvelope(invalid, CORE, 8453n), {
    code: "GAP",
    message: "correction reuses a block hash with conflicting identity",
  });
});

test("duplicate correction identity includes execution block context", () => {
  const value = correction();
  const conflictingExecution = {
    ...value.newTip.cursor,
    executionBlockNumber: value.newTip.cursor.executionBlockNumber + 1n,
  };

  assert.throws(() => validateCorrection(value, conflictingExecution, CORE, 8453n), {
    code: "GAP",
    message: "correction old tip does not match published state",
  });
});

test("finalized state permits identity inspection but never a new rollback", () => {
  const value = correction();
  const finalizedOld = {
    ...value.oldTip,
    cursor: { ...value.oldTip.cursor, commitment: Commitment.Finalized },
  };
  const finalizedEnvelope: ChainCorrection = { ...value, oldTip: finalizedOld, oldBranch: [finalizedOld] };

  validateCorrectionEnvelope(finalizedEnvelope, CORE, 8453n);
  assert.throws(() => validateCorrection(finalizedEnvelope, finalizedOld.cursor, CORE, 8453n), {
    code: "GAP",
    message: "correction cannot replace finalized branch state",
  });
  assert.equal(
    validateCorrection(value, { ...value.newTip.cursor, commitment: Commitment.Finalized }, CORE, 8453n),
    false,
  );
});

test("replacement log commitment must match its declared branch block", () => {
  const value = correction();
  const log = sourceLog(value.newTip, 3n, 0n);
  const invalid: ChainCorrection = {
    ...value,
    replacementLogs: [{ ...log, cursor: { ...log.cursor, commitment: Commitment.Finalized } }],
  };

  assert.throws(() => validateCorrectionEnvelope(invalid, CORE, 8453n), {
    code: "GAP",
    message: "correction replacement log commitment does not match its branch",
  });
});

test("custom-source correction cursors require bounded integer and commitment shapes", () => {
  const value = correction();
  const invalidValues: ChainCorrection[] = [
    {
      ...value,
      commonAncestor: {
        ...value.commonAncestor,
        cursor: { ...value.commonAncestor.cursor, executionBlockNumber: -1n },
      },
    },
    {
      ...value,
      newTip: {
        ...value.newTip,
        cursor: { ...value.newTip.cursor, commitment: "Unsafe" as Commitment },
      },
    },
    {
      ...value,
      oldTip: {
        ...value.oldTip,
        cursor: { ...value.oldTip.cursor, sourceSequence: 1n << 64n },
      },
    },
    {
      ...value,
      replacementLogs: [
        {
          ...sourceLog(value.newTip, 3n, 0n),
          cursor: { ...sourceLog(value.newTip, 3n, 0n).cursor, transactionIndex: 1n << 32n, logIndex: 0n },
        },
      ],
    },
  ];

  for (const invalid of invalidValues)
    assert.throws(() => validateCorrectionEnvelope(invalid, CORE, 8453n), { code: "GAP" });
});

test("custom-source correction logs require canonical address, topic, and data hex", () => {
  const value = correction();
  const base = sourceLog(value.newTip, 3n, 0n);
  const invalidLogs = [
    { ...base, address: "0x01" as Address },
    { ...base, topics: ["0x01" as Hex] },
    { ...base, data: "0x1" as Hex },
  ];

  for (const log of invalidLogs)
    assert.throws(() => validateCorrectionEnvelope({ ...value, replacementLogs: [log] }, CORE, 8453n), { code: "GAP" });
});

function correction(): ChainCorrection {
  const ancestor = block(100n, H100, hash("09"), 0n, Commitment.Finalized);
  const oldTip = block(101n, hash("11"), H100, 1n);
  const newTip = block(101n, hash("21"), H100, 2n);
  return {
    commonAncestor: ancestor,
    oldTip,
    newTip,
    oldBranch: [oldTip],
    newBranch: [newTip],
    replacementLogs: [],
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

function sourceLog(ref: BlockRef, sourceSequence: bigint, sourceSubIndex: bigint): ContractLog {
  return {
    address: CORE,
    topics: [],
    data: "0x",
    removed: false,
    cursor: {
      chainId: 8453n,
      blockNumber: ref.cursor.blockNumber,
      executionBlockNumber: ref.cursor.executionBlockNumber,
      blockHash: ref.cursor.blockHash!,
      sourceSequence,
      sourceSubIndex,
      commitment: Commitment.Realtime,
    },
  };
}

function unsequencedLog(ref: BlockRef): ContractLog {
  const value = sourceLog(ref, 1n, 1n);
  return {
    ...value,
    cursor: {
      chainId: value.cursor.chainId,
      blockNumber: value.cursor.blockNumber,
      executionBlockNumber: value.cursor.executionBlockNumber,
      blockHash: value.cursor.blockHash!,
      sourceSubIndex: value.cursor.sourceSubIndex,
      commitment: value.cursor.commitment,
    },
  };
}

function hash(byte: string): Hex {
  return `0x${byte.repeat(32)}` as Hex;
}
