import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { Address, QuoteState } from "@lunarbase-lab/pmm-v2-math";
import type { Hex } from "ox/Hex";
import {
  Commitment,
  CursorReorderBuffer,
  MAX_CORRECTION_HISTORY_BLOCKS,
  MAX_CORRECTION_HISTORY_BYTES,
  QuoteReducer,
  compareCursor,
  type ChainCursor,
  type CorrectionJournalLimits,
  type ContractLog,
} from "./index.js";

const CASH = "0x0000000000000000000000000000000000000001" as Address;
const HASH = `0x${"11".repeat(32)}` as Hex;

test("correction journal resource limits are public and protocol bounded", () => {
  const limits: CorrectionJournalLimits = {
    blockCapacity: MAX_CORRECTION_HISTORY_BLOCKS,
    byteCapacity: MAX_CORRECTION_HISTORY_BYTES,
  };

  assert.deepEqual(limits, { blockCapacity: 128, byteCapacity: 16 * 1024 * 1024 });
});

test("positioned cursors ignore transport sequence and sub-index", () => {
  const positioned = cursor({ transactionIndex: 2n, logIndex: 3n, sourceSequence: 10n, sourceSubIndex: 1n });
  const retried = { ...positioned, sourceSequence: 99n, sourceSubIndex: 77n };

  assert.equal(compareCursor(positioned, retried), 0);
  assert.equal(compareCursor(retried, positioned), 0);

  const log: ContractLog = { address: CASH, topics: [HASH], data: "0x", removed: false, cursor: positioned };
  const buffer = new CursorReorderBuffer(2);
  buffer.push({ kind: "Log", log });
  assert.throws(() => buffer.push({ kind: "Log", log: { ...log, cursor: retried } }), {
    message: "multiple updates share one cursor",
  });

  const streamed = cursor({ sourceSequence: 10n, sourceSubIndex: 1n });
  assert.equal(compareCursor(streamed, { ...streamed, sourceSubIndex: 2n }), -1);
});

test("realtime same-block events cannot downgrade a finalized head", () => {
  const state: QuoteState = {
    cash: CASH,
    cashReserve: 1_000n,
    lanes: new Map(),
    blacklistFeeMultiplier: 1n,
  };
  const reducer = new QuoteReducer(state, "Whitelisted");
  const initial = cursor({ sourceSequence: 0n });
  reducer.bootstrap(initial);
  reducer.observeHead({ ...initial, commitment: Commitment.Finalized });

  reducer.apply(cursor({ transactionIndex: 0n, logIndex: 0n, sourceSequence: 2n }), {
    kind: "BlacklistFeeMultiplierSet",
    multiplier: 2n,
  });

  assert.equal(reducer.cursor()?.commitment, Commitment.Finalized);
  assert.equal(reducer.cursor()?.executionBlockNumber, initial.executionBlockNumber);
});

function cursor(overrides: Partial<ChainCursor>): ChainCursor {
  return {
    chainId: 8453n,
    blockNumber: 100n,
    executionBlockNumber: 500n,
    blockHash: HASH,
    commitment: Commitment.Realtime,
    ...overrides,
  };
}
