import { strict as assert } from "node:assert";
import { test } from "node:test";
import { EMPTY_SLOT0, encodeLaneSlot0, type QuoteState } from "@lunarbase/math";
import { Commitment, MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION, decodeCheckpoint, encodeCheckpoint, type Checkpoint } from "./index.js";
import { BaseFlashblocksNormalizer, CursorReorderBuffer, keccak256Hex, MonadExecutionNormalizer, QuoteReducer } from "./index.js";

const address = (last: string) => `0x${last.padStart(40, "0")}`;

test("Rust-compatible checkpoint codec round-trips state and cursor", () => {
  const cash = address("1"); const asset = address("2"); const router = address("3");
  const state: QuoteState = { cash, lanes: new Map([[asset, { slot0: encodeLaneSlot0({ ...EMPTY_SLOT0, price: 2n }), exists: true, paused: false, blockDelay: 0n, slippageKBps: 0n }]]), totalPrincipalAmount: new Map([[asset, 9n]]), whitelist: new Map([[router, true]]), blacklistFeeMultiplier: 1n, partnerFeeBps: new Map([[`${router}:${asset}`, 500_000n]]), stateVersion: 4n };
  const checkpoint: Checkpoint = { schemaVersion: SCHEMA_VERSION, mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION, expectedRuntimeCodeHash: `0x${"ab".repeat(32)}`, cursor: { chainId: 8453n, blockNumber: 8n, blockHash: `0x${"cd".repeat(32)}`, transactionIndex: 1n, logIndex: 2n, sourceSequence: 3n, sourceSubIndex: 4n, commitment: Commitment.Canonical }, state };
  const decoded = decodeCheckpoint(encodeCheckpoint(checkpoint));
  assert.deepEqual(decoded, checkpoint);
});

test("Monad filtered logs accept sparse global seqnos and reject regressions", () => {
  const normalizer = new MonadExecutionNormalizer(143n);
  const base = { sourceSubIndex: 0n, blockNumber: 10n, transactionIndex: 0n, logIndex: 0n, address: address("4"), topics: [], data: "0x", commitment: Commitment.Realtime } as const;
  assert.equal(normalizer.normalizeTxnLog({ ...base, sequence: 100n }) !== undefined, true);
  assert.equal(normalizer.normalizeTxnLog({ ...base, sequence: 104n, logIndex: 1n }) !== undefined, true);
  assert.equal(normalizer.normalizeTxnLog({ ...base, sequence: 104n, logIndex: 1n }), undefined);
  let failed = false;
  try { normalizer.normalizeTxnLog({ ...base, sequence: 103n, logIndex: 2n }); } catch { failed = true; }
  assert.equal(failed, true);
});

test("Base Flashblocks allows multiple logs at one flashblock index", () => {
  const normalizer = new BaseFlashblocksNormalizer(8453n);
  const header = { payloadId: "0x01", blockNumber: 10n, index: 0n } as const;
  const log = (addressValue: string) => ({ header, transactionIndex: 0n, logIndex: 0n, address: addressValue, topics: [], data: "0x", removed: false });
  assert.equal(normalizer.normalizeLog(log(address("4"))).length, 2);
  assert.equal(normalizer.normalizeLog(log(address("5"))).length, 1);
});

test("heads promote commitment without regressing an event cursor", () => {
  const reducer = new QuoteReducer({ cash: address("1"), lanes: new Map(), totalPrincipalAmount: new Map(), whitelist: new Map(), blacklistFeeMultiplier: 0n, partnerFeeBps: new Map(), stateVersion: 0n });
  const cursor = { chainId: 8453n, blockNumber: 10n, blockHash: `0x${"07".repeat(32)}`, transactionIndex: 0n, logIndex: 3n, commitment: Commitment.Realtime };
  reducer.bootstrap(cursor);
  reducer.observeHead({ chainId: 8453n, blockNumber: 10n, blockHash: cursor.blockHash, commitment: Commitment.Finalized });
  assert.equal(reducer.cursor()?.commitment, Commitment.Finalized);
  assert.equal(reducer.cursor()?.logIndex, 3n);
  reducer.observeHead({ chainId: 8453n, blockNumber: 9n, blockHash: `0x${"08".repeat(32)}`, commitment: Commitment.Realtime });
  assert.equal(reducer.cursor()?.blockNumber, 10n);
});

test("TypeScript RPC code hash uses Ethereum Keccak-256", () => {
  assert.equal(keccak256Hex(new Uint8Array()), "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
});

test("bounded reorder buffer emits deterministic cursor order", () => {
  const buffer = new CursorReorderBuffer(2);
  const later = { kind: "Head" as const, cursor: { chainId: 143n, blockNumber: 11n, commitment: Commitment.Realtime } };
  const earlier = { kind: "Head" as const, cursor: { chainId: 143n, blockNumber: 10n, commitment: Commitment.Realtime } };
  buffer.push(later);
  buffer.push(earlier);
  assert.deepEqual(buffer.drainAll(), [earlier, later]);
});

test("block head does not hide first log in the same block", () => {
  const asset = address("2");
  const reducer = new QuoteReducer({ cash: address("1"), lanes: new Map(), totalPrincipalAmount: new Map(), whitelist: new Map(), blacklistFeeMultiplier: 0n, partnerFeeBps: new Map(), stateVersion: 0n });
  reducer.bootstrap({ chainId: 8453n, blockNumber: 10n, commitment: Commitment.Realtime });
  reducer.apply({ chainId: 8453n, blockNumber: 10n, transactionIndex: 0n, logIndex: 0n, commitment: Commitment.Realtime }, { kind: "LaneAdded", asset });
  assert.equal(reducer.state().lanes.get(asset)?.exists, true);
});
