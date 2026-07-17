import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { EMPTY_SLOT0, encodeLaneSlot0, type QuoteState } from "@lunarbase/math";
import {
  BaseFlashblocksNormalizer,
  Commitment,
  ConnectedQuoteClient,
  CursorReorderBuffer,
  InMemoryRedisStore,
  MATH_COMPATIBILITY_VERSION,
  MonadExecutionNormalizer,
  Network,
  ProvisionalOverlay,
  QuoteReducer,
  SCHEMA_VERSION,
  decodeCheckpoint,
  encodeCheckpoint,
  hexString,
  keccak256Hex,
  parseNormalizedUpdate,
  type ChainEventSource,
  type Checkpoint,
  type DeploymentConfig,
  type SnapshotProvider,
} from "../index.js";

const address = (last: string) => `0x${last.padStart(40, "0")}`;

test("Rust-compatible checkpoint codec round-trips state and cursor", () => {
  const cash = address("1");
  const asset = address("2");
  const router = address("3");
  const state: QuoteState = {
    cash,
    lanes: new Map([
      [
        asset,
        {
          slot0: encodeLaneSlot0({ ...EMPTY_SLOT0, price: 2n }),
          exists: true,
          paused: false,
          blockDelay: 0n,
          slippageKBps: 0n,
        },
      ],
    ]),
    totalPrincipalAmount: new Map([[asset, 9n]]),
    whitelist: new Map([[router, true]]),
    blacklistFeeMultiplier: 1n,
    partnerFeeBps: new Map([[`${router}:${asset}`, 500_000n]]),
    stateVersion: 4n,
  };
  const checkpoint: Checkpoint = {
    schemaVersion: SCHEMA_VERSION,
    mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    expectedRuntimeCodeHash: `0x${"ab".repeat(32)}`,
    cursor: {
      chainId: 8453n,
      blockNumber: 8n,
      blockHash: `0x${"cd".repeat(32)}`,
      transactionIndex: 1n,
      logIndex: 2n,
      sourceSequence: 3n,
      sourceSubIndex: 4n,
      commitment: Commitment.Canonical,
    },
    state,
  };
  const decoded = decodeCheckpoint(encodeCheckpoint(checkpoint));
  assert.deepEqual(decoded, checkpoint);
});

test("Monad filtered logs accept sparse global seqnos and reject regressions", () => {
  const normalizer = new MonadExecutionNormalizer(143n);
  const base = {
    sourceSubIndex: 0n,
    blockNumber: 10n,
    transactionIndex: 0n,
    logIndex: 0n,
    address: address("4"),
    topics: [],
    data: "0x",
    commitment: Commitment.Realtime,
  } as const;
  assert.equal(normalizer.normalizeTxnLog({ ...base, sequence: 100n }) !== undefined, true);
  assert.equal(normalizer.normalizeTxnLog({ ...base, sequence: 104n, logIndex: 1n }) !== undefined, true);
  assert.equal(normalizer.normalizeTxnLog({ ...base, sequence: 104n, logIndex: 1n }), undefined);
  let failed = false;
  try {
    normalizer.normalizeTxnLog({ ...base, sequence: 103n, logIndex: 2n });
  } catch {
    failed = true;
  }
  assert.equal(failed, true);
});

test("Base Flashblocks allows multiple logs at one flashblock index", () => {
  const normalizer = new BaseFlashblocksNormalizer(8453n);
  const header = { payloadId: "0x01", blockNumber: 10n, index: 0n } as const;
  const log = (addressValue: string) => ({
    header,
    transactionIndex: 0n,
    logIndex: 0n,
    address: addressValue,
    topics: [],
    data: "0x",
    removed: false,
  });
  assert.equal(normalizer.normalizeLog(log(address("4"))).length, 2);
  assert.equal(normalizer.normalizeLog(log(address("5"))).length, 1);
});

test("heads promote commitment without regressing an event cursor", () => {
  const reducer = new QuoteReducer({
    cash: address("1"),
    lanes: new Map(),
    totalPrincipalAmount: new Map(),
    whitelist: new Map(),
    blacklistFeeMultiplier: 0n,
    partnerFeeBps: new Map(),
    stateVersion: 0n,
  });
  const cursor = {
    chainId: 8453n,
    blockNumber: 10n,
    blockHash: `0x${"07".repeat(32)}`,
    transactionIndex: 0n,
    logIndex: 3n,
    commitment: Commitment.Realtime,
  };
  reducer.bootstrap(cursor);
  reducer.observeHead({
    chainId: 8453n,
    blockNumber: 10n,
    blockHash: cursor.blockHash,
    commitment: Commitment.Finalized,
  });
  assert.equal(reducer.cursor()?.commitment, Commitment.Finalized);
  assert.equal(reducer.cursor()?.logIndex, 3n);
  reducer.observeHead({
    chainId: 8453n,
    blockNumber: 9n,
    blockHash: `0x${"08".repeat(32)}`,
    commitment: Commitment.Realtime,
  });
  assert.equal(reducer.cursor()?.blockNumber, 10n);
});

test("TypeScript RPC code hash uses Ethereum Keccak-256", () => {
  assert.equal(keccak256Hex(new Uint8Array()), "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
});

test("bounded reorder buffer emits deterministic cursor order", () => {
  const buffer = new CursorReorderBuffer(2);
  const later = { kind: "Head" as const, cursor: { chainId: 143n, blockNumber: 11n, commitment: Commitment.Realtime } };
  const earlier = {
    kind: "Head" as const,
    cursor: { chainId: 143n, blockNumber: 10n, commitment: Commitment.Realtime },
  };
  buffer.push(later);
  buffer.push(earlier);
  assert.deepEqual(buffer.drainAll(), [earlier, later]);
});

test("block head does not hide first log in the same block", () => {
  const asset = address("2");
  const reducer = new QuoteReducer({
    cash: address("1"),
    lanes: new Map(),
    totalPrincipalAmount: new Map(),
    whitelist: new Map(),
    blacklistFeeMultiplier: 0n,
    partnerFeeBps: new Map(),
    stateVersion: 0n,
  });
  reducer.bootstrap({ chainId: 8453n, blockNumber: 10n, commitment: Commitment.Realtime });
  reducer.apply(
    { chainId: 8453n, blockNumber: 10n, transactionIndex: 0n, logIndex: 0n, commitment: Commitment.Realtime },
    { kind: "LaneAdded", asset },
  );
  assert.equal(reducer.state().lanes.get(asset)?.exists, true);
});

test("connected client bootstraps from a bounded source handoff", async () => {
  const core = address("1");
  const config: DeploymentConfig = {
    network: Network.Base,
    chainId: 8453n,
    core,
    deploymentBlock: 1n,
    expectedRuntimeCodeHash: `0x${"00".repeat(32)}`,
    contractCompatibilityVersion: "test",
    httpRpcUrl: "http://127.0.0.1:8545",
    realtimeSource: "test",
    redis: { url: "redis://127.0.0.1/", streamMaxLen: 8, dedupTtlSeconds: 60n, checkpointIntervalUpdates: 1 },
    explicitLaneAssets: [],
    eagerRouters: [],
  };
  const checkpointStore = new InMemoryRedisStore(8);
  const provider: SnapshotProvider = {
    snapshot: async () => ({
      state: {
        cash: core,
        lanes: new Map(),
        totalPrincipalAmount: new Map(),
        whitelist: new Map(),
        blacklistFeeMultiplier: 0n,
        partnerFeeBps: new Map(),
        stateVersion: 0n,
      },
      cursor: { chainId: 8453n, blockNumber: 10n, commitment: Commitment.Finalized },
      runtimeCodeHash: config.expectedRuntimeCodeHash,
    }),
  };
  const source: ChainEventSource = {
    network: Network.Base,
    snapshotCursor: async () => ({ chainId: 8453n, blockNumber: 10n, commitment: Commitment.Finalized }),
    backfill: async () => [],
    subscribe: (_filter, signal) =>
      (async function* (): AsyncGenerator<never> {
        await new Promise<void>((resolve) => signal?.addEventListener("abort", () => resolve(), { once: true }));
        yield* [] as never[];
      })(),
  };
  const client = await ConnectedQuoteClient.connect(provider, source, {
    deployment: config,
    filter: { address: core, topics: [] },
    laneAssets: [],
    routers: [],
    bufferCapacity: 8,
    reconnectDelayMilliseconds: 10,
    checkpointStore,
  });
  await client.awaitReady(Commitment.Finalized);
  assert.equal(client.health().ready, true);
  assert.equal(checkpointStore.load()?.cursor.blockNumber, 10n);
  await client.shutdown();
  assert.equal(client.health().ready, false);
});

test("in-memory Redis checkpoint store deduplicates replayed cursors", () => {
  const store = new InMemoryRedisStore(8);
  const checkpoint: Checkpoint = {
    schemaVersion: SCHEMA_VERSION,
    mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    expectedRuntimeCodeHash: `0x${"00".repeat(32)}`,
    cursor: { chainId: 8453n, blockNumber: 1n, transactionIndex: 0n, logIndex: 0n, commitment: Commitment.Canonical },
    state: {
      cash: address("1"),
      lanes: new Map(),
      totalPrincipalAmount: new Map(),
      whitelist: new Map(),
      blacklistFeeMultiplier: 0n,
      partnerFeeBps: new Map(),
      stateVersion: 0n,
    },
  };
  const update = {
    kind: "Head" as const,
    cursor: { chainId: 8453n, blockNumber: 2n, commitment: Commitment.Canonical },
  };
  store.commit(checkpoint, [update]);
  store.commit(checkpoint, [update]);
  assert.equal(store.updates().length, 1);
});

test("versioned normalized replay fixture decodes in the TypeScript boundary", () => {
  const lines = readFileSync(
    new URL("../../../../fixtures/event-replay/monad-exec-events/normalized-updates.jsonl", import.meta.url),
    "utf8",
  )
    .trim()
    .split("\n");
  const updates = lines.map((line) => parseNormalizedUpdate(JSON.parse(line)));
  assert.deepEqual(
    updates.map((update) => update.kind),
    ["Head", "Log", "Head", "Gap"],
  );
  const first = updates[0];
  if (first.kind !== "Head") throw new Error("fixture must start with a head");
  assert.equal(first.cursor.chainId, 143n);
  assert.equal(first.cursor.sourceSequence, 1000n);
  const log = updates[1];
  if (log.kind !== "Log") throw new Error("fixture must contain a log");
  assert.equal(log.log.cursor.sourceSequence, 1004n);
  const gap = updates[3];
  if (gap.kind !== "Gap") throw new Error("fixture must end with a gap");
  assert.equal(gap.reason, "Monad parser subscription gap; skipped=3");
  const checkpoint: Checkpoint = {
    schemaVersion: SCHEMA_VERSION,
    mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    expectedRuntimeCodeHash: `0x${"00".repeat(32)}`,
    cursor: first.cursor,
    state: {
      cash: address("1"),
      lanes: new Map(),
      totalPrincipalAmount: new Map(),
      whitelist: new Map(),
      blacklistFeeMultiplier: 0n,
      partnerFeeBps: new Map(),
      stateVersion: 0n,
    },
  };
  assert.equal(
    hexString(encodeCheckpoint(checkpoint)),
    "0x4c4251310002000000446c756e6172626173652d636f6e74726163747340323464623437623836366538313530613064393163666664383065666534396466383531373962353a6d6174682d76310000000000000000000000000000000000000000000000000000000000000000000000000000008f00000000000002bc01aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000100000000000003e8000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
  );
});

test("provisional overlay commits only after canonical match", () => {
  const overlay = new ProvisionalOverlay();
  const cursor = {
    chainId: 8453n,
    blockNumber: 10n,
    transactionIndex: 0n,
    logIndex: 1n,
    commitment: Commitment.Realtime,
  };
  const event = { kind: "SwapExecuted" as const };
  overlay.begin({ ...cursor, logIndex: 0n });
  overlay.push(cursor, event);
  assert.deepEqual(overlay.commitCanonical([[cursor, event]]), cursor);
  assert.equal(overlay.updates().length, 0);
  overlay.begin(cursor);
  overlay.push(cursor, event);
  overlay.discard();
  assert.equal(overlay.updates().length, 0);
});
