import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { Address, QuoteState } from "@lunarbase-lab/pmm-v2-math";
import type { Hex } from "ox/Hex";
import {
  BoundedRingBuffer,
  Commitment,
  ConnectedQuoteClient,
  MATH_COMPATIBILITY_VERSION,
  Network,
  QuoteIndexer,
  ownContractFilter,
  ownDeploymentConfig,
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
import { withDeadline } from "./indexer/lifecycle.js";
import { BoundedUpdateQueue } from "./indexer/update_queue.js";

const CASH = "0x0000000000000000000000000000000000000001" as Address;
const CORE = "0x0000000000000000000000000000000000000002" as Address;
const IMPLEMENTATION = "0x0000000000000000000000000000000000000003" as Address;
const HASH = `0x${"11".repeat(32)}` as Hex;

test("bounded ring buffer preserves FIFO order across wrap-around", () => {
  const ring = new BoundedRingBuffer<number>(3);
  assert.equal(ring.push(1), true);
  assert.equal(ring.push(2), true);
  assert.equal(ring.push(3), true);
  assert.equal(ring.push(4), false);
  assert.equal(ring.shift(), 1);
  assert.equal(ring.push(4), true);
  assert.equal(ring.peek(1), 3);
  assert.deepEqual(ring.drainAll(), [2, 3, 4]);
  assert.equal(ring.length, 0);
});

test("queue and deadline paths release abort listeners", async () => {
  const tracked = new TrackedAbortSignal();
  const queue = new BoundedUpdateQueue(2, 1024);
  const pending = queue.next(tracked.signal);
  queue.push({ kind: "Head", cursor: cursor() });
  assert.equal((await pending)?.kind, "Head");
  assert.equal(tracked.listeners, 0);

  await assert.rejects(
    withDeadline("hung operation", 5, tracked.signal, () => new Promise<never>(() => undefined)),
    /exceeded its 5 ms deadline/,
  );
  assert.equal(tracked.listeners, 0);

  const inner = new TrackedAbortSignal();
  await assert.rejects(
    withDeadline(
      "nested wait",
      5,
      tracked.signal,
      () => queue.next(inner.signal),
      () => inner.abort(),
    ),
    /deadline/,
  );
  assert.equal(inner.listeners, 0);
});

test("deployment and filter ownership detach caller mutations", () => {
  const input = deployment();
  const filter: ContractFilter = { address: CORE, topics: [HASH] };
  const ownedDeployment = ownDeploymentConfig(input);
  const ownedFilter = ownContractFilter(filter);
  const indexer = QuoteIndexer.fromSnapshot(snapshot(), input);

  (input as MutableDeployment).core = CASH;
  (input as MutableDeployment).expectedImplementationCodeHash = `0x${"22".repeat(32)}` as Hex;
  (filter as { address: Address }).address = CASH;

  assert.equal(ownedDeployment.core, CORE);
  assert.equal(ownedFilter.address, CORE);
  assert.equal(indexer.health().implementationCodeHash, HASH);
  assert.equal(Object.isFrozen(ownedDeployment), true);
  assert.equal(Object.isFrozen(ownedDeployment.explicitLaneAssets), true);
  assert.equal(Object.isFrozen(ownedFilter.topics), true);
});

test("connect fails within the source operation deadline for a hung subscription", async () => {
  const source = new HungSubscribeSource();
  const started = performance.now();
  await assert.rejects(ConnectedQuoteClient.connect(connectConfig(20), source), /deadline/);
  assert.ok(performance.now() - started < 250);
});

test("shutdown cancels recovery and bounds a hung iterator return", async () => {
  const source = new HungIteratorSource();
  const client = await ConnectedQuoteClient.connect(connectConfig(25), source);
  source.hangSnapshot = true;
  const recovery = client.recover();
  await Promise.resolve();

  const started = performance.now();
  await client.shutdown(250);
  await recovery;
  assert.ok(performance.now() - started < 250);
  assert.equal(source.returnCalls, 1);
  assert.equal(client.health().ready, false);
  assert.throws(() => client.quoteMany([]), { code: "NOT_READY" });
});

class HungSubscribeSource implements ChainDataSource {
  readonly network = Network.Base;
  snapshot(): Promise<BootstrapSnapshot> {
    return Promise.resolve(snapshot());
  }
  backfill(_request: BackfillRequest): Promise<readonly ContractLog[]> {
    return Promise.resolve([]);
  }
  subscribe(_filter: ContractFilter, _signal?: AbortSignal): Promise<AsyncIterable<ChainUpdate>> {
    return new Promise(() => undefined);
  }
  canonicalHead(): Promise<ChainCursor> {
    return Promise.resolve(cursor());
  }
  validateCheckpoint(_checkpoint: Checkpoint): Promise<boolean> {
    return Promise.resolve(false);
  }
}

class HungIteratorSource extends HungSubscribeSource {
  hangSnapshot = false;
  returnCalls = 0;

  override snapshot(): Promise<BootstrapSnapshot> {
    return this.hangSnapshot ? new Promise(() => undefined) : Promise.resolve(snapshot());
  }

  override async subscribe(): Promise<AsyncIterable<ChainUpdate>> {
    return {
      [Symbol.asyncIterator]: (): AsyncIterator<ChainUpdate> => ({
        next: () => new Promise(() => undefined),
        return: () => {
          this.returnCalls += 1;
          return new Promise(() => undefined);
        },
      }),
    };
  }
}

class TrackedAbortSignal {
  private readonly callbacks = new Set<EventListenerOrEventListenerObject>();
  readonly signal = this as unknown as AbortSignal;
  aborted = false;

  get listeners(): number {
    return this.callbacks.size;
  }

  addEventListener(type: string, callback: EventListenerOrEventListenerObject | null): void {
    if (type === "abort" && callback) this.callbacks.add(callback);
  }

  removeEventListener(type: string, callback: EventListenerOrEventListenerObject | null): void {
    if (type === "abort" && callback) this.callbacks.delete(callback);
  }

  abort(): void {
    if (this.aborted) return;
    this.aborted = true;
    for (const callback of [...this.callbacks]) {
      if (typeof callback === "function") callback.call(this.signal, new Event("abort"));
      else callback.handleEvent(new Event("abort"));
    }
  }
}

type MutableDeployment = {
  core: Address;
  expectedImplementationCodeHash: Hex;
};

function connectConfig(operationTimeout: number): ClientConnectConfig {
  return {
    deployment: deployment(),
    filter: { address: CORE, topics: [] },
    queueBound: 16,
    queueByteBound: 1024 * 1024,
    reconnectDelayMilliseconds: 5,
    sourceStallTimeoutMilliseconds: 1_000,
    sourceOperationTimeoutMilliseconds: operationTimeout,
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
    expectedImplementationCodeHash: HASH,
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    explicitLaneAssets: [],
  };
}

function snapshot(): BootstrapSnapshot {
  const state: QuoteState = {
    cash: CASH,
    cashReserve: 1_000_000n,
    lanes: new Map(),
    blacklistFeeMultiplier: 1n,
  };
  return { state, cursor: cursor(), implementation: IMPLEMENTATION, implementationCodeHash: HASH };
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
