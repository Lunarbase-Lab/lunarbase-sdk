import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { getAddress } from "viem";
import { createPairingStateStore, type PairingCheckpoint } from "./pairing-state.js";

const POOL = getAddress("0x0000000000000000000000000000000000000001");
const ACTOR = getAddress("0x0000000000000000000000000000000000000006");
const ASSET1 = getAddress("0x0000000000000000000000000000000000000003");
const CASH = getAddress("0x0000000000000000000000000000000000000002");
const HASH = `0x${"ab".repeat(32)}` as const;

async function withStore(run: (path: string) => Promise<void>): Promise<void> {
  const directory = await mkdtemp(join(tmpdir(), "lunarbase-pairing-state-"));
  try {
    await run(join(directory, "state.json"));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

test("atomically round-trips an opening checkpoint with mode 0600", async () => {
  await withStore(async (path) => {
    const store = createPairingStateStore({ chainId: 97, pool: POOL, actor: ACTOR }, path);
    const checkpoint: PairingCheckpoint = {
      cursor: { blockNumber: 123n, blockHash: HASH },
      phase: { kind: "opening" },
    };
    assert.equal(await store.load(), undefined);
    await store.save(checkpoint);
    assert.deepEqual(await store.load(), checkpoint);
    assert.equal((await stat(path)).mode & 0o777, 0o600);
  });
});

test("round-trips a pending return without number precision loss", async () => {
  await withStore(async (path) => {
    const store = createPairingStateStore({ chainId: 97, pool: POOL, actor: ACTOR }, path);
    const checkpoint: PairingCheckpoint = {
      cursor: { blockNumber: 123n, blockHash: HASH },
      phase: { kind: "return", assetIn: ASSET1, assetOut: CASH, maximumAmountIn: (1n << 255n) - 1n },
    };
    await store.save(checkpoint);
    assert.deepEqual(await store.load(), checkpoint);
  });
});

test("fails closed for malformed or mismatched checkpoint identity", async () => {
  await withStore(async (path) => {
    const store = createPairingStateStore({ chainId: 97, pool: POOL, actor: ACTOR }, path);
    await writeFile(path, "{}\n", { mode: 0o600 });
    await assert.rejects(store.load(), /malformed/);

    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        chainId: 56,
        pool: POOL,
        actor: ACTOR,
        cursor: { blockNumber: "123", blockHash: HASH },
        phase: { kind: "opening" },
      }),
    );
    await assert.rejects(store.load(), /another deployment or actor/);
    assert.doesNotMatch(await readFile(path, "utf8"), /private/i);
  });
});
