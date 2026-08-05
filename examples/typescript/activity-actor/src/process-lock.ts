import { open, unlink } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Address } from "viem";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const lockPath = resolve(packageRoot, ".actor.lock");

/** Prevents two local processes from sharing one actor nonce stream. */
export async function acquireProcessLock(actor: Address): Promise<() => Promise<void>> {
  let handle;
  try {
    handle = await open(lockPath, "wx", 0o600);
    await handle.writeFile(JSON.stringify({ pid: process.pid, actor, startedAt: new Date().toISOString() }));
    await handle.close();
  } catch (error) {
    await handle?.close();
    if ((error as NodeJS.ErrnoException).code === "EEXIST")
      throw new Error(
        `actor lock already exists at ${lockPath}; ensure no actor process is running before removing it`,
        {
          cause: error,
        },
      );
    throw error;
  }

  let released = false;
  return async () => {
    if (released) return;
    released = true;
    await unlink(lockPath).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
    });
  };
}
