import type {
  Address,
  BootstrapSnapshot,
  ChainCursor,
  ChainUpdate,
  DeploymentConfig,
  SnapshotProvider,
} from "./model.js";
import { IndexerError } from "./model.js";

export { type BootstrapSnapshot, type SnapshotProvider } from "./model.js";
/** Validates the core deployment and Redis bounds required for bootstrap. */
export function validateDeploymentConfig(config: DeploymentConfig): void {
  if (config.chainId === 0n || config.httpRpcUrl.length === 0 || config.contractCompatibilityVersion.length === 0)
    throw new Error("invalid deployment configuration");
  if (config.redis.streamMaxLen <= 0 || config.redis.checkpointIntervalUpdates <= 0)
    throw new Error("Redis bounds must be positive");
}
/**
 * Bounded queue used while a realtime subscription is handed off to a
 * block-tagged snapshot. Overflow poisons the queue and requires a resnapshot.
 */
export class BufferedUpdateQueue {
  private readonly values: ChainUpdate[] = [];
  private poisoned = false;
  /** Creates a queue with an explicit memory bound. */
  constructor(readonly capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) throw new Error("buffer capacity must be positive");
  }
  /** Appends one update or poisons the handoff on overflow. */
  push(update: ChainUpdate): void {
    if (this.poisoned || this.values.length >= this.capacity) {
      this.poisoned = true;
      throw new Error("snapshot handoff buffer overflow; resnapshot required");
    }
    this.values.push(update);
  }
  /** Returns whether the queue has observed an unrecoverable overflow. */
  isPoisoned(): boolean {
    return this.poisoned;
  }
  /** Removes all buffered updates; poisoned queues cannot be drained. */
  drain(): ChainUpdate[] {
    if (this.poisoned) throw new Error("snapshot handoff buffer is poisoned");
    return this.values.splice(0);
  }
}
/** Orders handoff updates by cursor position and update kind. */
export function updateOrder(left: ChainUpdate, right: ChainUpdate): number {
  const cursor = (update: ChainUpdate): ChainCursor | undefined =>
    update.kind === "Log"
      ? update.log.cursor
      : update.kind === "Head"
        ? update.cursor
        : update.kind === "Reorg"
          ? update.newHead
          : update.kind === "Gap"
            ? update.cursor
            : undefined;
  const rank = (update: ChainUpdate): number =>
    update.kind === "Head" ? 0 : update.kind === "Log" ? 1 : update.kind === "Reorg" ? 2 : 3;
  const a = cursor(left);
  const b = cursor(right);
  if (!a || !b) return rank(left) - rank(right);
  for (const [x, y] of [
    [a.blockNumber, b.blockNumber],
    [a.transactionIndex ?? 0n, b.transactionIndex ?? 0n],
    [a.logIndex ?? 0n, b.logIndex ?? 0n],
  ] as const) {
    if (x < y) return -1;
    if (x > y) return 1;
  }
  return rank(left) - rank(right);
}
/** Fetches and code-hash-checks a provider snapshot before it becomes ready state. */
export async function fetchBootstrapSnapshot(
  provider: SnapshotProvider,
  config: DeploymentConfig,
  laneAssets: readonly Address[],
  routers: readonly Address[],
  expectedCodeHash: string,
): Promise<BootstrapSnapshot> {
  validateDeploymentConfig(config);
  if (config.expectedRuntimeCodeHash.toLowerCase() !== expectedCodeHash.toLowerCase())
    throw new IndexerError("CODE_HASH_MISMATCH", "deployment code hash mismatch");
  const snapshot = await provider.snapshot(config, laneAssets, routers);
  if (snapshot.runtimeCodeHash.toLowerCase() !== expectedCodeHash.toLowerCase())
    throw new IndexerError("CODE_HASH_MISMATCH", "snapshot code hash mismatch");
  return snapshot;
}
