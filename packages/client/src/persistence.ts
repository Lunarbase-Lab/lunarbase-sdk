import type { Checkpoint, CheckpointStore, ChainUpdate, RedisAtomicCommand } from "./model.js";
import { MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION } from "./model.js";
import type { Address } from "@lunarbase/math";
import { decodeCheckpoint, decodeUpdate, encodeCheckpoint, encodeUpdate } from "./protocol/codec.js";

/** Redis key set namespaced by chain id and Core address. */
export interface RedisNamespace {
  tag: string;
  meta: string;
  state: string;
  checkpoint: string;
  updates: string;
  writerLease: string;
}
/** Builds a collision-resistant Redis namespace for one deployment. */
export function redisNamespace(chainId: bigint, core: Address): RedisNamespace {
  const tag = `${chainId.toString()}:${core.toLowerCase()}`;
  return {
    tag,
    meta: `lb:{${tag}}:meta`,
    state: `lb:{${tag}}:state`,
    checkpoint: `lb:{${tag}}:checkpoint`,
    updates: `lb:{${tag}}:updates`,
    writerLease: `lb:{${tag}}:writer-lease`,
  };
}

/** Deterministic bounded checkpoint store for tests and embedded callers. */
export class InMemoryRedisStore implements CheckpointStore {
  private checkpointValue?: Checkpoint;
  private readonly updateValues: ChainUpdate[] = [];
  private readonly dedupValues = new Set<string>();
  /** Creates an in-memory store with a bounded update history. */
  constructor(private readonly maxUpdates: number) {
    if (!Number.isSafeInteger(maxUpdates) || maxUpdates <= 0) throw new Error("maxUpdates must be positive");
  }
  /** Returns the latest checkpoint, if one has been committed. */
  load(): Checkpoint | undefined {
    return this.checkpointValue;
  }
  /** Atomically records a checkpoint and deduplicated update batch. */
  commit(checkpoint: Checkpoint, updates: readonly ChainUpdate[]): void {
    this.checkpointValue = checkpoint;
    for (const update of updates) {
      const identity = updateIdentity(update);
      if (this.dedupValues.has(identity)) continue;
      this.dedupValues.add(identity);
      this.updateValues.push(update);
      while (this.updateValues.length > this.maxUpdates) this.updateValues.shift();
    }
  }
  /** Returns a defensive copy of retained updates in stream order. */
  updates(): readonly ChainUpdate[] {
    return [...this.updateValues];
  }
}

/** Minimal asynchronous Redis transport required by `RedisCheckpointStore`. */
export interface RedisCheckpointTransport {
  get(key: string): Promise<Uint8Array | undefined>;
  hGetAll(key: string): Promise<Readonly<Record<string, string>>>;
  ping(): Promise<void>;
  atomic(commands: readonly RedisAtomicCommand[]): Promise<void>;
  xRange(key: string): Promise<readonly Uint8Array[]>;
  setNxEx(key: string, value: string, ttlSeconds: bigint): Promise<boolean>;
  compareAndDelete(key: string, value: string): Promise<boolean>;
}
/** Durable checkpoint store using one atomic provider command batch per commit. */
export class RedisCheckpointStore {
  /** Creates a Redis store with bounded stream length and deduplication TTL. */
  constructor(
    readonly transport: RedisCheckpointTransport,
    readonly namespace: RedisNamespace,
    readonly maxUpdates: number,
    readonly dedupTtlSeconds = 86_400n,
  ) {
    if (!Number.isSafeInteger(maxUpdates) || maxUpdates <= 0 || dedupTtlSeconds <= 0n)
      throw new Error("Redis bounds must be positive");
  }
  /** Loads and decodes the durable checkpoint. */
  async load(): Promise<Checkpoint | undefined> {
    const bytes = await this.transport.get(this.namespace.checkpoint);
    return bytes ? decodeCheckpoint(bytes) : undefined;
  }
  /** Persists checkpoint, metadata, deduplicated updates, and stream trim atomically. */
  async commit(checkpoint: Checkpoint, updates: readonly ChainUpdate[]): Promise<void> {
    const bytes = encodeCheckpoint(checkpoint);
    const commands: RedisAtomicCommand[] = [
      { kind: "set", key: this.namespace.checkpoint, value: bytes },
      { kind: "set", key: this.namespace.state, value: bytes },
      {
        kind: "hset",
        key: this.namespace.meta,
        fields: {
          schema_version: checkpoint.schemaVersion.toString(),
          math_compatibility_version: checkpoint.mathCompatibilityVersion,
          expected_runtime_code_hash: checkpoint.expectedRuntimeCodeHash.toLowerCase(),
        },
      },
      ...updates.map((update) => ({
        kind: "xaddIfNew" as const,
        key: this.namespace.updates,
        dedupKey: `${this.namespace.updates}:dedup:${updateIdentity(update)}`,
        dedupTtlSeconds: this.dedupTtlSeconds,
        value: encodeUpdate(update),
      })),
      { kind: "xtrim", key: this.namespace.updates, maxLen: this.maxUpdates },
    ];
    await this.transport.atomic(commands);
  }
  /** Reads and decodes all retained updates in stream order. */
  async updates(): Promise<readonly ChainUpdate[]> {
    return (await this.transport.xRange(this.namespace.updates)).map((value) => decodeUpdate(value));
  }
  /** Acquires the single-writer lease with NX/EX semantics. */
  acquireWriterLease(owner: string, ttlSeconds: bigint): Promise<boolean> {
    if (ttlSeconds <= 0n) throw new Error("lease TTL must be positive");
    return this.transport.setNxEx(this.namespace.writerLease, owner, ttlSeconds);
  }
  /** Releases the lease only if it still belongs to `owner`. */
  releaseWriterLease(owner: string): Promise<boolean> {
    return this.transport.compareAndDelete(this.namespace.writerLease, owner);
  }
  /** Verifies schema, math version, and runtime code hash metadata. */
  async validateMeta(
    expectedRuntimeCodeHash: string,
    mathCompatibilityVersion = MATH_COMPATIBILITY_VERSION,
  ): Promise<boolean> {
    const fields = await this.transport.hGetAll(this.namespace.meta);
    return (
      fields.schema_version === SCHEMA_VERSION.toString() &&
      fields.math_compatibility_version === mathCompatibilityVersion &&
      fields.expected_runtime_code_hash?.toLowerCase() === expectedRuntimeCodeHash.toLowerCase()
    );
  }
  /** Performs a provider-level connectivity check. */
  health(): Promise<void> {
    return this.transport.ping();
  }
}

function updateIdentity(update: ChainUpdate): string {
  if (update.kind === "SourceHealth") return `health:${update.healthy}`;
  const cursor = update.kind === "Log" ? update.log.cursor : update.kind === "Reorg" ? update.newHead : update.cursor;
  const kind = update.kind;
  return `${kind}:${cursor?.blockNumber.toString() ?? "none"}:${cursor?.transactionIndex?.toString() ?? "none"}:${cursor?.logIndex?.toString() ?? "none"}:${cursor?.sourceSequence?.toString() ?? "none"}`;
}
