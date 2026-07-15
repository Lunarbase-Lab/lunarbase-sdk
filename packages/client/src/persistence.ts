import type { Checkpoint, CheckpointStore, ChainUpdate, RedisConfig, RedisAtomicCommand } from "./model.js";
import type { Address } from "@lunarbase/math";
import { decodeCheckpoint, decodeUpdate, encodeCheckpoint, encodeUpdate } from "./codec.js";

export interface RedisNamespace { tag: string; meta: string; state: string; checkpoint: string; updates: string; writerLease: string; }
export function redisNamespace(chainId: bigint, core: Address): RedisNamespace { const tag = `${chainId.toString()}:${core.toLowerCase()}`; return { tag, meta: `lb:{${tag}}:meta`, state: `lb:{${tag}}:state`, checkpoint: `lb:{${tag}}:checkpoint`, updates: `lb:{${tag}}:updates`, writerLease: `lb:{${tag}}:writer-lease` }; }

export class InMemoryRedisStore implements CheckpointStore {
  private checkpointValue?: Checkpoint; private readonly updateValues: ChainUpdate[] = [];
  constructor(private readonly maxUpdates: number) { if (!Number.isSafeInteger(maxUpdates) || maxUpdates <= 0) throw new Error("maxUpdates must be positive"); }
  load(): Checkpoint | undefined { return this.checkpointValue; }
  commit(checkpoint: Checkpoint, updates: readonly ChainUpdate[]): void { this.checkpointValue = checkpoint; this.updateValues.push(...updates); while (this.updateValues.length > this.maxUpdates) this.updateValues.shift(); }
  updates(): readonly ChainUpdate[] { return [...this.updateValues]; }
}

export interface RedisCheckpointTransport { get(key: string): Promise<Uint8Array | undefined>; atomic(commands: readonly RedisAtomicCommand[]): Promise<void>; xRange(key: string): Promise<readonly Uint8Array[]>; setNxEx(key: string, value: string, ttlSeconds: bigint): Promise<boolean>; compareAndDelete(key: string, value: string): Promise<boolean>; }
export class RedisCheckpointStore {
  constructor(readonly transport: RedisCheckpointTransport, readonly namespace: RedisNamespace, readonly maxUpdates: number) { if (!Number.isSafeInteger(maxUpdates) || maxUpdates <= 0) throw new Error("maxUpdates must be positive"); }
  async load(): Promise<Checkpoint | undefined> { const bytes = await this.transport.get(this.namespace.checkpoint); return bytes ? decodeCheckpoint(bytes) : undefined; }
  async commit(checkpoint: Checkpoint, updates: readonly ChainUpdate[]): Promise<void> { const bytes = encodeCheckpoint(checkpoint); const commands: RedisAtomicCommand[] = [{ kind: "set", key: this.namespace.checkpoint, value: bytes }, { kind: "set", key: this.namespace.state, value: bytes }, { kind: "hset", key: this.namespace.meta, fields: { schema_version: checkpoint.schemaVersion.toString(), math_compatibility_version: checkpoint.mathCompatibilityVersion, expected_runtime_code_hash: checkpoint.expectedRuntimeCodeHash.toLowerCase() } }, ...updates.map((update) => ({ kind: "xadd" as const, key: this.namespace.updates, value: encodeUpdate(update) })), { kind: "xtrim", key: this.namespace.updates, maxLen: this.maxUpdates }]; await this.transport.atomic(commands); }
  async updates(): Promise<readonly ChainUpdate[]> { return (await this.transport.xRange(this.namespace.updates)).map((value) => decodeUpdate(value)); }
  acquireWriterLease(owner: string, ttlSeconds: bigint): Promise<boolean> { if (ttlSeconds <= 0n) throw new Error("lease TTL must be positive"); return this.transport.setNxEx(this.namespace.writerLease, owner, ttlSeconds); }
  releaseWriterLease(owner: string): Promise<boolean> { return this.transport.compareAndDelete(this.namespace.writerLease, owner); }
}
