import type { Address, ChainCursor, ChainEventSource, ChainUpdate, Commitment, ContractFilter, ContractLog, Network } from "./model.js";
import { Commitment as CommitmentValue, Network as NetworkValue } from "./model.js";
import type { Word } from "@lunarbase/math";

export interface NormalizedBackend { snapshotCursor(network: Network): Promise<ChainCursor>; backfill(request: import("./model.js").BackfillRequest): Promise<readonly ContractLog[]>; subscribe(network: Network, filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate>; }
export class NetworkSource implements ChainEventSource {
  constructor(readonly network: Network, private readonly backend: NormalizedBackend) {}
  snapshotCursor(): Promise<ChainCursor> { return this.backend.snapshotCursor(this.network); }
  backfill(request: import("./model.js").BackfillRequest): Promise<readonly ContractLog[]> { return this.backend.backfill(request); }
  subscribe(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> { return this.backend.subscribe(this.network, filter, signal); }
}

export class BaseFlashblocksTracker {
  private payloadId?: string; private blockNumber?: bigint; private latestIndex?: bigint;
  observe(payloadId: string, blockNumber: bigint, index: bigint): boolean { if (this.payloadId === payloadId) { if (this.blockNumber !== blockNumber) throw new Error("Flashblocks payload changed block context"); if (this.latestIndex !== undefined && index < this.latestIndex) throw new Error("Flashblocks index regression"); if (this.latestIndex === index) return false; this.latestIndex = index; return true; } this.payloadId = payloadId; this.blockNumber = blockNumber; this.latestIndex = index; return true; }
  reset(): void { this.payloadId = undefined; this.blockNumber = undefined; this.latestIndex = undefined; }
}
export interface FlashblockHeader { payloadId: string; blockNumber: bigint; blockHash?: string; index: bigint; }
export interface FlashblockLog { header: FlashblockHeader; transactionIndex: bigint; logIndex: bigint; address: Address; topics: readonly Word[]; data: string; removed: boolean; }
export class BaseFlashblocksNormalizer {
  private readonly tracker = new BaseFlashblocksTracker();
  constructor(readonly chainId: bigint) {}
  normalizeHeader(header: FlashblockHeader): ChainUpdate | undefined { if (!this.tracker.observe(header.payloadId, header.blockNumber, header.index)) return undefined; return { kind: "Head", cursor: { chainId: this.chainId, blockNumber: header.blockNumber, blockHash: header.blockHash, sourceSequence: header.index, commitment: CommitmentValue.Realtime } }; }
  normalizeLog(log: FlashblockLog): ChainUpdate[] { const head = this.normalizeHeader(log.header); const updates: ChainUpdate[] = head ? [head] : []; updates.push({ kind: "Log", log: { address: log.address, topics: log.topics, data: log.data, removed: log.removed, cursor: { chainId: this.chainId, blockNumber: log.header.blockNumber, blockHash: log.header.blockHash, transactionIndex: log.transactionIndex, logIndex: log.logIndex, sourceSequence: log.header.index, sourceSubIndex: log.logIndex, commitment: CommitmentValue.Realtime } } }); return updates; }
  reset(): void { this.tracker.reset(); }
}

export class MonadRingTracker {
  private lastSequence?: bigint; private lastSubIndex = -1n;
  observe(sequence: bigint, subIndex = 0n): boolean { if (this.lastSequence === undefined) { this.lastSequence = sequence; this.lastSubIndex = subIndex; return true; } if (sequence === this.lastSequence) { if (subIndex <= this.lastSubIndex) return false; this.lastSubIndex = subIndex; return true; } if (sequence === this.lastSequence + 1n) { this.lastSequence = sequence; this.lastSubIndex = subIndex; return true; } throw new Error("Monad execution-event sequence gap"); }
  observeSparse(sequence: bigint, subIndex = 0n): boolean { if (this.lastSequence === undefined) { this.lastSequence = sequence; this.lastSubIndex = subIndex; return true; } if (sequence < this.lastSequence) throw new Error("Monad execution-event sequence regression"); if (sequence === this.lastSequence && subIndex <= this.lastSubIndex) return false; this.lastSequence = sequence; this.lastSubIndex = subIndex; return true; }
  rewind(): void { this.lastSequence = undefined; this.lastSubIndex = -1n; }
}
export interface MonadExecutionLog { sequence: bigint; sourceSubIndex: bigint; blockNumber: bigint; blockHash?: string; transactionIndex: bigint; logIndex: bigint; address: Address; topics: readonly Word[]; data: string; commitment: Commitment; }
export class MonadExecutionNormalizer {
  private readonly tracker = new MonadRingTracker();
  constructor(readonly chainId: bigint) {}
  normalizeTxnLog(log: MonadExecutionLog): ChainUpdate | undefined { if (!this.tracker.observeSparse(log.sequence, log.sourceSubIndex)) return undefined; return { kind: "Log", log: { address: log.address, topics: log.topics, data: log.data, removed: false, cursor: { chainId: this.chainId, blockNumber: log.blockNumber, blockHash: log.blockHash, transactionIndex: log.transactionIndex, logIndex: log.logIndex, sourceSequence: log.sequence, sourceSubIndex: log.sourceSubIndex, commitment: log.commitment } } }; }
  normalizeHead(head: MonadHead): ChainUpdate { return { kind: "Head", cursor: { chainId: this.chainId, blockNumber: head.blockNumber, blockHash: head.blockHash, sourceSequence: head.sequence, commitment: head.commitment } }; }
  normalizeGap(reason: string): ChainUpdate { this.tracker.rewind(); return { kind: "Gap", reason }; }
}
export interface MonadHead { sequence: bigint; blockNumber: bigint; blockHash?: string; commitment: Commitment; }

export interface ArbitrumExecutionContext { l2BlockNumber: bigint; evmParentBlockNumber: bigint; }
export interface ArbitrumHead { context: ArbitrumExecutionContext; blockHash?: string; commitment: Commitment; }
export class ArbitrumNitroNormalizer {
  constructor(readonly chainId: bigint) {}
  normalizeHead(head: ArbitrumHead): ChainUpdate { return { kind: "Head", cursor: { chainId: this.chainId, blockNumber: head.context.l2BlockNumber, blockHash: head.blockHash, sourceSequence: head.context.evmParentBlockNumber, commitment: head.commitment } }; }
  normalizeLog(log: ContractLog): ChainUpdate { if (log.cursor.chainId !== this.chainId) throw new Error("Arbitrum chain id mismatch"); return { kind: "Log", log }; }
}

export class ProvisionalOverlay {
  private values: Array<readonly [ChainCursor, import("./model.js").QuoteEvent]> = [];
  begin(_baseCursor: ChainCursor): void { this.values = []; }
  push(cursor: ChainCursor, event: import("./model.js").QuoteEvent): void { this.values.push([cursor, event]); }
  updates(): readonly (readonly [ChainCursor, import("./model.js").QuoteEvent])[] { return this.values; }
  verifyCanonical(canonical: readonly (readonly [ChainCursor, import("./model.js").QuoteEvent])[]): void { const stable = (value: unknown) => JSON.stringify(value, (_key, item) => typeof item === "bigint" ? `${item}n` : item); if (stable(this.values) !== stable(canonical)) throw new Error("Flashblocks provisional overlay diverged from canonical logs"); }
  clear(): void { this.values = []; }
}

export class BaseFlashblocksSource extends NetworkSource { constructor(backend: NormalizedBackend) { super(NetworkValue.Base, backend); } }
export class MonadExecutionEventsSource extends NetworkSource { constructor(backend: NormalizedBackend) { super(NetworkValue.Monad, backend); } }
export class ArbitrumNitroSource extends NetworkSource { constructor(backend: NormalizedBackend) { super(NetworkValue.Arbitrum, backend); } }
