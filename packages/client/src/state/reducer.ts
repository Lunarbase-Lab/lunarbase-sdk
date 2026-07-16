import { BPS, type LaneState, type QuoteState } from "@lunarbase/math";
import type { Address, ChainCursor, Checkpoint, QuoteEvent } from "../model.js";
import { commitmentRank, ReducerError, SCHEMA_VERSION, MATH_COMPATIBILITY_VERSION } from "../model.js";

const U64_MAX = (1n << 64n) - 1n;
function cursorOrder(cursor: ChainCursor): readonly [bigint, bigint, bigint] {
  return [cursor.blockNumber, cursor.transactionIndex ?? 0n, cursor.logIndex ?? 0n];
}
function compareCursor(a: ChainCursor, b: ChainCursor): number {
  const left = cursorOrder(a);
  const right = cursorOrder(b);
  for (let i = 0; i < left.length; i += 1) {
    if (left[i] < right[i]) return -1;
    if (left[i] > right[i]) return 1;
  }
  return 0;
}
function feeKey(router: Address, asset: Address): string {
  return `${router.toLowerCase()}:${asset.toLowerCase()}`;
}
function emptyLane(): LaneState {
  return { slot0: 0n, exists: false, paused: false, blockDelay: 0n, slippageKBps: 0n };
}
function cloneLane(lane: LaneState): LaneState {
  return { ...lane };
}
type MutableQuoteState = Omit<QuoteState, "lanes" | "totalPrincipalAmount" | "whitelist" | "partnerFeeBps"> & {
  lanes: Map<Address, LaneState>;
  totalPrincipalAmount: Map<Address, bigint>;
  whitelist: Map<Address, boolean>;
  partnerFeeBps: Map<string, bigint>;
};
function cloneState(state: QuoteState): MutableQuoteState {
  return {
    cash: state.cash,
    lanes: new Map([...state.lanes].map(([key, lane]) => [key, cloneLane(lane)])),
    totalPrincipalAmount: new Map(state.totalPrincipalAmount),
    whitelist: new Map(state.whitelist),
    blacklistFeeMultiplier: state.blacklistFeeMultiplier,
    partnerFeeBps: new Map(state.partnerFeeBps),
    stateVersion: state.stateVersion,
  };
}

export class QuoteReducer {
  private currentState: MutableQuoteState;
  private lastCursor?: ChainCursor;
  private ready = false;
  /** Creates a single-writer reducer from a mutable clone of a quote snapshot. */
  constructor(state: QuoteState) {
    this.currentState = cloneState(state);
  }
  /** Restores a ready reducer from a compatibility-checked checkpoint. */
  static fromCheckpoint(checkpoint: Checkpoint): QuoteReducer {
    const reducer = new QuoteReducer(checkpoint.state);
    reducer.lastCursor = checkpoint.cursor;
    reducer.ready = true;
    return reducer;
  }
  /** Returns a defensive immutable-state clone. */
  state(): QuoteState {
    return cloneState(this.currentState);
  }
  /** Returns the last accepted cursor, if bootstrapped. */
  cursor(): ChainCursor | undefined {
    return this.lastCursor;
  }
  /** Returns whether the reducer may serve fresh quotes. */
  isReady(): boolean {
    return this.ready;
  }
  /** Revokes readiness after a gap, reorg, or persistence failure. */
  markNotReady(): void {
    this.ready = false;
  }
  /** Publishes the current state as ready after a complete handoff/recovery. */
  publishReady(): void {
    this.ready = true;
  }
  /** Installs the initial cursor and marks the reducer ready. */
  bootstrap(cursor: ChainCursor): void {
    this.lastCursor = cursor;
    this.ready = true;
  }
  /** Advances the cursor from a head while checking chain/hash consistency. */
  observeHead(head: ChainCursor): void {
    if (!this.lastCursor) {
      this.lastCursor = head;
      return;
    }
    if (this.lastCursor.chainId !== head.chainId)
      throw new ReducerError("CHAIN_ID_MISMATCH", "cursor chain id mismatch");
    if (
      this.lastCursor.blockNumber === head.blockNumber &&
      this.lastCursor.blockHash &&
      head.blockHash &&
      this.lastCursor.blockHash.toLowerCase() !== head.blockHash.toLowerCase()
    )
      throw new ReducerError("BLOCK_HASH_MISMATCH", "block hash mismatch");
    if (head.blockNumber < this.lastCursor.blockNumber) return;
    if (head.blockNumber === this.lastCursor.blockNumber) {
      this.lastCursor = {
        ...this.lastCursor,
        blockHash: this.lastCursor.blockHash ?? head.blockHash,
        commitment:
          commitmentRank(head.commitment) > commitmentRank(this.lastCursor.commitment)
            ? head.commitment
            : this.lastCursor.commitment,
      };
      return;
    }
    this.lastCursor = head;
  }
  /** Applies one ordered event transactionally, rolling back on validation failure. */
  apply(cursor: ChainCursor, event: QuoteEvent): void {
    if (this.lastCursor) {
      if (this.lastCursor.chainId !== cursor.chainId)
        throw new ReducerError("CHAIN_ID_MISMATCH", "cursor chain id mismatch");
      if (
        this.lastCursor.blockNumber === cursor.blockNumber &&
        this.lastCursor.blockHash &&
        cursor.blockHash &&
        this.lastCursor.blockHash.toLowerCase() !== cursor.blockHash.toLowerCase()
      )
        throw new ReducerError("BLOCK_HASH_MISMATCH", "block hash mismatch");
      const blockHeadToEvent =
        this.lastCursor.blockNumber === cursor.blockNumber &&
        this.lastCursor.transactionIndex === undefined &&
        this.lastCursor.logIndex === undefined &&
        cursor.transactionIndex !== undefined &&
        cursor.logIndex !== undefined;
      const order = compareCursor(cursor, this.lastCursor);
      if (!blockHeadToEvent && order < 0) throw new ReducerError("CURSOR_REGRESSION", "cursor regression");
      if (!blockHeadToEvent && order === 0) return;
    }
    const previousState = cloneState(this.currentState);
    const previousCursor = this.lastCursor;
    try {
      this.applyEvent(event);
      this.currentState.stateVersion += 1n;
      if (this.currentState.stateVersion > U64_MAX) throw new ReducerError("ARITHMETIC", "state version overflow");
      this.lastCursor = cursor;
    } catch (error) {
      this.currentState = previousState;
      this.lastCursor = previousCursor;
      throw error;
    }
  }
  private applyEvent(event: QuoteEvent): void {
    switch (event.kind) {
      case "LaneAdded": {
        const lane = this.currentState.lanes.get(event.asset) ?? emptyLane();
        this.currentState.lanes.set(event.asset, { ...lane, exists: true });
        break;
      }
      case "LaneRemoved":
        this.currentState.lanes.delete(event.asset);
        break;
      case "LaneUpdated": {
        const lane = this.currentState.lanes.get(event.asset) ?? emptyLane();
        this.currentState.lanes.set(event.asset, { ...lane, slot0: event.slot0 });
        break;
      }
      case "SlippageKSet": {
        if (event.newK > BPS) throw new ReducerError("INVALID_SLIPPAGE_K", "slippage K exceeds BPS");
        const lane = this.currentState.lanes.get(event.asset) ?? emptyLane();
        this.currentState.lanes.set(event.asset, { ...lane, slippageKBps: event.newK });
        break;
      }
      case "PartnerInfoSet":
      case "PartnerFeeSet":
        if (event.fee > BPS) throw new ReducerError("INVALID_WIDTH", "partner fee exceeds BPS");
        this.currentState.partnerFeeBps.set(feeKey(event.router, event.asset), event.fee);
        break;
      case "WhitelistSet":
        this.currentState.whitelist.set(event.router, event.whitelisted);
        break;
      case "BlacklistFeeMultiplierSet":
        this.currentState.blacklistFeeMultiplier = event.multiplier;
        break;
      case "DepositExecuted": {
        const current = this.currentState.totalPrincipalAmount.get(event.asset) ?? 0n;
        if (event.principal > (1n << 128n) - 1n)
          throw new ReducerError("INVALID_WIDTH", "principal does not fit uint128");
        const next = current + event.principal;
        if (next > (1n << 128n) - 1n) throw new ReducerError("INVALID_WIDTH", "principal storage overflow");
        this.currentState.totalPrincipalAmount.set(event.asset, next);
        break;
      }
      case "WithdrawalExecuted": {
        const current = this.currentState.totalPrincipalAmount.get(event.asset) ?? 0n;
        if (event.principal > (1n << 128n) - 1n)
          throw new ReducerError("INVALID_WIDTH", "principal does not fit uint128");
        if (event.principal > current) throw new ReducerError("ARITHMETIC", "principal underflow");
        this.currentState.totalPrincipalAmount.set(event.asset, current - event.principal);
        break;
      }
      case "SwapExecuted":
        break;
    }
  }
  /** Serializes the current state and cursor into a durable checkpoint. */
  checkpoint(codeHash: string): Checkpoint | undefined {
    return this.lastCursor
      ? {
          schemaVersion: SCHEMA_VERSION,
          mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
          expectedRuntimeCodeHash: codeHash,
          cursor: this.lastCursor,
          state: this.state(),
        }
      : undefined;
  }
}
