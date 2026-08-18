/** Ordered single-writer quote-state reducer. */
import {
  BPS,
  quote as computeQuote,
  type Address,
  type FeeClass,
  type LaneState,
  type QuoteOutcome,
  type QuotePolicy,
  type QuoteRequest,
  type QuoteState,
} from "@lunarbase-lab/pmm-v2-math";
import {
  setLaneSlot0BlockDelay,
  setLaneSlot0Exists,
  setLaneSlot0Paused,
  setLaneSlot0PricePushThreshold,
  setLaneSlot0SlippageKBps,
} from "@lunarbase-lab/pmm-v2-math/slot0";
import {
  Commitment,
  commitmentRank,
  IndexerError,
  MATH_COMPATIBILITY_VERSION,
  ReducerError,
  SCHEMA_VERSION,
} from "../model.js";
import type { ChainCursor, Checkpoint, DeploymentConfig, QuoteEvent, VerifiedRouterSnapshot } from "../model.js";
import { quoteEventsEqual } from "./event_identity.js";
import { compareCursor } from "../source.js";
import { restoreReducerUndo, type ReducerUndo } from "./reducer_undo.js";

const U128_MAX = (1n << 128n) - 1n;
const key = (address: Address): Address => address.toLowerCase() as Address;

function emptyLane(): LaneState {
  return {
    slot0: 0n,
    assetReserve: 0n,
    totalPrincipalAmount: 0n,
  };
}

function cloneState(state: QuoteState): QuoteState {
  return {
    cash: state.cash,
    cashReserve: state.cashReserve,
    lanes: new Map([...state.lanes].map(([asset, lane]) => [key(asset), { ...lane }])),
    blacklistFeeMultiplier: state.blacklistFeeMultiplier,
  };
}

function cloneVerifiedRouter(state: VerifiedRouterSnapshot | undefined): VerifiedRouterSnapshot | undefined {
  return state
    ? {
        router: key(state.router),
        partnerFeeBps: new Map([...state.partnerFeeBps].map(([asset, fee]) => [key(asset), fee])),
      }
    : undefined;
}

/** In-memory reducer whose maps never escape the client API. */
export class QuoteReducer {
  /** Complete quote-critical state mutated only by this ordered reducer. */
  private state: QuoteState;
  /** Last normalized head or event position accepted by the reducer. */
  private cursorValue?: ChainCursor;
  /** Last event cursor, isolated from independently arriving head updates. */
  private eventCursorValue?: ChainCursor;
  /** Decoder-owned immutable payload at the last positioned event cursor. */
  private lastEventValue?: QuoteEvent;
  /** Latest accepted head cursor used for execution-block context. */
  private headCursorValue?: ChainCursor;
  /** Fail-closed publication flag cleared by gaps and reducer failures. */
  private ready = false;

  /** Creates a not-ready reducer for one fee class and optional verified router. */
  constructor(
    state: QuoteState,
    private readonly feeClass: FeeClass,
    private readonly verifiedRouterState?: VerifiedRouterSnapshot,
  ) {
    this.state = cloneState(state);
    this.verifiedRouterState = cloneVerifiedRouter(verifiedRouterState);
  }

  /** Restores a prevalidated checkpoint. */
  static fromCheckpoint(checkpoint: Checkpoint, feeClass: FeeClass): QuoteReducer {
    const reducer = new QuoteReducer(checkpoint.state, feeClass);
    reducer.bootstrap(checkpoint.cursor);
    return reducer;
  }

  /** Copies mutable state only for a rare correction candidate. */
  fork(): QuoteReducer {
    const reducer = new QuoteReducer(this.state, this.feeClass, this.verifiedRouterState);
    reducer.cursorValue = this.cursorValue ? { ...this.cursorValue } : undefined;
    reducer.eventCursorValue = this.eventCursorValue ? { ...this.eventCursorValue } : undefined;
    reducer.headCursorValue = this.headCursorValue ? { ...this.headCursorValue } : undefined;
    reducer.lastEventValue = this.lastEventValue;
    reducer.ready = this.ready;
    return reducer;
  }

  /** Returns the last accepted cursor. */
  cursor(): ChainCursor | undefined {
    return this.cursorValue ? { ...this.cursorValue } : undefined;
  }

  /** Reports whether quotes may use current state. */
  isReady(): boolean {
    return this.ready;
  }

  /** Revokes readiness until a complete canonical recovery succeeds. */
  markNotReady(): void {
    this.ready = false;
  }

  /** Publishes recovered state. */
  publishReady(): void {
    this.ready = true;
  }

  /** Installs the initial block-tagged cursor. */
  bootstrap(cursor: ChainCursor): void {
    this.cursorValue = { ...cursor };
    this.eventCursorValue = { ...cursor };
    this.headCursorValue = { ...cursor };
    this.lastEventValue = undefined;
    this.ready = true;
  }

  /** Advances head metadata without changing quote state. */
  observeHead(head: ChainCursor): void {
    const event = this.eventCursorValue;
    if (event?.blockNumber === head.blockNumber) this.validateChainAndHash(event, head);
    const current = this.headCursorValue;
    if (!current) {
      this.headCursorValue = { ...head };
      this.cursorValue = { ...head };
      return;
    }
    this.validateChainAndHash(current, head, true);
    if (head.blockNumber < current.blockNumber) return;
    if (head.blockNumber > current.blockNumber) {
      this.headCursorValue = { ...head };
      this.cursorValue = { ...head };
      return;
    }
    const progression = isRealtimeProgression(current, head);
    const next = {
      ...current,
      executionBlockNumber: head.executionBlockNumber,
      blockHash: current.blockHash === undefined || progression ? head.blockHash : current.blockHash,
      commitment:
        commitmentRank(head.commitment) > commitmentRank(current.commitment) ? head.commitment : current.commitment,
      sourceSequence:
        (head.sourceSequence ?? 0n) > (current.sourceSequence ?? 0n) ? head.sourceSequence : current.sourceSequence,
      sourceSubIndex:
        (head.sourceSequence ?? 0n) > (current.sourceSequence ?? 0n) ? head.sourceSubIndex : current.sourceSubIndex,
    };
    this.headCursorValue = next;
    const published = this.cursorValue;
    if (!published || published.blockNumber <= head.blockNumber) this.cursorValue = { ...next };
  }

  /** Applies one already-ordered quote-critical event. */
  apply(cursor: ChainCursor, event: QuoteEvent): ReducerUndo | undefined {
    const previous = this.eventCursorValue;
    const head = this.headCursorValue;
    if (head?.blockNumber === cursor.blockNumber) this.validateChainAndHash(head, cursor);
    if (previous) {
      this.validateChainAndHash(previous, cursor);
      const previousIsBlockBoundary =
        previous.blockNumber === cursor.blockNumber &&
        previous.transactionIndex === undefined &&
        previous.logIndex === undefined &&
        cursor.transactionIndex !== undefined &&
        cursor.logIndex !== undefined;
      if (!previousIsBlockBoundary) {
        const order = compareCursor(cursor, previous);
        if (order < 0) throw new ReducerError("CURSOR_REGRESSION", "event cursor regression");
        if (order === 0) {
          if (this.lastEventValue && quoteEventsEqual(this.lastEventValue, event)) return undefined;
          throw new ReducerError("DUPLICATE_EVENT_CONFLICT", "conflicting event payload at the same cursor");
        }
      }
    }
    this.validateEvent(event);
    const undo = this.applyEvent(event);
    this.eventCursorValue = { ...cursor };
    this.lastEventValue = event;
    const published = this.cursorValue;
    if (!published || compareCursor(cursor, published) >= 0) {
      const sameBlockHead = head?.blockNumber === cursor.blockNumber ? head : undefined;
      const executionBlockNumber = sameBlockHead?.executionBlockNumber ?? cursor.executionBlockNumber;
      let commitment = cursor.commitment;
      if (sameBlockHead && commitmentRank(sameBlockHead.commitment) > commitmentRank(commitment))
        commitment = sameBlockHead.commitment;
      if (
        published?.blockNumber === cursor.blockNumber &&
        commitmentRank(published.commitment) > commitmentRank(commitment)
      )
        commitment = published.commitment;
      this.cursorValue = {
        ...cursor,
        executionBlockNumber,
        commitment,
      };
    } else if ((cursor.sourceSequence ?? 0n) > (published.sourceSequence ?? 0n)) {
      this.cursorValue = {
        ...published,
        sourceSequence: cursor.sourceSequence,
        sourceSubIndex: cursor.sourceSubIndex,
      };
    }
    return undo;
  }

  /** Restores one compact state before-image on a private correction candidate. */
  revert(undo: ReducerUndo): void {
    restoreReducerUndo(this.state, this.verifiedRouterState, undo);
  }

  /** Resets ordering metadata after all post-ancestor state undos were applied. */
  prepareCorrection(commonAncestor: ChainCursor): void {
    const current = this.cursorValue;
    if (current && current.chainId !== commonAncestor.chainId)
      throw new ReducerError("CHAIN_ID_MISMATCH", "correction ancestor chain id mismatch");
    this.cursorValue = { ...commonAncestor };
    this.eventCursorValue = { ...commonAncestor };
    this.lastEventValue = undefined;
    this.headCursorValue = { ...commonAncestor };
  }

  /** Computes one quote synchronously from the current event-loop snapshot. */
  quote(request: QuoteRequest): QuoteOutcome {
    const cursor = this.requireReadyCursor();
    return computeQuote(request, cursor.executionBlockNumber, this.state, this.policyFor(request));
  }

  /** Computes a batch without yielding, so every result uses one cursor/state. */
  quoteMany(requests: readonly QuoteRequest[]): readonly QuoteOutcome[] {
    const cursor = this.requireReadyCursor();
    return requests.map((request) =>
      computeQuote(request, cursor.executionBlockNumber, this.state, this.policyFor(request)),
    );
  }

  /** Returns the optional execution caller whose allocation is verified. */
  verifiedRouter(): Address | undefined {
    return this.verifiedRouterState?.router;
  }

  /** Creates a deep-cloned restart checkpoint outside the quote path. */
  checkpoint(config: DeploymentConfig): Checkpoint | undefined {
    if (!this.cursorValue) return undefined;
    return {
      schemaVersion: SCHEMA_VERSION,
      mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
      expectedImplementation: config.expectedImplementation,
      expectedImplementationCodeHash: config.expectedImplementationCodeHash,
      chainId: config.chainId,
      network: config.network,
      core: config.core,
      deploymentBlock: config.deploymentBlock,
      explicitLaneAssets: [...config.explicitLaneAssets],
      cursor: { ...this.cursorValue },
      state: cloneState(this.state),
    };
  }

  private requireReadyCursor(): ChainCursor {
    if (!this.ready) throw new IndexerError("NOT_READY", "indexer is not ready");
    if (!this.cursorValue) throw new IndexerError("NO_CURSOR", "indexer has no cursor");
    return this.cursorValue;
  }

  private validateChainAndHash(previous: ChainCursor, next: ChainCursor, allowRealtimeProgression = false): void {
    if (previous.chainId !== next.chainId) throw new ReducerError("CHAIN_ID_MISMATCH", "cursor chain id mismatch");
    if (previous.blockNumber !== next.blockNumber) return;
    const oneHashMissing = (previous.blockHash === undefined) !== (next.blockHash === undefined);
    const hashesConflict =
      oneHashMissing ||
      (previous.blockHash !== undefined &&
        next.blockHash !== undefined &&
        previous.blockHash.toLowerCase() !== next.blockHash.toLowerCase());
    const executionBlockConflict = !hashesConflict && previous.executionBlockNumber !== next.executionBlockNumber;
    if (
      executionBlockConflict ||
      (hashesConflict && !(allowRealtimeProgression && isRealtimeProgression(previous, next)))
    )
      throw new ReducerError("BLOCK_HASH_MISMATCH", "block hash mismatch");
  }

  private validateEvent(event: QuoteEvent): void {
    if (
      event.kind === "SlippageKSet" &&
      (!Number.isSafeInteger(event.newK) || event.newK < 0 || BigInt(event.newK) > BPS)
    )
      throw new ReducerError("INVALID_SLIPPAGE_K", "slippage K exceeds BPS");
    if (
      event.kind === "PricePushThresholdSet" &&
      (!Number.isSafeInteger(event.pricePushThreshold) ||
        event.pricePushThreshold < 0 ||
        event.pricePushThreshold > 0x7f)
    )
      throw new ReducerError("INVALID_WIDTH", "price-push threshold does not fit uint7");
    if (
      (event.kind === "PartnerInfoSet" || event.kind === "PartnerFeeSet") &&
      this.verifiedRouterState !== undefined &&
      key(event.router) === key(this.verifiedRouterState.router) &&
      (!Number.isSafeInteger(event.fee) || event.fee < 0 || BigInt(event.fee) > BPS)
    )
      throw new ReducerError("INVALID_WIDTH", "partner fee exceeds BPS");
    if (
      (event.kind === "DepositExecuted" || event.kind === "WithdrawalExecuted") &&
      (event.principal < 0n || event.principal > U128_MAX)
    )
      throw new ReducerError("INVALID_WIDTH", "principal does not fit uint128");
  }

  private applyEvent(event: QuoteEvent): ReducerUndo | undefined {
    switch (event.kind) {
      case "LaneAdded": {
        if (this.verifiedRouterState)
          throw new ReducerError(
            "VERIFIED_ROUTER_REFRESH_REQUIRED",
            "verified router allocation requires a snapshot refresh",
          );
        const asset = key(event.asset);
        const previous = this.state.lanes.get(asset);
        const lane = this.lane(event.asset);
        const slot0 = setLaneSlot0Exists(lane.slot0, true);
        lane.slot0 = setLaneSlot0Paused(slot0, true);
        this.setLane(event.asset, lane);
        return { kind: "Lane", asset, previous };
      }
      case "LaneRemoved": {
        const asset = key(event.asset);
        const previousLane = this.state.lanes.get(asset);
        const fees = this.verifiedRouterState?.partnerFeeBps;
        const hadPartnerFee = fees?.has(asset) ?? false;
        const previousPartnerFee = fees?.get(asset);
        (this.state.lanes as Map<Address, LaneState>).delete(asset);
        if (this.verifiedRouterState) (this.verifiedRouterState.partnerFeeBps as Map<Address, number>).delete(asset);
        return { kind: "LaneAndPartner", asset, previousLane, hadPartnerFee, previousPartnerFee };
      }
      case "LaneUpdated": {
        const asset = key(event.asset);
        const lane = this.existingLane(event.asset);
        const previous = this.state.lanes.get(asset)!;
        lane.slot0 = event.slot0;
        this.setLane(event.asset, lane);
        return { kind: "Lane", asset, previous };
      }
      case "SlippageKSet": {
        const asset = key(event.asset);
        const lane = this.existingLane(event.asset);
        const previous = this.state.lanes.get(asset)!;
        lane.slot0 = setLaneSlot0SlippageKBps(lane.slot0, event.newK);
        this.setLane(event.asset, lane);
        return { kind: "Lane", asset, previous };
      }
      case "LanePausedSet": {
        const asset = key(event.asset);
        const lane = this.existingLane(event.asset);
        const previous = this.state.lanes.get(asset)!;
        lane.slot0 = setLaneSlot0Paused(lane.slot0, event.paused);
        this.setLane(event.asset, lane);
        return { kind: "Lane", asset, previous };
      }
      case "PricePushThresholdSet": {
        const asset = key(event.asset);
        const lane = this.existingLane(event.asset);
        const previous = this.state.lanes.get(asset)!;
        lane.slot0 = setLaneSlot0PricePushThreshold(lane.slot0, event.pricePushThreshold, event.enabled);
        this.setLane(event.asset, lane);
        return { kind: "Lane", asset, previous };
      }
      case "BlockDelaySet": {
        const asset = key(event.asset);
        const lane = this.existingLane(event.asset);
        const previous = this.state.lanes.get(asset)!;
        lane.slot0 = setLaneSlot0BlockDelay(lane.slot0, event.blockDelay);
        this.setLane(event.asset, lane);
        return { kind: "Lane", asset, previous };
      }
      case "PartnerInfoSet":
      case "PartnerFeeSet":
        if (
          this.verifiedRouterState &&
          key(event.router) === key(this.verifiedRouterState.router) &&
          (key(event.asset) === key(this.state.cash) || this.state.lanes.has(key(event.asset)))
        ) {
          const asset = key(event.asset);
          const fees = this.verifiedRouterState.partnerFeeBps as Map<Address, number>;
          const hadValue = fees.has(asset);
          const previous = fees.get(asset);
          fees.set(asset, event.fee);
          return { kind: "PartnerFee", asset, hadValue, previous };
        }
        return undefined;
      case "WhitelistSet":
        if (
          this.verifiedRouterState &&
          key(event.router) === key(this.verifiedRouterState.router) &&
          event.whitelisted !== (this.feeClass === "Whitelisted")
        )
          throw new ReducerError("FEE_CLASS_MISMATCH", "verified router fee class changed");
        return undefined;
      case "BlacklistFeeMultiplierSet": {
        const previous = this.state.blacklistFeeMultiplier;
        this.state.blacklistFeeMultiplier = event.multiplier;
        return { kind: "BlacklistMultiplier", previous };
      }
      case "DepositExecuted": {
        const asset = key(event.asset);
        const lane = this.existingLane(event.asset);
        const previous = this.state.lanes.get(asset)!;
        const next = lane.totalPrincipalAmount + event.principal;
        if (next > U128_MAX) throw new ReducerError("ARITHMETIC", "principal storage overflow");
        lane.totalPrincipalAmount = next;
        this.setLane(event.asset, lane);
        return { kind: "Lane", asset, previous };
      }
      case "WithdrawalExecuted": {
        const asset = key(event.asset);
        const lane = this.existingLane(event.asset);
        const previous = this.state.lanes.get(asset)!;
        if (event.principal > lane.totalPrincipalAmount) throw new ReducerError("ARITHMETIC", "principal underflow");
        lane.totalPrincipalAmount -= event.principal;
        this.setLane(event.asset, lane);
        return { kind: "Lane", asset, previous };
      }
      case "Sync": {
        const previousCashReserve = this.state.cashReserve;
        let asset: Address | undefined;
        let previousLane: LaneState | undefined;
        let lane: LaneState | undefined;
        if (key(event.asset) !== key(this.state.cash)) {
          asset = key(event.asset);
          lane = this.existingLane(asset);
          previousLane = this.state.lanes.get(asset)!;
        }
        this.state.cashReserve = event.cashReserve;
        if (lane && asset) {
          lane.assetReserve = event.assetReserve;
          this.setLane(asset, lane);
        }
        return { kind: "CashAndLane", asset, previousCashReserve, previousLane };
      }
      case "ImplementationUpgraded":
        throw new ReducerError("IMPLEMENTATION_UPGRADED", "Core implementation upgraded");
    }
  }

  private lane(asset: Address): LaneState {
    return { ...(this.state.lanes.get(key(asset)) ?? emptyLane()) };
  }

  private existingLane(asset: Address): LaneState {
    const lane = this.state.lanes.get(key(asset));
    if (lane === undefined) throw new ReducerError("UNKNOWN_LANE", "event references an unknown lane");
    return { ...lane };
  }

  private setLane(asset: Address, lane: LaneState): void {
    (this.state.lanes as Map<Address, LaneState>).set(key(asset), lane);
  }

  private policyFor(request: QuoteRequest): QuotePolicy {
    if (!this.verifiedRouterState) return { feeClass: this.feeClass };
    const feeAsset = request.mode === "ExactIn" ? request.assetOut : request.assetIn;
    return {
      feeClass: this.feeClass,
      verifiedPartnerFeeBps: this.verifiedRouterState.partnerFeeBps.get(key(feeAsset)) ?? 0,
    };
  }
}

function isRealtimeProgression(previous: ChainCursor, next: ChainCursor): boolean {
  return (
    previous.commitment === Commitment.Realtime &&
    next.commitment === Commitment.Realtime &&
    (next.sourceSequence ?? 0n) > (previous.sourceSequence ?? 0n)
  );
}
