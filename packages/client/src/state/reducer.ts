/** Ordered single-writer quote-state reducer. */
import {
  BPS,
  quote as computeQuote,
  setLaneSlot0BlockDelay,
  setLaneSlot0Exists,
  setLaneSlot0Paused,
  setLaneSlot0PricePushThreshold,
  setLaneSlot0SlippageKBps,
  type Address,
  type LaneState,
  type QuoteOutcome,
  type QuoteRequest,
  type QuoteState,
} from "@lunarbase/math";
import {
  Commitment,
  commitmentRank,
  IndexerError,
  MATH_COMPATIBILITY_VERSION,
  ReducerError,
  SCHEMA_VERSION,
} from "../model.js";
import type { ChainCursor, Checkpoint, DeploymentConfig, QuoteEvent } from "../model.js";
import { compareCursor } from "../source.js";

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
    feeProfile: {
      whitelisted: state.feeProfile.whitelisted,
      blacklistFeeMultiplier: state.feeProfile.blacklistFeeMultiplier,
      partnerFeeBps: new Map([...state.feeProfile.partnerFeeBps].map(([asset, fee]) => [key(asset), fee])),
    },
  };
}

/** In-memory reducer whose maps never escape the client API. */
export class QuoteReducer {
  /** Complete quote-critical state mutated only by this ordered reducer. */
  private state: QuoteState;
  /** Last normalized head or event position accepted by the reducer. */
  private cursorValue?: ChainCursor;
  /** Fail-closed publication flag cleared by gaps and reducer failures. */
  private ready = false;

  /** Creates a not-ready reducer for one configured router. */
  constructor(
    state: QuoteState,
    /** Only router whose fee and whitelist updates affect this instance. */
    private readonly configuredRouter: Address,
  ) {
    this.state = cloneState(state);
  }

  /** Restores a prevalidated checkpoint. */
  static fromCheckpoint(checkpoint: Checkpoint): QuoteReducer {
    const reducer = new QuoteReducer(checkpoint.state, checkpoint.router);
    reducer.cursorValue = { ...checkpoint.cursor };
    reducer.ready = true;
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
    this.ready = true;
  }

  /** Advances head metadata without changing quote state. */
  observeHead(head: ChainCursor): void {
    const current = this.cursorValue;
    if (!current) {
      this.cursorValue = { ...head };
      return;
    }
    this.validateChainAndHash(current, head);
    if (head.blockNumber < current.blockNumber) return;
    if (head.blockNumber > current.blockNumber) {
      this.cursorValue = { ...head };
      return;
    }
    const progression = isRealtimeProgression(current, head);
    this.cursorValue = {
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
  }

  /** Applies one already-ordered quote-critical event. */
  apply(cursor: ChainCursor, event: QuoteEvent): void {
    const previous = this.cursorValue;
    if (previous) {
      this.validateChainAndHash(previous, cursor);
      const previousIsHead =
        previous.blockNumber === cursor.blockNumber &&
        previous.transactionIndex === undefined &&
        previous.logIndex === undefined &&
        cursor.transactionIndex !== undefined &&
        cursor.logIndex !== undefined;
      const order = compareCursor(cursor, previous);
      if (!previousIsHead && order < 0) throw new ReducerError("CURSOR_REGRESSION", "cursor regression");
      if (!previousIsHead && order === 0) return;
    }
    this.validateEvent(event);
    this.applyEvent(event);
    this.cursorValue = { ...cursor };
  }

  /** Computes one quote synchronously from the current event-loop snapshot. */
  quote(request: QuoteRequest): QuoteOutcome {
    const cursor = this.requireReadyCursor();
    return computeQuote(request, cursor.executionBlockNumber, this.state);
  }

  /** Computes a batch without yielding, so every result uses one cursor/state. */
  quoteMany(requests: readonly QuoteRequest[]): readonly QuoteOutcome[] {
    const cursor = this.requireReadyCursor();
    return requests.map((request) => computeQuote(request, cursor.executionBlockNumber, this.state));
  }

  /** Creates a deep-cloned v4 restart checkpoint outside the quote path. */
  checkpoint(config: DeploymentConfig): Checkpoint | undefined {
    if (!this.cursorValue) return undefined;
    return {
      schemaVersion: SCHEMA_VERSION,
      mathCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
      expectedImplementation: config.expectedImplementation,
      expectedImplementationCodeHash: config.expectedImplementationCodeHash,
      chainId: config.chainId,
      core: config.core,
      router: config.router,
      cursor: { ...this.cursorValue },
      state: cloneState(this.state),
    };
  }

  private requireReadyCursor(): ChainCursor {
    if (!this.ready) throw new IndexerError("NOT_READY", "indexer is not ready");
    if (!this.cursorValue) throw new IndexerError("NO_CURSOR", "indexer has no cursor");
    return this.cursorValue;
  }

  private validateChainAndHash(previous: ChainCursor, next: ChainCursor): void {
    if (previous.chainId !== next.chainId) throw new ReducerError("CHAIN_ID_MISMATCH", "cursor chain id mismatch");
    if (
      previous.blockNumber === next.blockNumber &&
      previous.blockHash &&
      next.blockHash &&
      previous.blockHash.toLowerCase() !== next.blockHash.toLowerCase() &&
      !isRealtimeProgression(previous, next)
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
      (!Number.isSafeInteger(event.fee) || event.fee < 0 || BigInt(event.fee) > BPS)
    )
      throw new ReducerError("INVALID_WIDTH", "partner fee exceeds BPS");
    if (
      (event.kind === "DepositExecuted" || event.kind === "WithdrawalExecuted") &&
      (event.principal < 0n || event.principal > U128_MAX)
    )
      throw new ReducerError("INVALID_WIDTH", "principal does not fit uint128");
  }

  private applyEvent(event: QuoteEvent): void {
    switch (event.kind) {
      case "LaneAdded": {
        const lane = this.lane(event.asset);
        const slot0 = setLaneSlot0Exists(lane.slot0, true);
        lane.slot0 = setLaneSlot0Paused(slot0, true);
        this.setLane(event.asset, lane);
        break;
      }
      case "LaneRemoved":
        (this.state.lanes as Map<Address, LaneState>).delete(key(event.asset));
        break;
      case "LaneUpdated": {
        const lane = this.existingLane(event.asset);
        lane.slot0 = event.slot0;
        this.setLane(event.asset, lane);
        break;
      }
      case "SlippageKSet": {
        const lane = this.existingLane(event.asset);
        lane.slot0 = setLaneSlot0SlippageKBps(lane.slot0, event.newK);
        this.setLane(event.asset, lane);
        break;
      }
      case "LanePausedSet": {
        const lane = this.existingLane(event.asset);
        lane.slot0 = setLaneSlot0Paused(lane.slot0, event.paused);
        this.setLane(event.asset, lane);
        break;
      }
      case "PricePushThresholdSet": {
        const lane = this.existingLane(event.asset);
        lane.slot0 = setLaneSlot0PricePushThreshold(lane.slot0, event.pricePushThreshold, event.enabled);
        this.setLane(event.asset, lane);
        break;
      }
      case "BlockDelaySet": {
        const lane = this.existingLane(event.asset);
        lane.slot0 = setLaneSlot0BlockDelay(lane.slot0, event.blockDelay);
        this.setLane(event.asset, lane);
        break;
      }
      case "PartnerInfoSet":
      case "PartnerFeeSet":
        if (key(event.router) === key(this.configuredRouter))
          (this.state.feeProfile.partnerFeeBps as Map<Address, number>).set(key(event.asset), event.fee);
        break;
      case "WhitelistSet":
        if (key(event.router) === key(this.configuredRouter)) this.state.feeProfile.whitelisted = event.whitelisted;
        break;
      case "BlacklistFeeMultiplierSet":
        this.state.feeProfile.blacklistFeeMultiplier = event.multiplier;
        break;
      case "DepositExecuted": {
        const lane = this.existingLane(event.asset);
        const next = lane.totalPrincipalAmount + event.principal;
        if (next > U128_MAX) throw new ReducerError("ARITHMETIC", "principal storage overflow");
        lane.totalPrincipalAmount = next;
        this.setLane(event.asset, lane);
        break;
      }
      case "WithdrawalExecuted": {
        const lane = this.existingLane(event.asset);
        if (event.principal > lane.totalPrincipalAmount) throw new ReducerError("ARITHMETIC", "principal underflow");
        lane.totalPrincipalAmount -= event.principal;
        this.setLane(event.asset, lane);
        break;
      }
      case "Sync": {
        this.state.cashReserve = event.cashReserve;
        if (key(event.asset) !== key(this.state.cash)) {
          const lane = this.existingLane(event.asset);
          lane.assetReserve = event.assetReserve;
          this.setLane(event.asset, lane);
        }
        break;
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
}

function isRealtimeProgression(previous: ChainCursor, next: ChainCursor): boolean {
  return (
    previous.commitment === Commitment.Realtime &&
    next.commitment === Commitment.Realtime &&
    (next.sourceSequence ?? 0n) > (previous.sourceSequence ?? 0n)
  );
}
