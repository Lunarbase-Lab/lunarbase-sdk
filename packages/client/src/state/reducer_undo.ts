/** Compact before-images for allocation-safe optimistic rollback. */
import type { Address, LaneState, QuoteState } from "@lunarbase-lab/pmm-v2-math";
import type { VerifiedRouterSnapshot } from "../model.js";

/** State mutation before-image; unchanged fields are never retained. */
export type ReducerUndo =
  | { readonly kind: "Lane"; readonly asset: Address; readonly previous?: LaneState }
  | {
      readonly kind: "LaneAndPartner";
      readonly asset: Address;
      readonly previousLane?: LaneState;
      readonly hadPartnerFee: boolean;
      readonly previousPartnerFee?: number;
    }
  | {
      readonly kind: "CashAndLane";
      readonly asset?: Address;
      readonly previousCashReserve: bigint;
      readonly previousLane?: LaneState;
    }
  | { readonly kind: "PartnerFee"; readonly asset: Address; readonly hadValue: boolean; readonly previous?: number }
  | { readonly kind: "BlacklistMultiplier"; readonly previous: bigint };

/** Conservative retained-memory charge for one compact before-image. */
export function reducerUndoRetainedBytes(undo: ReducerUndo): number {
  return undo.kind === "CashAndLane" || undo.kind === "LaneAndPartner" ? 256 : 160;
}

/** Applies one before-image to an isolated correction candidate. */
export function restoreReducerUndo(
  state: QuoteState,
  verifiedRouter: VerifiedRouterSnapshot | undefined,
  undo: ReducerUndo,
): void {
  switch (undo.kind) {
    case "Lane":
      restoreLane(state, undo.asset, undo.previous);
      break;
    case "LaneAndPartner": {
      restoreLane(state, undo.asset, undo.previousLane);
      const fees = verifiedRouter?.partnerFeeBps as Map<Address, number> | undefined;
      if (undo.hadPartnerFee) fees?.set(undo.asset, undo.previousPartnerFee!);
      else fees?.delete(undo.asset);
      break;
    }
    case "CashAndLane":
      state.cashReserve = undo.previousCashReserve;
      if (undo.asset) restoreLane(state, undo.asset, undo.previousLane);
      break;
    case "PartnerFee": {
      const fees = verifiedRouter?.partnerFeeBps as Map<Address, number> | undefined;
      if (undo.hadValue) fees?.set(undo.asset, undo.previous!);
      else fees?.delete(undo.asset);
      break;
    }
    case "BlacklistMultiplier":
      state.blacklistFeeMultiplier = undo.previous;
      break;
  }
}

function restoreLane(state: QuoteState, asset: Address, previous: LaneState | undefined): void {
  const lanes = state.lanes as Map<Address, LaneState>;
  if (previous) lanes.set(asset, previous);
  else lanes.delete(asset);
}
