/** Allocation-free semantic comparison for already decoded quote events. */
import type { QuoteEvent } from "../model.js";

/** Returns whether two decoder-owned events describe the same state transition. */
export function quoteEventsEqual(left: QuoteEvent, right: QuoteEvent): boolean {
  switch (left.kind) {
    case "LaneAdded":
      return right.kind === "LaneAdded" && left.asset === right.asset;
    case "LaneRemoved":
      return right.kind === "LaneRemoved" && left.asset === right.asset;
    case "LaneUpdated":
      return right.kind === "LaneUpdated" && left.asset === right.asset && left.slot0 === right.slot0;
    case "SlippageKSet":
      return right.kind === "SlippageKSet" && left.asset === right.asset && left.newK === right.newK;
    case "LanePausedSet":
      return right.kind === "LanePausedSet" && left.asset === right.asset && left.paused === right.paused;
    case "PricePushThresholdSet":
      return (
        right.kind === "PricePushThresholdSet" &&
        left.asset === right.asset &&
        left.pricePushThreshold === right.pricePushThreshold &&
        left.enabled === right.enabled
      );
    case "BlockDelaySet":
      return right.kind === "BlockDelaySet" && left.asset === right.asset && left.blockDelay === right.blockDelay;
    case "PartnerInfoSet":
      return (
        right.kind === "PartnerInfoSet" &&
        left.router === right.router &&
        left.asset === right.asset &&
        left.fee === right.fee
      );
    case "PartnerFeeSet":
      return (
        right.kind === "PartnerFeeSet" &&
        left.router === right.router &&
        left.asset === right.asset &&
        left.fee === right.fee
      );
    case "WhitelistSet":
      return right.kind === "WhitelistSet" && left.router === right.router && left.whitelisted === right.whitelisted;
    case "BlacklistFeeMultiplierSet":
      return right.kind === "BlacklistFeeMultiplierSet" && left.multiplier === right.multiplier;
    case "DepositExecuted":
      return right.kind === "DepositExecuted" && left.asset === right.asset && left.principal === right.principal;
    case "WithdrawalExecuted":
      return right.kind === "WithdrawalExecuted" && left.asset === right.asset && left.principal === right.principal;
    case "Sync":
      return (
        right.kind === "Sync" &&
        left.asset === right.asset &&
        left.assetReserve === right.assetReserve &&
        left.cashReserve === right.cashReserve
      );
    case "ImplementationUpgraded":
      return right.kind === "ImplementationUpgraded" && left.implementation === right.implementation;
  }
}
