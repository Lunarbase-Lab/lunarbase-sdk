import * as Abi from "ox/Abi";
import * as AbiEvent from "ox/AbiEvent";

/**
 * Minimal pinned Core ABI used by bootstrap and quote-critical event replay.
 *
 * Keeping one parsed ABI removes handwritten selectors and makes both RPC
 * calls and event decoding derive their widths from the Solidity interface.
 */
export const CORE_ABI = Abi.from([
  "function cash() view returns (address)",
  "function lane(address asset) view returns (bytes32 laneWord)",
  "function reserves(address asset) view returns (uint128 assetReserve, uint128 treasuryFees, uint128 partnerFees, uint128 escrowedAssets, uint128 totalPrincipalAmount)",
  "function whitelist(address account) view returns (bool)",
  "function blacklistFeeMultiplier() view returns (uint256)",
  "function partners(address router, address asset) view returns (uint128 cumFees, uint32 fee, uint32 latestWithdrawTimestamp, address operator)",
  "event LaneAdded(address indexed asset)",
  "event LaneRemoved(address indexed asset)",
  "event LaneUpdated(address indexed asset, bytes32 slot0)",
  "event LanePausedSet(address indexed asset, bool previousPaused, bool newPaused)",
  "event PricePushThresholdSet(address indexed asset, uint8 previousThreshold, uint8 newThreshold, bool previousEnabled, bool newEnabled)",
  "event SlippageKSet(address indexed asset, uint32 previousK, uint32 newK)",
  "event BlockDelaySet(address indexed asset, uint8 previousBlockDelay, uint8 newBlockDelay)",
  "event PartnerInfoSet(address indexed router, address indexed asset, uint32 fee, address indexed operator)",
  "event PartnerFeeSet(address indexed router, address indexed asset, uint32 fee)",
  "event WhitelistSet(address indexed account, bool whitelisted)",
  "event BlacklistFeeMultiplierSet(uint256 multiplier)",
  "event DepositExecuted(uint256 indexed id, address indexed lpAuthority, address indexed asset, uint128 principalAmount)",
  "event WithdrawalExecuted(uint256 indexed id, address indexed lpAuthority, address indexed asset, uint128 principalAmount, uint256 principalOut, uint256 penaltyAmount, address principalReceiver)",
  "event Sync(address indexed lane, uint128 assetReserve, uint128 cashReserve)",
  "event Upgraded(address indexed implementation)",
] as const);

/** Parsed quote-critical events keyed by their Solidity names. */
export const CORE_EVENTS = {
  LaneAdded: AbiEvent.from("event LaneAdded(address indexed asset)"),
  LaneRemoved: AbiEvent.from("event LaneRemoved(address indexed asset)"),
  LaneUpdated: AbiEvent.from("event LaneUpdated(address indexed asset, bytes32 slot0)"),
  LanePausedSet: AbiEvent.from("event LanePausedSet(address indexed asset, bool previousPaused, bool newPaused)"),
  PricePushThresholdSet: AbiEvent.from(
    "event PricePushThresholdSet(address indexed asset, uint8 previousThreshold, uint8 newThreshold, bool previousEnabled, bool newEnabled)",
  ),
  SlippageKSet: AbiEvent.from("event SlippageKSet(address indexed asset, uint32 previousK, uint32 newK)"),
  BlockDelaySet: AbiEvent.from(
    "event BlockDelaySet(address indexed asset, uint8 previousBlockDelay, uint8 newBlockDelay)",
  ),
  PartnerInfoSet: AbiEvent.from(
    "event PartnerInfoSet(address indexed router, address indexed asset, uint32 fee, address indexed operator)",
  ),
  PartnerFeeSet: AbiEvent.from("event PartnerFeeSet(address indexed router, address indexed asset, uint32 fee)"),
  WhitelistSet: AbiEvent.from("event WhitelistSet(address indexed account, bool whitelisted)"),
  BlacklistFeeMultiplierSet: AbiEvent.from("event BlacklistFeeMultiplierSet(uint256 multiplier)"),
  DepositExecuted: AbiEvent.from(
    "event DepositExecuted(uint256 indexed id, address indexed lpAuthority, address indexed asset, uint128 principalAmount)",
  ),
  WithdrawalExecuted: AbiEvent.from(
    "event WithdrawalExecuted(uint256 indexed id, address indexed lpAuthority, address indexed asset, uint128 principalAmount, uint256 principalOut, uint256 penaltyAmount, address principalReceiver)",
  ),
  Sync: AbiEvent.from("event Sync(address indexed lane, uint128 assetReserve, uint128 cashReserve)"),
  Upgraded: AbiEvent.from("event Upgraded(address indexed implementation)"),
} as const;

/** Topic selectors for all state transitions consumed by the quote reducer. */
export const CORE_EVENT_TOPICS = {
  LaneAdded: AbiEvent.getSelector(CORE_EVENTS.LaneAdded),
  LaneRemoved: AbiEvent.getSelector(CORE_EVENTS.LaneRemoved),
  LaneUpdated: AbiEvent.getSelector(CORE_EVENTS.LaneUpdated),
  LanePausedSet: AbiEvent.getSelector(CORE_EVENTS.LanePausedSet),
  PricePushThresholdSet: AbiEvent.getSelector(CORE_EVENTS.PricePushThresholdSet),
  SlippageKSet: AbiEvent.getSelector(CORE_EVENTS.SlippageKSet),
  BlockDelaySet: AbiEvent.getSelector(CORE_EVENTS.BlockDelaySet),
  PartnerInfoSet: AbiEvent.getSelector(CORE_EVENTS.PartnerInfoSet),
  PartnerFeeSet: AbiEvent.getSelector(CORE_EVENTS.PartnerFeeSet),
  WhitelistSet: AbiEvent.getSelector(CORE_EVENTS.WhitelistSet),
  BlacklistFeeMultiplierSet: AbiEvent.getSelector(CORE_EVENTS.BlacklistFeeMultiplierSet),
  DepositExecuted: AbiEvent.getSelector(CORE_EVENTS.DepositExecuted),
  WithdrawalExecuted: AbiEvent.getSelector(CORE_EVENTS.WithdrawalExecuted),
  Sync: AbiEvent.getSelector(CORE_EVENTS.Sync),
  Upgraded: AbiEvent.getSelector(CORE_EVENTS.Upgraded),
} as const;
