//! Compact quote-critical state and public quote request/result types.

use crate::slot0::{lane_slot0_exists, lane_slot0_paused};
use crate::types::{Address, MathError, U256};
use std::collections::HashMap;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Effective fee state for the router configured by a client or indexer.
pub struct FeeProfile {
    /// Whether the configured router bypasses the global blacklist multiplier.
    pub whitelisted: bool,
    /// Global multiplier applied only when `whitelisted` is false.
    pub blacklist_fee_multiplier: U256,
    /// Share of the explicit fee assigned to the runtime's router, keyed by fee asset.
    pub partner_fee_bps: HashMap<Address, u32>,
}

impl Default for FeeProfile {
    fn default() -> Self {
        Self {
            whitelisted: true,
            blacklist_fee_multiplier: U256::ONE,
            partner_fee_bps: HashMap::new(),
        }
    }
}

impl FeeProfile {
    /// Returns the configured router's explicit-fee share for `asset`.
    #[inline]
    pub fn partner_fee_bps(&self, asset: Address) -> U256 {
        U256::from(self.partner_fee_bps.get(&asset).copied().unwrap_or(0))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Immutable quote-critical snapshot held by the runtime.
///
/// `HashMap` provides constant-time lane lookup on the hot path. Persistence
/// sorts a separate checkpoint DTO, so deterministic bytes do not constrain
/// the in-memory representation.
pub struct QuoteState {
    /// Settlement asset used as the intermediate asset for routed quotes.
    pub cash: Address,
    /// Free CASH balance available for output settlement after liabilities.
    pub cash_reserve: u128,
    /// Quote-critical lane state keyed by non-cash asset address.
    pub lanes: HashMap<Address, LaneState>,
    /// Effective fee configuration for the single runtime router.
    pub fee_profile: FeeProfile,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Compact quote-critical state for one lane.
///
/// Reserve and principal are stored next to the packed lane word, eliminating
/// secondary map lookups while preserving the contract's single-word controls.
pub struct LaneState {
    /// Raw packed Solidity lane word used directly on the hot path.
    pub slot0: U256,
    /// Free lane-asset balance available for output settlement after liabilities.
    pub asset_reserve: u128,
    /// Total active principal used as the denominator of lane slippage.
    pub total_principal_amount: u128,
}

impl LaneState {
    /// Builds a lane from its packed word and quote-critical reserves.
    pub const fn new(slot0: U256, asset_reserve: u128, total_principal_amount: u128) -> Self {
        Self {
            slot0,
            asset_reserve,
            total_principal_amount,
        }
    }

    /// Returns the packed existence bit.
    #[inline]
    pub fn exists(&self) -> bool {
        lane_slot0_exists(self.slot0)
    }

    /// Returns the packed lane pause bit.
    #[inline]
    pub fn paused(&self) -> bool {
        lane_slot0_paused(self.slot0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Direction of the public Solidity quote operation.
pub enum QuoteMode {
    /// Treat `amount` as input and maximize the resulting output.
    ExactIn,
    /// Treat `amount` as required output and calculate the necessary input.
    ExactOut,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Pure quote parameters. Router and freshness policy belong to the runtime.
pub struct QuoteRequest {
    /// Asset transferred by the caller.
    pub asset_in: Address,
    /// Asset the caller expects to receive.
    pub asset_out: Address,
    /// Exact input or output amount according to `mode`.
    pub amount: U256,
    /// Direction in which the quote equations and rounding are evaluated.
    pub mode: QuoteMode,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Full off-chain quote result, including the retained fee allocation details.
pub struct QuoteResult {
    /// Total input required or consumed by the quote.
    pub amount_in: U256,
    /// Total output produced or requested by the quote.
    pub amount_out: U256,
    /// Asset in which protocol and partner fees are denominated.
    pub fee_asset: Address,
    /// Complete fee amount before splitting partner and treasury shares.
    pub fee_amount: U256,
    /// Portion of `fee_amount` assigned to the configured partner.
    pub partner_fee: U256,
    /// Remaining portion of `fee_amount` assigned to the treasury.
    pub treasury_fee: U256,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Deterministic reason why a quote cannot be produced.
pub enum UnavailableReason {
    /// A zero-sized swap has no executable quote.
    ZeroAmount,
    /// Input and output assets must be different.
    EqualAssets,
    /// The required asset lane does not exist in the supplied state.
    MissingLane(Address),
    /// The required lane is currently paused.
    PausedLane(Address),
    /// The lane's price update has exceeded its configured block TTL.
    StaleLane(Address),
    /// Slippage cannot be evaluated because principal is zero.
    ZeroPrincipal(Address),
    /// Price conversion produced a zero pre-fee anchor amount.
    ZeroAnchor,
    /// Fee and slippage deductions consume the complete quote anchor.
    SpreadConsumesAnchor,
    /// The free output reserve cannot cover the transfer and output-side fee.
    InsufficientOutputReserve(Address),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Rich quote result preserving availability diagnostics.
pub enum QuoteOutcome {
    /// A complete quote that can be returned to the caller.
    Available(QuoteResult),
    /// A deterministic protocol-state reason why no quote is available.
    Unavailable(UnavailableReason),
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
/// Solidity-compatible checked arithmetic failure.
pub enum QuoteError {
    /// Checked arithmetic failed with the same boundary as Solidity.
    #[error(transparent)]
    Arithmetic(#[from] MathError),
}
