//! Compact quote-critical state and public quote request/result types.

use crate::types::{Address, MathError, U256};
use std::collections::HashMap;

const LANE_EXISTS: u8 = 1;
const LANE_PAUSED: u8 = 1 << 1;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Fees for the single router configured by a client or indexer instance.
///
/// The router address belongs to deployment identity rather than pure quote
/// math. Keeping only its effective fee state removes a router lookup from
/// every quote while retaining the exact Solidity fee split per asset.
pub struct FeeProfile {
    /// Whether the configured router bypasses the global blacklist multiplier.
    pub whitelisted: bool,
    /// Global multiplier applied only when `whitelisted` is false.
    pub blacklist_fee_multiplier: U256,
    /// Partner fee configured for the runtime's router, keyed by fee asset.
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
    /// Returns the configured router's partner fee for `asset`.
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
    /// Quote-critical lane state keyed by non-cash asset address.
    pub lanes: HashMap<Address, LaneState>,
    /// Effective fee configuration for the single runtime router.
    pub fee_profile: FeeProfile,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Compact quote-critical state for one lane.
///
/// Principal is stored next to `slot0`, eliminating a second map lookup.
/// Boolean fields share one byte so the native representation remains small.
pub struct LaneState {
    /// Raw packed Solidity `Lane.slot0` word used directly on the hot path.
    pub slot0: U256,
    /// Total active principal used as the denominator of lane slippage.
    pub total_principal_amount: u128,
    /// Lane-specific slippage coefficient in protocol BPS.
    pub slippage_k_bps: u32,
    /// Number of execution blocks required after the latest price update.
    pub block_delay: u8,
    /// Compact lifecycle flags for lane existence and pause state.
    flags: u8,
}

impl LaneState {
    /// Builds a lane with explicit lifecycle flags.
    pub const fn new(
        slot0: U256,
        total_principal_amount: u128,
        slippage_k_bps: u32,
        block_delay: u8,
        exists: bool,
        paused: bool,
    ) -> Self {
        let mut flags = 0;
        if exists {
            flags |= LANE_EXISTS;
        }
        if paused {
            flags |= LANE_PAUSED;
        }
        Self {
            slot0,
            total_principal_amount,
            slippage_k_bps,
            block_delay,
            flags,
        }
    }

    /// Returns whether the Core currently exposes this lane.
    #[inline]
    pub const fn exists(&self) -> bool {
        self.flags & LANE_EXISTS != 0
    }

    /// Returns whether quotes through this lane are paused.
    #[inline]
    pub const fn paused(&self) -> bool {
        self.flags & LANE_PAUSED != 0
    }

    /// Updates the existence bit without touching other compact flags.
    #[inline]
    pub fn set_exists(&mut self, exists: bool) {
        self.flags = if exists {
            self.flags | LANE_EXISTS
        } else {
            self.flags & !LANE_EXISTS
        };
    }

    /// Updates the pause bit without touching other compact flags.
    #[inline]
    pub fn set_paused(&mut self, paused: bool) {
        self.flags = if paused {
            self.flags | LANE_PAUSED
        } else {
            self.flags & !LANE_PAUSED
        };
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
    /// The lane has not passed its configured post-update block delay.
    DelayedLane(Address),
    /// The required lane has no usable price.
    ZeroPrice(Address),
    /// Slippage cannot be evaluated because principal is zero.
    ZeroPrincipal(Address),
    /// Price conversion produced a zero pre-fee anchor amount.
    ZeroAnchor,
    /// Fee and slippage deductions consume the complete quote anchor.
    SpreadConsumesAnchor,
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
