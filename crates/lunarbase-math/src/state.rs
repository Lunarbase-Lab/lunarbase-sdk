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
    pub whitelisted: bool,
    pub blacklist_fee_multiplier: U256,
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
    pub cash: Address,
    pub lanes: HashMap<Address, LaneState>,
    pub fee_profile: FeeProfile,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Compact quote-critical state for one lane.
///
/// Principal is stored next to `slot0`, eliminating a second map lookup.
/// Boolean fields share one byte so the native representation remains small.
pub struct LaneState {
    pub slot0: U256,
    pub total_principal_amount: u128,
    pub slippage_k_bps: u32,
    pub block_delay: u8,
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
    ExactIn,
    ExactOut,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Pure quote parameters. Router and freshness policy belong to the runtime.
pub struct QuoteRequest {
    pub asset_in: Address,
    pub asset_out: Address,
    pub amount: U256,
    pub mode: QuoteMode,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Full off-chain quote result, including the retained fee allocation details.
pub struct QuoteResult {
    pub amount_in: U256,
    pub amount_out: U256,
    pub fee_asset: Address,
    pub fee_amount: U256,
    pub partner_fee: U256,
    pub treasury_fee: U256,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Deterministic reason why a quote cannot be produced.
pub enum UnavailableReason {
    ZeroAmount,
    EqualAssets,
    MissingLane(Address),
    PausedLane(Address),
    DelayedLane(Address),
    ZeroPrice(Address),
    ZeroPrincipal(Address),
    ZeroAnchor,
    SpreadConsumesAnchor,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Rich quote result preserving availability diagnostics.
pub enum QuoteOutcome {
    Available(QuoteResult),
    Unavailable(UnavailableReason),
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
/// Solidity-compatible checked arithmetic failure.
pub enum QuoteError {
    #[error(transparent)]
    Arithmetic(#[from] MathError),
}
