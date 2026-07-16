use crate::{Address, U256};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Immutable state required by the quote engine.
///
/// Maps use deterministic `BTreeMap` ordering so checkpoint bytes are stable
/// across processes and can be compared with the TypeScript codec.
pub struct QuoteState {
    pub cash: Address,
    pub lanes: BTreeMap<Address, LaneState>,
    pub total_principal_amount: BTreeMap<Address, U256>,
    pub whitelist: BTreeMap<Address, bool>,
    pub blacklist_fee_multiplier: U256,
    pub partner_fee_bps: BTreeMap<(Address, Address), U256>,
    pub state_version: u64,
}
pub trait QuoteStateView {
    /// Returns the cached lane for an asset, if it exists in the snapshot.
    fn lane(&self, asset: Address) -> Option<&LaneState>;
    /// Returns active principal for an asset, defaulting to zero when absent.
    fn total_principal_amount(&self, asset: Address) -> U256;
    /// Returns whether a router is whitelisted in this snapshot.
    fn is_whitelisted(&self, router: Address) -> bool;
    /// Returns the global blacklist fee multiplier.
    fn blacklist_fee_multiplier(&self) -> U256;
    /// Returns a router/asset partner fee, defaulting to zero when absent.
    fn partner_fee_bps(&self, router: Address, asset: Address) -> U256;
}
impl QuoteStateView for QuoteState {
    fn lane(&self, asset: Address) -> Option<&LaneState> {
        self.lanes.get(&asset)
    }
    fn total_principal_amount(&self, asset: Address) -> U256 {
        self.total_principal_amount
            .get(&asset)
            .copied()
            .unwrap_or(U256::ZERO)
    }
    fn is_whitelisted(&self, router: Address) -> bool {
        self.whitelist.get(&router).copied().unwrap_or(false)
    }
    fn blacklist_fee_multiplier(&self) -> U256 {
        self.blacklist_fee_multiplier
    }
    fn partner_fee_bps(&self, router: Address, asset: Address) -> U256 {
        self.partner_fee_bps
            .get(&(router, asset))
            .copied()
            .unwrap_or(U256::ZERO)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Quote-critical state for one lane in addition to its packed slot word.
pub struct LaneState {
    pub slot0: U256,
    pub exists: bool,
    pub paused: bool,
    pub block_delay: u8,
    pub slippage_k_bps: u32,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Snapshot identity and EVM execution context used to validate a quote.
pub struct QuoteContext {
    pub cash: Address,
    pub execution_block_number: U256,
    pub state_version: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Direction of the public Solidity quote operation.
pub enum QuoteMode {
    ExactIn,
    ExactOut,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Caller-supplied quote parameters. `router` is explicit because Solidity
/// derives it from `msg.sender` and it affects fees.
pub struct QuoteRequest {
    pub router: Address,
    pub asset_in: Address,
    pub asset_out: Address,
    pub amount: U256,
    pub mode: QuoteMode,
}
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Full off-chain quote result, including fee allocation details hidden by
/// the Solidity scalar getters.
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
/// Errors that abort quote evaluation instead of producing an unavailable
/// sentinel. These correspond to snapshot identity or checked arithmetic
/// violations.
pub enum QuoteError {
    #[error(transparent)]
    Arithmetic(#[from] crate::MathError),
    #[error("state snapshot cash does not match quote context")]
    CashMismatch,
    #[error("state snapshot version does not match quote context")]
    StateVersionMismatch,
}
