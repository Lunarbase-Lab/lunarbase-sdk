//! Block-tagged snapshot returned by a [`crate::source::ChainDataSource`].

use crate::model::ChainCursor;
use lunarbase_math::QuoteState;
use lunarbase_math::{Address, B256};
use std::collections::HashMap;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Router-specific accounting snapshot kept outside quote-critical chain state.
pub struct VerifiedRouterSnapshot {
    /// Execution caller whose whitelist class and fee shares were verified.
    pub router: Address,
    /// Partner share keyed by the asset in which the quote fee is denominated.
    pub partner_fee_bps: HashMap<Address, u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Fully materialized state and cursor read at one coherent block tag.
pub struct BootstrapSnapshot {
    /// Fully materialized quote state read at one coherent block tag.
    pub state: QuoteState,
    /// Canonical cursor identifying the block from which `state` was read.
    pub cursor: ChainCursor,
    /// ERC-1967 implementation active at the snapshot block.
    pub implementation: Address,
    /// Keccak-256 runtime bytecode hash of `implementation`.
    pub implementation_code_hash: B256,
    /// Optional exact-router accounting data verified at the same block.
    pub verified_router: Option<VerifiedRouterSnapshot>,
}
