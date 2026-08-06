//! Block-tagged snapshot returned by a [`crate::source::ChainDataSource`].

use crate::model::ChainCursor;
use lunarbase_math::QuoteState;
use lunarbase_math::{Address, B256};

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
}
