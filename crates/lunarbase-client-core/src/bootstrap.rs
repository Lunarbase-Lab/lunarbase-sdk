//! Block-tagged snapshot returned by a [`crate::source::ChainDataSource`].

use crate::model::ChainCursor;
use lunarbase_math::state::QuoteState;
use lunarbase_math::types::B256;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Fully materialized state and cursor read at one coherent block tag.
pub struct BootstrapSnapshot {
    /// Fully materialized quote state read at one coherent block tag.
    pub state: QuoteState,
    /// Canonical cursor identifying the block from which `state` was read.
    pub cursor: ChainCursor,
    /// Keccak-256 hash of the Core runtime bytecode at the snapshot block.
    pub runtime_code_hash: B256,
}
