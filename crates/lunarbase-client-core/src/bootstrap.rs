//! Block-tagged snapshot returned by a [`crate::ChainDataSource`].

use crate::ChainCursor;
use lunarbase_math::QuoteState;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Fully materialized state and cursor read at one coherent block tag.
pub struct BootstrapSnapshot {
    pub state: QuoteState,
    pub cursor: ChainCursor,
    pub runtime_code_hash: [u8; 32],
}
