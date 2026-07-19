//! Block-tagged snapshot returned by a [`crate::source::ChainDataSource`].

use crate::model::ChainCursor;
use lunarbase_math::state::QuoteState;
use lunarbase_math::types::B256;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Fully materialized state and cursor read at one coherent block tag.
pub struct BootstrapSnapshot {
    pub state: QuoteState,
    pub cursor: ChainCursor,
    pub runtime_code_hash: B256,
}
