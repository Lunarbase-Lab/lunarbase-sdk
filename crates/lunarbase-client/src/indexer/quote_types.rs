//! Quote responses and health views returned by the embeddable client.

use crate::model::{ChainCursor, Commitment};
use lunarbase_math::state::QuoteOutcome;
use lunarbase_math::types::B256;

#[derive(Clone, Debug, Eq, PartialEq)]
/// One quote plus the exact state cursor used for evaluation.
pub struct ClientQuote {
    /// Bit-exact quote result or deterministic unavailability reason.
    pub outcome: QuoteOutcome,
    /// Exact normalized state position used for evaluation.
    pub cursor: ChainCursor,
    /// EVM-visible block number supplied to time-dependent quote math.
    pub execution_block_number: u64,
    /// Core implementation bytecode hash associated with the state snapshot.
    pub implementation_code_hash: B256,
    /// Quote-math compatibility profile used by this result.
    pub math_compatibility_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Batch evaluated under one shared state read guard and cursor.
pub struct ClientBatchQuote {
    /// Results evaluated under one shared state read guard.
    pub outcomes: Vec<QuoteOutcome>,
    /// Single normalized state position shared by every result.
    pub cursor: ChainCursor,
    /// Single EVM-visible block number shared by every result.
    pub execution_block_number: u64,
    /// Core implementation bytecode hash associated with the shared snapshot.
    pub implementation_code_hash: B256,
    /// Quote-math compatibility profile used by this batch.
    pub math_compatibility_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Observable health and canonical cursor state.
pub struct IndexerHealth {
    /// Whether the runtime currently permits quotes.
    pub ready: bool,
    /// Confidence level of the latest accepted state.
    pub commitment: Commitment,
    /// Latest accepted normalized position, absent before bootstrap.
    pub cursor: Option<ChainCursor>,
    /// EVM-visible block used by the latest state, absent before bootstrap.
    pub execution_block_number: Option<u64>,
    /// Expected Core implementation bytecode hash for this deployment.
    pub implementation_code_hash: B256,
    /// Quote-math compatibility profile used by this runtime.
    pub math_compatibility_version: String,
}
