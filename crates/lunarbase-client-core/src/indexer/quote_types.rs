use crate::{ChainCursor, Commitment};
use lunarbase_math::{QuoteOutcome, B256};

#[derive(Clone, Debug, Eq, PartialEq)]
/// One quote plus the exact state cursor used for evaluation.
pub struct ClientQuote {
    pub outcome: QuoteOutcome,
    pub cursor: ChainCursor,
    pub execution_block_number: u64,
    pub contract_code_hash: B256,
    pub math_compatibility_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Batch evaluated under one shared state read guard and cursor.
pub struct ClientBatchQuote {
    pub outcomes: Vec<QuoteOutcome>,
    pub cursor: ChainCursor,
    pub execution_block_number: u64,
    pub contract_code_hash: B256,
    pub math_compatibility_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Observable health and canonical cursor state.
pub struct IndexerHealth {
    pub ready: bool,
    pub commitment: Commitment,
    pub cursor: Option<ChainCursor>,
    pub execution_block_number: Option<u64>,
    pub code_hash: B256,
    pub math_compatibility_version: String,
}
