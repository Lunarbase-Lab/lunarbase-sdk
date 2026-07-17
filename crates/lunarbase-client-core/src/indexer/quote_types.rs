#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientQuote {
    pub outcome: QuoteOutcome,
    pub cursor: ChainCursor,
    pub commitment: Commitment,
    pub observed_at: SystemTime,
    pub age: Duration,
    pub stale: bool,
    pub contract_code_hash: [u8; 32],
    pub math_compatibility_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexerHealth {
    pub ready: bool,
    pub commitment: Commitment,
    pub cursor: Option<ChainCursor>,
    pub code_hash: [u8; 32],
    pub math_compatibility_version: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FreshnessPolicy {
    pub minimum_commitment: Commitment,
    pub max_age_blocks: Option<u64>,
}

impl Default for FreshnessPolicy {
    fn default() -> Self {
        Self {
            minimum_commitment: Commitment::Realtime,
            max_age_blocks: None,
        }
    }
}
