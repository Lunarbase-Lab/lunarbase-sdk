//! Executed Arbitrum Nitro state normalization.

use lunarbase_client_core::{ChainCursor, ChainUpdate, Commitment, ContractLog, SourceError};
use lunarbase_math::U256;

/// Nitro block context containing both L2 and EVM-visible parent heights.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArbitrumExecutionContext {
    pub l2_block_number: u64,
    pub evm_parent_block_number: u64,
}

impl ArbitrumExecutionContext {
    /// Returns the block number observed by the EVM `NUMBER` opcode.
    pub fn execution_block_number(self) -> U256 {
        U256::from(self.evm_parent_block_number)
    }
}

/// Executed Nitro head with quote-relevant parent-chain context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArbitrumHead {
    pub context: ArbitrumExecutionContext,
    pub block_hash: Option<[u8; 32]>,
    pub commitment: Commitment,
}

/// Converts executed Nitro records into common runtime updates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArbitrumNitroNormalizer {
    chain_id: u64,
}

impl ArbitrumNitroNormalizer {
    /// Creates a normalizer for one Arbitrum chain id.
    pub fn new(chain_id: u64) -> Self {
        Self { chain_id }
    }

    /// Converts a Nitro head while retaining EVM parent block context.
    pub fn normalize_head(&self, head: ArbitrumHead) -> ChainUpdate {
        ChainUpdate::Head(ChainCursor {
            chain_id: self.chain_id,
            block_number: head.context.l2_block_number,
            block_hash: head.block_hash,
            transaction_index: None,
            log_index: None,
            source_sequence: Some(head.context.evm_parent_block_number),
            source_sub_index: None,
            commitment: head.commitment,
        })
    }

    /// Validates that an executed or backfilled log belongs to this chain.
    pub fn normalize_log(&self, log: ContractLog) -> Result<ChainUpdate, SourceError> {
        if log.cursor.chain_id != self.chain_id {
            return Err(SourceError::NetworkMismatch);
        }
        Ok(ChainUpdate::Log(log))
    }
}
