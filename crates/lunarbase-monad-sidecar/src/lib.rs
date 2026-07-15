//! Sidecar-facing normalized event types.
//!
//! The hugetlbfs/event-ring FFI belongs in the deployment-specific binary. The
//! library exposes the safe boundary consumed by the TypeScript client and
//! requires ring gaps to become explicit `ChainUpdate::Gap` values.

mod parser;
mod protocol;

pub use parser::*;
pub use protocol::*;

use lunarbase_client::{ChainCursor, ChainUpdate, Commitment, ContractLog, Network, SourceError};
use lunarbase_math::{Address, U256};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SidecarConfig {
    pub ring_path: String,
    pub core: Address,
    pub chain_id: u64,
    pub bounded_queue_capacity: usize,
}

impl SidecarConfig {
    pub fn network(&self) -> Network {
        Network::Monad
    }
}

/// Converts a ring transaction log into the common source record. The caller
/// supplies source sequence/sub-index from the ring and EVM transaction/log
/// positions from the execution event.
#[allow(clippy::too_many_arguments)]
pub fn normalize_txn_log(
    config: &SidecarConfig,
    block_number: u64,
    block_hash: Option<[u8; 32]>,
    transaction_index: u32,
    log_index: u32,
    source_sequence: u64,
    source_sub_index: u32,
    topics: Vec<U256>,
    data: Vec<u8>,
    commitment: Commitment,
) -> Result<ChainUpdate, SourceError> {
    Ok(ChainUpdate::Log(ContractLog {
        address: config.core,
        topics,
        data,
        removed: false,
        cursor: ChainCursor {
            chain_id: config.chain_id,
            block_number,
            block_hash,
            transaction_index: Some(transaction_index),
            log_index: Some(log_index),
            source_sequence: Some(source_sequence),
            source_sub_index: Some(source_sub_index),
            commitment,
        },
    }))
}

pub fn normalize_gap(reason: impl Into<String>) -> ChainUpdate {
    ChainUpdate::Gap {
        cursor: None,
        reason: reason.into(),
    }
}
