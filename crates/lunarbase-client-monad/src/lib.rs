//! Monad parser and execution-events client.
//!
//! The crate supplies a parser reader today and shares the universal
//! [`MonadExecutionEngine`] with future native event-ring readers deployed
//! beside a Monad node.

mod parser;
mod protocol;

pub use parser::*;
pub use protocol::*;

pub use lunarbase_client_core::{ExecutionEventReader, MonadExecutionEngine};

use lunarbase_client_core::{
    ChainUpdate, Commitment, ExecutionLog, MonadExecutionNormalizer, Network, SourceError,
};
use lunarbase_math::{Address, U256};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SidecarConfig {
    pub ring_path: String,
    pub core: Address,
    pub chain_id: u64,
    pub bounded_queue_capacity: usize,
}

/// Backward-compatible name for native Monad reader deployment settings.
pub type MonadNodeConfig = SidecarConfig;

impl SidecarConfig {
    /// Returns the network identity carried by this sidecar configuration.
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
    let mut normalizer = MonadExecutionNormalizer::new(config.chain_id);
    normalizer
        .normalize_log(ExecutionLog {
            sequence: source_sequence,
            source_sub_index,
            block_number,
            block_hash,
            transaction_index,
            log_index,
            address: config.core,
            topics,
            data,
            commitment,
        })?
        .ok_or_else(|| SourceError::Gap("duplicate Monad execution log".into()))
}

/// Converts parser/ring discontinuity into an explicit recovery marker.
pub fn normalize_gap(reason: impl Into<String>) -> ChainUpdate {
    ChainUpdate::Gap {
        cursor: None,
        reason: reason.into(),
    }
}
