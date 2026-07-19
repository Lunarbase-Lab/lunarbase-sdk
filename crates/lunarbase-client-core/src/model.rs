//! Provider-independent runtime model shared by every network adapter.

use lunarbase_math::state::QuoteState;
use lunarbase_math::types::{Address, B256, Bytes, U256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Current durable checkpoint schema.
pub const SCHEMA_VERSION: u16 = 3;

/// Pinned Solidity implementation used by both pure math packages.
pub const MATH_COMPATIBILITY_VERSION: &str =
    "lunarbase-contracts@24db47b866e8150a0d91cffd80efe49df85179b5:math-v1";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
/// Supported chain families.
pub enum Network {
    Base,
    Monad,
    Arbitrum,
}

impl Network {
    /// Returns the default mainnet chain id for this network family.
    pub const fn default_chain_id(self) -> u64 {
        match self {
            Self::Base => 8453,
            Self::Monad => 143,
            Self::Arbitrum => 42161,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
/// Confidence level attached to every normalized cursor.
pub enum Commitment {
    Realtime,
    Canonical,
    Finalized,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
/// Ordered provider position plus the EVM-visible execution block.
///
/// `source_sequence` is used only to order provider messages. Network-specific
/// parent heights belong in `execution_block_number`, so math never interprets
/// a transport sequence as EVM semantics.
pub struct ChainCursor {
    pub chain_id: u64,
    pub block_number: u64,
    pub execution_block_number: u64,
    pub block_hash: Option<B256>,
    pub transaction_index: Option<u32>,
    pub log_index: Option<u32>,
    pub source_sequence: Option<u64>,
    pub source_sub_index: Option<u32>,
    pub commitment: Commitment,
}

impl ChainCursor {
    /// Creates a block-level cursor for networks whose provider and execution
    /// heights are identical.
    pub fn block(
        chain_id: u64,
        block_number: u64,
        block_hash: Option<B256>,
        commitment: Commitment,
    ) -> Self {
        Self::execution_block(chain_id, block_number, block_number, block_hash, commitment)
    }

    /// Creates a block-level cursor with an explicit EVM execution height.
    pub fn execution_block(
        chain_id: u64,
        block_number: u64,
        execution_block_number: u64,
        block_hash: Option<B256>,
        commitment: Commitment,
    ) -> Self {
        Self {
            chain_id,
            block_number,
            execution_block_number,
            block_hash,
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment,
        }
    }

    pub(crate) fn event_order(&self) -> (u64, u32, u32, u64, u32) {
        let transport_order = if self.transaction_index.is_none() && self.log_index.is_none() {
            self.source_sequence.unwrap_or(0)
        } else {
            0
        };
        (
            self.block_number,
            self.transaction_index.unwrap_or(0),
            self.log_index.unwrap_or(0),
            transport_order,
            self.source_sub_index.unwrap_or(0),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Normalized EVM contract log independent of its transport.
pub struct ContractLog {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
    pub removed: bool,
    pub cursor: ChainCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Complete normalized update vocabulary consumed by the reducer.
pub enum ChainUpdate {
    Head(ChainCursor),
    Log(ContractLog),
    Reorg {
        old_head: ChainCursor,
        new_head: ChainCursor,
    },
    Gap {
        cursor: Option<ChainCursor>,
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Address/topic filter applied before event decoding.
pub struct ContractFilter {
    pub address: Address,
    pub topics: Vec<B256>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Inclusive canonical log range used during recovery and lane discovery.
pub struct BackfillRequest {
    pub from_block: u64,
    pub to_block: u64,
    pub filter: ContractFilter,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Transport or continuity failure.
pub enum SourceError {
    #[error("source network mismatch")]
    NetworkMismatch,
    #[error("source gap: {0}")]
    Gap(String),
    #[error("source unavailable: {0}")]
    Unavailable(String),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// ABI-shape failure while decoding a quote-critical Core event.
pub enum LogDecodeError {
    #[error("event log has no topic0")]
    MissingTopic0,
    #[error("event log has an invalid topic count")]
    InvalidTopicCount,
    #[error("event log has invalid ABI data length")]
    InvalidDataLength,
    #[error("event log contains an invalid address topic")]
    InvalidAddress,
    #[error("event log contains an invalid boolean")]
    InvalidBoolean,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Deployment identity and bootstrap configuration.
pub struct DeploymentConfig {
    pub network: Network,
    pub chain_id: u64,
    pub core: Address,
    pub router: Address,
    pub expect_whitelisted: bool,
    pub deployment_block: u64,
    pub expected_runtime_code_hash: B256,
    pub contract_compatibility_version: String,
    pub http_rpc_url: String,
    pub realtime_source: String,
    pub explicit_lane_assets: Vec<Address>,
}

impl DeploymentConfig {
    /// Validates local invariants before any network task is started.
    pub fn validate(&self) -> Result<(), SourceError> {
        if self.chain_id == 0 {
            return Err(SourceError::Unavailable("invalid chain id".into()));
        }
        if self.core == Address::ZERO || self.router == Address::ZERO {
            return Err(SourceError::Unavailable(
                "Core and configured router must be non-zero".into(),
            ));
        }
        if self.http_rpc_url.is_empty()
            || self.realtime_source.is_empty()
            || self.contract_compatibility_version.is_empty()
        {
            return Err(SourceError::Unavailable(
                "RPC, realtime source, and compatibility version are required".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Versioned, deployment-bound state used only for bootstrap acceleration.
pub struct Checkpoint {
    pub schema_version: u16,
    pub math_compatibility_version: String,
    pub expected_runtime_code_hash: B256,
    pub chain_id: u64,
    pub core: Address,
    pub router: Address,
    pub cursor: ChainCursor,
    pub state: QuoteState,
}

impl Checkpoint {
    /// Checks local schema and deployment identity before an RPC canonicality
    /// query is attempted.
    pub fn is_compatible(&self, deployment: &DeploymentConfig) -> bool {
        self.schema_version == SCHEMA_VERSION
            && self.math_compatibility_version == MATH_COMPATIBILITY_VERSION
            && self.expected_runtime_code_hash == deployment.expected_runtime_code_hash
            && self.chain_id == deployment.chain_id
            && self.core == deployment.core
            && self.router == deployment.router
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Decoded quote-critical Core transition.
pub enum QuoteEvent {
    LaneAdded {
        asset: Address,
    },
    LaneRemoved {
        asset: Address,
    },
    LaneUpdated {
        asset: Address,
        slot0: U256,
    },
    SlippageKSet {
        asset: Address,
        new_k: U256,
    },
    PartnerInfoSet {
        router: Address,
        asset: Address,
        fee: U256,
    },
    PartnerFeeSet {
        router: Address,
        asset: Address,
        fee: U256,
    },
    WhitelistSet {
        router: Address,
        whitelisted: bool,
    },
    BlacklistFeeMultiplierSet {
        multiplier: U256,
    },
    DepositExecuted {
        asset: Address,
        principal: U256,
    },
    WithdrawalExecuted {
        asset: Address,
        principal: U256,
    },
}
