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
    /// Base, including Flashblocks-compatible realtime transports.
    Base,
    /// Monad, including execution-event and portable WebSocket transports.
    Monad,
    /// Arbitrum, including Nitro execution-aware transports.
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
    /// Low-latency provider state that may still be replaced or reordered.
    Realtime,
    /// State confirmed against the canonical executed chain.
    Canonical,
    /// Canonical state that the provider reports as finalized.
    Finalized,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
/// Ordered provider position plus the EVM-visible execution block.
///
/// `source_sequence` is used only to order provider messages. Network-specific
/// parent heights belong in `execution_block_number`, so math never interprets
/// a transport sequence as EVM semantics.
pub struct ChainCursor {
    /// EIP-155 chain identifier used to reject cross-network updates.
    pub chain_id: u64,
    /// Monotonic provider block position used for stream ordering and recovery.
    pub block_number: u64,
    /// EVM-visible block number supplied to block-dependent quote math.
    pub execution_block_number: u64,
    /// Canonical or provisional hash of `block_number`, when supplied by the source.
    pub block_hash: Option<B256>,
    /// Transaction position for a log-level cursor.
    pub transaction_index: Option<u32>,
    /// Log position within the block for deterministic event ordering.
    pub log_index: Option<u32>,
    /// Transport-local sequence used only when transaction ordering is unavailable.
    pub source_sequence: Option<u64>,
    /// Position of an update inside one transport message or sequence.
    pub source_sub_index: Option<u32>,
    /// Confidence level attached to this observed chain position.
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
    /// Contract that emitted the log.
    pub address: Address,
    /// Indexed ABI topics, with the event signature at index zero.
    pub topics: Vec<B256>,
    /// Unindexed ABI-encoded event payload.
    pub data: Bytes,
    /// Whether the provider retracted this log during a reorganization.
    pub removed: bool,
    /// Fully normalized chain and event position.
    pub cursor: ChainCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Complete normalized update vocabulary consumed by the reducer.
pub enum ChainUpdate {
    /// Advances chain position without changing quote-critical contract state.
    Head(ChainCursor),
    /// Applies one contract log at its normalized event position.
    Log(ContractLog),
    /// Signals that the previous head was replaced by another branch.
    Reorg {
        /// Last head observed on the abandoned branch.
        old_head: ChainCursor,
        /// First known head on the replacement branch.
        new_head: ChainCursor,
    },
    /// Signals missing or unordered source data that requires canonical recovery.
    Gap {
        /// Last trustworthy or first affected source position, when known.
        cursor: Option<ChainCursor>,
        /// Human-readable discontinuity reported by the adapter.
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Address/topic filter applied before event decoding.
pub struct ContractFilter {
    /// Contract address accepted by the source.
    pub address: Address,
    /// Allowed event signature topics; an empty list accepts every topic zero.
    pub topics: Vec<B256>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Inclusive canonical log range used during recovery and lane discovery.
pub struct BackfillRequest {
    /// First canonical block included in the query.
    pub from_block: u64,
    /// Last canonical block included in the query.
    pub to_block: u64,
    /// Contract and event topics to retrieve.
    pub filter: ContractFilter,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Transport or continuity failure.
pub enum SourceError {
    /// The adapter belongs to a different network family than the deployment.
    #[error("source network mismatch")]
    NetworkMismatch,
    /// The source detected missing or non-contiguous updates.
    #[error("source gap: {0}")]
    Gap(String),
    /// The source could not perform the requested operation.
    #[error("source unavailable: {0}")]
    Unavailable(String),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// ABI-shape failure while decoding a quote-critical Core event.
pub enum LogDecodeError {
    /// The log does not contain an event signature topic.
    #[error("event log has no topic0")]
    MissingTopic0,
    /// The number of indexed topics differs from the pinned contract ABI.
    #[error("event log has an invalid topic count")]
    InvalidTopicCount,
    /// The unindexed payload is not a valid sequence of expected ABI words.
    #[error("event log has invalid ABI data length")]
    InvalidDataLength,
    /// An indexed ABI word is not a canonically padded EVM address.
    #[error("event log contains an invalid address topic")]
    InvalidAddress,
    /// An ABI word intended as a boolean is neither zero nor one.
    #[error("event log contains an invalid boolean")]
    InvalidBoolean,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Deployment identity and bootstrap configuration.
pub struct DeploymentConfig {
    /// Network-specific adapter family required by this deployment.
    pub network: Network,
    /// EIP-155 chain identifier expected from every source cursor.
    pub chain_id: u64,
    /// LunarBase Core contract whose quote-critical state is indexed.
    pub core: Address,
    /// Single router whose whitelist and partner-fee profile is tracked.
    pub router: Address,
    /// Whether bootstrap must reject a router that is not whitelisted.
    pub expect_whitelisted: bool,
    /// First block that can contain relevant deployment logs.
    pub deployment_block: u64,
    /// Pinned hash of the Core runtime bytecode used for compatibility validation.
    pub expected_runtime_code_hash: B256,
    /// Human-readable contracts revision expected by the client package.
    pub contract_compatibility_version: String,
    /// HTTP JSON-RPC endpoint used only for bootstrap and canonical recovery.
    pub http_rpc_url: String,
    /// Adapter-specific realtime endpoint or native event-ring locator.
    pub realtime_source: String,
    /// Optional fixed lane assets that avoid discovery scans during bootstrap.
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
        if self.expected_runtime_code_hash == B256::ZERO {
            return Err(SourceError::Unavailable(
                "expected Core runtime code hash must be non-zero".into(),
            ));
        }
        if self.contract_compatibility_version != MATH_COMPATIBILITY_VERSION {
            return Err(SourceError::Unavailable(format!(
                "contract compatibility mismatch: expected {MATH_COMPATIBILITY_VERSION}"
            )));
        }
        if self.http_rpc_url.is_empty() || self.realtime_source.is_empty() {
            return Err(SourceError::Unavailable(
                "RPC and realtime source are required".into(),
            ));
        }
        let mut lanes = std::collections::HashSet::with_capacity(self.explicit_lane_assets.len());
        for asset in &self.explicit_lane_assets {
            if *asset == Address::ZERO {
                return Err(SourceError::Unavailable(
                    "explicit lane assets must be non-zero".into(),
                ));
            }
            if !lanes.insert(*asset) {
                return Err(SourceError::Unavailable(
                    "explicit lane assets must be unique".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Versioned, deployment-bound state used only for bootstrap acceleration.
pub struct Checkpoint {
    /// Persistence schema version; incompatible versions are discarded.
    pub schema_version: u16,
    /// Exact pure-math compatibility identifier used to create the state.
    pub math_compatibility_version: String,
    /// Core runtime bytecode hash verified before this checkpoint was created.
    pub expected_runtime_code_hash: B256,
    /// EIP-155 chain identifier that owns the checkpoint.
    pub chain_id: u64,
    /// Core contract whose state is serialized.
    pub core: Address,
    /// Configured router whose fee profile is embedded in the state.
    pub router: Address,
    /// Last fully applied and verified source position.
    pub cursor: ChainCursor,
    /// Complete in-memory quote state at `cursor`.
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
    /// Marks a newly configured asset lane as available.
    LaneAdded {
        /// ERC-20 asset identifying the lane.
        asset: Address,
    },
    /// Removes an asset lane from quote state.
    LaneRemoved {
        /// ERC-20 asset identifying the lane.
        asset: Address,
    },
    /// Replaces the packed lane storage word.
    LaneUpdated {
        /// ERC-20 asset identifying the lane.
        asset: Address,
        /// Raw packed `Lane.slot0` word emitted by Core.
        slot0: U256,
    },
    /// Changes the lane-specific slippage coefficient.
    SlippageKSet {
        /// ERC-20 asset identifying the lane.
        asset: Address,
        /// New basis-point coefficient before storage-width validation.
        new_k: U256,
    },
    /// Initializes or replaces configured-router partner information.
    PartnerInfoSet {
        /// Router whose fee profile changed.
        router: Address,
        /// Asset lane to which the partner fee applies.
        asset: Address,
        /// New partner fee in contract basis-point units.
        fee: U256,
    },
    /// Changes the configured router's fee for one asset.
    PartnerFeeSet {
        /// Router whose fee profile changed.
        router: Address,
        /// Asset lane to which the partner fee applies.
        asset: Address,
        /// New partner fee in contract basis-point units.
        fee: U256,
    },
    /// Changes whether a router bypasses the global blacklist multiplier.
    WhitelistSet {
        /// Router whose whitelist status changed.
        router: Address,
        /// New whitelist status.
        whitelisted: bool,
    },
    /// Replaces the global fee multiplier used by non-whitelisted routers.
    BlacklistFeeMultiplierSet {
        /// New multiplier before storage-width validation.
        multiplier: U256,
    },
    /// Increases quoteable principal for an asset lane.
    DepositExecuted {
        /// ERC-20 asset identifying the lane.
        asset: Address,
        /// Principal added to the lane.
        principal: U256,
    },
    /// Decreases quoteable principal for an asset lane.
    WithdrawalExecuted {
        /// ERC-20 asset identifying the lane.
        asset: Address,
        /// Principal removed from the lane.
        principal: U256,
    },
}
