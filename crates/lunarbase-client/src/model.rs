//! Provider-independent runtime model shared by every network source.

use lunarbase_math::state::QuoteState;
use lunarbase_math::types::{Address, B256, Bytes, U256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Current durable checkpoint schema.
pub const SCHEMA_VERSION: u16 = 4;

/// Pinned Solidity implementation used by both pure math packages.
pub const MATH_COMPATIBILITY_VERSION: &str =
    "lunarbase-contracts@4bbf4d4666ac29412d7fbd946fd7a0fba8f9ac6d:math-v4";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
/// Supported chain families.
pub enum Network {
    /// Standard EVM JSON-RPC using canonical `logs + newHeads` subscriptions.
    Evm,
    /// Base, including Flashblocks-compatible realtime transports.
    Base,
    /// Monad, including execution-event and portable WebSocket transports.
    Monad,
    /// Arbitrum, including Nitro execution-aware transports.
    Arbitrum,
}

impl Network {
    /// Returns the default mainnet chain id for a chain-specific source family.
    ///
    /// Generic EVM sources have no single default chain and therefore return
    /// `None`; their EIP-155 chain id must always be supplied explicitly.
    pub const fn default_chain_id(self) -> Option<u64> {
        match self {
            Self::Evm => None,
            Self::Base => Some(8453),
            Self::Monad => Some(143),
            Self::Arbitrum => Some(42161),
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

    /// Returns the deterministic transport/event ordering tuple.
    ///
    /// Source drivers use this key to sort canonical backfills before handing
    /// them to the common reducer. `source_sequence` is considered only when
    /// transaction and log coordinates are unavailable.
    pub fn event_order(&self) -> (u64, u32, u32, u64, u32) {
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
        /// Human-readable discontinuity reported by the source.
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
    /// The source belongs to a different network family than the deployment.
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
    /// Network source family required by this deployment.
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
    /// Pinned ERC-1967 implementation behind the Core proxy.
    pub expected_implementation: Address,
    /// Pinned runtime bytecode hash of `expected_implementation`.
    pub expected_implementation_code_hash: B256,
    /// Human-readable contracts revision expected by the client package.
    pub contract_compatibility_version: String,
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
        if self.expected_implementation == Address::ZERO {
            return Err(SourceError::Unavailable(
                "expected Core implementation must be non-zero".into(),
            ));
        }
        if self.expected_implementation_code_hash == B256::ZERO {
            return Err(SourceError::Unavailable(
                "expected Core implementation code hash must be non-zero".into(),
            ));
        }
        if self.contract_compatibility_version != MATH_COMPATIBILITY_VERSION {
            return Err(SourceError::Unavailable(format!(
                "contract compatibility mismatch: expected {MATH_COMPATIBILITY_VERSION}"
            )));
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
    /// ERC-1967 implementation verified before this checkpoint was created.
    pub expected_implementation: Address,
    /// Runtime bytecode hash of the verified implementation.
    pub expected_implementation_code_hash: B256,
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
            && self.expected_implementation == deployment.expected_implementation
            && self.expected_implementation_code_hash
                == deployment.expected_implementation_code_hash
            && self.chain_id == deployment.chain_id
            && self.core == deployment.core
            && self.router == deployment.router
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Decoded quote-critical Core transition.
pub enum QuoteEvent {
    /// Adds a paused asset lane with threshold checks enabled.
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
        /// New basis-point coefficient decoded at its Solidity width.
        new_k: u32,
    },
    /// Changes whether swaps through one lane are paused.
    LanePausedSet {
        /// ERC-20 asset identifying the lane.
        asset: Address,
        /// New lane pause state emitted by Core.
        paused: bool,
    },
    /// Changes one lane's price-push threshold policy.
    PricePushThresholdSet {
        /// ERC-20 asset identifying the lane.
        asset: Address,
        /// New seven-bit percentage threshold.
        price_push_threshold: u8,
        /// Whether the threshold is enforced for later operator updates.
        enabled: bool,
    },
    /// Changes the inclusive quote TTL after a lane update.
    BlockDelaySet {
        /// ERC-20 asset identifying the lane.
        asset: Address,
        /// New quote TTL decoded at its Solidity width.
        block_delay: u8,
    },
    /// Initializes or replaces configured-router partner information.
    PartnerInfoSet {
        /// Router whose fee profile changed.
        router: Address,
        /// Asset lane to which the partner fee applies.
        asset: Address,
        /// New partner fee in contract basis-point units.
        fee: u32,
    },
    /// Changes the configured router's fee for one asset.
    PartnerFeeSet {
        /// Router whose fee profile changed.
        router: Address,
        /// Asset lane to which the partner fee applies.
        asset: Address,
        /// New partner fee in contract basis-point units.
        fee: u32,
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
        principal: u128,
    },
    /// Decreases quoteable principal for an asset lane.
    WithdrawalExecuted {
        /// ERC-20 asset identifying the lane.
        asset: Address,
        /// Principal removed from the lane.
        principal: u128,
    },
    /// Replaces free reserves after a settlement or liquidity transition.
    Sync {
        /// Lane whose asset reserve changed; may equal the configured cash asset.
        asset: Address,
        /// Free reserve of `asset` after liabilities.
        asset_reserve: u128,
        /// Free cash reserve after liabilities.
        cash_reserve: u128,
    },
    /// Signals a proxy implementation change that requires compatibility recovery.
    ImplementationUpgraded {
        /// Newly installed implementation reported by ERC-1967.
        implementation: Address,
    },
}
