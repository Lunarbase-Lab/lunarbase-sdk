//! Stable core domain model shared by transports, persistence, and quoting.
//!
//! Types in this module describe chain cursors, normalized updates, deployment
//! identity, and checkpoint metadata. They contain no provider-specific wire
//! parsing and are re-exported from the crate root for API compatibility.

use lunarbase_math::{Address, QuoteState, U256};
use serde::{Deserialize, Serialize};
use thiserror::Error;
pub const SCHEMA_VERSION: u16 = 2;
/// Compatibility string shared by checkpoints and both quote implementations.
pub const MATH_COMPATIBILITY_VERSION: &str =
    "lunarbase-contracts@24db47b866e8150a0d91cffd80efe49df85179b5:math-v1";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
/// Supported chain families. Provider-specific details stay behind the source
/// boundary and never enter pure quote math.
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
/// Ordered position of a block, log, and provider-specific source sequence.
pub struct ChainCursor {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: Option<[u8; 32]>,
    pub transaction_index: Option<u32>,
    pub log_index: Option<u32>,
    pub source_sequence: Option<u64>,
    pub source_sub_index: Option<u32>,
    pub commitment: Commitment,
}

impl ChainCursor {
    /// Creates a block-level cursor without transaction or log coordinates.
    ///
    /// Such a cursor is used for heads and snapshot boundaries; the reducer
    /// treats it as the end-of-block watermark when ordering events.
    pub fn block(
        chain_id: u64,
        block_number: u64,
        block_hash: Option<[u8; 32]>,
        commitment: Commitment,
    ) -> Self {
        Self {
            chain_id,
            block_number,
            block_hash,
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment,
        }
    }

    pub(crate) fn event_order(&self) -> (u64, u32, u32) {
        (
            self.block_number,
            self.transaction_index.unwrap_or(0),
            self.log_index.unwrap_or(0),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Normalized EVM contract log independent of the transport that produced it.
pub struct ContractLog {
    pub address: Address,
    pub topics: Vec<U256>,
    pub data: Vec<u8>,
    pub removed: bool,
    pub cursor: ChainCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// All normalized source messages consumed by the ordered reducer.
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
    SourceHealth {
        healthy: bool,
        detail: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Address/topic filter applied before allocation-heavy event decoding.
pub struct ContractFilter {
    pub address: Address,
    pub topics: Vec<U256>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Inclusive canonical log range used during recovery and bootstrap discovery.
pub struct BackfillRequest {
    pub from_block: u64,
    pub to_block: u64,
    pub filter: ContractFilter,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Transport or continuity failure. A gap must stop freshness claims until
/// canonical recovery succeeds.
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
/// Redis stream/checkpoint bounds and deduplication policy.
pub struct RedisConfig {
    pub url: String,
    pub stream_max_len: usize,
    pub dedup_ttl_seconds: u64,
    pub checkpoint_interval_updates: usize,
}

impl Default for RedisConfig {
    /// Provides conservative local-development bounds.
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1/".into(),
            stream_max_len: 10_000,
            dedup_ttl_seconds: 86_400,
            checkpoint_interval_updates: 100,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Deployment identity and all external source/storage configuration.
pub struct DeploymentConfig {
    pub network: Network,
    pub chain_id: u64,
    pub core: Address,
    pub deployment_block: u64,
    pub expected_runtime_code_hash: [u8; 32],
    pub contract_compatibility_version: String,
    pub http_rpc_url: String,
    pub realtime_source: String,
    pub redis: RedisConfig,
    pub explicit_lane_assets: Vec<Address>,
    pub eager_routers: Vec<Address>,
}

impl DeploymentConfig {
    /// Validates mandatory identity fields and bounded Redis settings.
    ///
    /// This does not contact RPC or Redis; it only rejects configurations that
    /// could make readiness or persistence semantics ambiguous.
    pub fn validate(&self) -> Result<(), SourceError> {
        if self.chain_id == 0 {
            return Err(SourceError::Unavailable("invalid chain id".into()));
        }
        if self.http_rpc_url.is_empty() || self.contract_compatibility_version.is_empty() {
            return Err(SourceError::Unavailable(
                "RPC URL and compatibility version are required".into(),
            ));
        }
        if self.redis.stream_max_len == 0 || self.redis.checkpoint_interval_updates == 0 {
            return Err(SourceError::Unavailable(
                "Redis stream and checkpoint bounds must be non-zero".into(),
            ));
        }
        Ok(())
    }
    /// Builds the cluster-safe Redis namespace for this deployment.
    pub fn namespace(&self) -> RedisNamespace {
        RedisNamespace::new(self.chain_id, self.core)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
/// Hash-tagged Redis keys belonging to one chain/Core deployment.
pub struct RedisNamespace {
    pub tag: String,
    pub meta: String,
    pub state: String,
    pub checkpoint: String,
    pub updates: String,
    pub writer_lease: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Compatibility metadata written alongside a durable checkpoint.
pub struct RedisMeta {
    pub schema_version: u16,
    pub math_compatibility_version: String,
    pub expected_runtime_code_hash: [u8; 32],
}

impl RedisNamespace {
    /// Constructs keys sharing `{chain_id:core}` so Redis Cluster scripts stay
    /// within one hash slot.
    pub fn new(chain_id: u64, core: Address) -> Self {
        let tag = format!("{chain_id}:{}", core.to_hex());
        Self {
            meta: format!("lb:{{{tag}}}:meta"),
            state: format!("lb:{{{tag}}}:state"),
            checkpoint: format!("lb:{{{tag}}}:checkpoint"),
            updates: format!("lb:{{{tag}}}:updates"),
            writer_lease: format!("lb:{{{tag}}}:writer-lease"),
            tag,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Durable state snapshot from which a reducer can resume safely.
pub struct Checkpoint {
    pub schema_version: u16,
    pub math_compatibility_version: String,
    pub expected_runtime_code_hash: [u8; 32],
    pub cursor: ChainCursor,
    pub state: QuoteState,
}
/// Quote-critical event decoded from a Core log. Unknown contract events are
/// intentionally omitted before this type is constructed, so decoding can be
/// parallelized while reduction stays single-writer.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    SwapExecuted,
}
