use lunarbase_math::{Address, QuoteState, U256};
use serde::{Deserialize, Serialize};
use thiserror::Error;
pub const SCHEMA_VERSION: u16 = 2;
pub const MATH_COMPATIBILITY_VERSION: &str =
    "lunarbase-contracts@24db47b866e8150a0d91cffd80efe49df85179b5:math-v1";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Network {
    Base,
    Monad,
    Arbitrum,
}

impl Network {
    pub const fn default_chain_id(self) -> u64 {
        match self {
            Self::Base => 8453,
            Self::Monad => 143,
            Self::Arbitrum => 42161,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Commitment {
    Realtime,
    Canonical,
    Finalized,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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
pub struct ContractLog {
    pub address: Address,
    pub topics: Vec<U256>,
    pub data: Vec<u8>,
    pub removed: bool,
    pub cursor: ChainCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
pub struct ContractFilter {
    pub address: Address,
    pub topics: Vec<U256>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillRequest {
    pub from_block: u64,
    pub to_block: u64,
    pub filter: ContractFilter,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SourceError {
    #[error("source network mismatch")]
    NetworkMismatch,
    #[error("source gap: {0}")]
    Gap(String),
    #[error("source unavailable: {0}")]
    Unavailable(String),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
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
pub struct RedisConfig {
    pub url: String,
    pub stream_max_len: usize,
    pub dedup_ttl_seconds: u64,
    pub checkpoint_interval_updates: usize,
}

impl Default for RedisConfig {
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
    pub fn namespace(&self) -> RedisNamespace {
        RedisNamespace::new(self.chain_id, self.core)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisNamespace {
    pub tag: String,
    pub meta: String,
    pub state: String,
    pub checkpoint: String,
    pub updates: String,
    pub writer_lease: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RedisMeta {
    pub schema_version: u16,
    pub math_compatibility_version: String,
    pub expected_runtime_code_hash: [u8; 32],
}

impl RedisNamespace {
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
pub struct Checkpoint {
    pub schema_version: u16,
    pub math_compatibility_version: String,
    pub expected_runtime_code_hash: [u8; 32],
    pub cursor: ChainCursor,
    pub state: QuoteState,
}
/// reducer, so decoding can be parallelized and reduction stays single-writer.
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
