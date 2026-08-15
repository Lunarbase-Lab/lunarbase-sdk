//! Redis checkpoint persistence for faster restarts.

use lunarbase_client::model::{ChainCursor, Checkpoint, Commitment, DeploymentConfig, Network};
use lunarbase_math::{Address, B256, U256};
use lunarbase_math::{LaneState, QuoteState};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::Duration,
};
use thiserror::Error;

const REDIS_TIMEOUT: Duration = Duration::from_secs(2);
const STORE_CHECKPOINT_LUA: &str = r#"
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
local separator = string.find(current, '\n', 1, true)
if not separator then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
local current_order = string.sub(current, 1, separator - 1)
if ARGV[2] > current_order then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
if ARGV[2] == current_order and current == ARGV[1] then
  return 2
end
return 0
"#;

#[derive(Clone, Debug)]
/// One-key Redis store. `SET` is atomic and the key has no TTL.
pub struct RedisCheckpointStore {
    /// Redis connection URL used only by blocking checkpoint workers.
    url: String,
    /// Deployment- and schema-specific key containing the full checkpoint DTO.
    key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Outcome of one atomic monotonic checkpoint write.
pub enum StoreOutcome {
    /// A strictly newer checkpoint replaced the stored value.
    Stored,
    /// The exact checkpoint was already persisted at this cursor.
    Unchanged,
    /// A newer or conflicting same-position checkpoint already exists.
    Stale,
}

#[derive(Debug, Error)]
/// Redis transport or checkpoint DTO failure.
pub enum CheckpointError {
    /// Redis connection or command execution failed.
    #[error("Redis: {0}")]
    Redis(String),
    /// The versioned checkpoint value is not valid JSON.
    #[error("checkpoint JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A decoded DTO contains an invalid EVM primitive or incompatible value.
    #[error("checkpoint DTO: {0}")]
    Invalid(String),
    /// The blocking Redis worker panicked or was cancelled.
    #[error("checkpoint worker: {0}")]
    Worker(String),
}

impl RedisCheckpointStore {
    /// Creates the current deployment-specific key.
    pub fn new(url: impl Into<String>, deployment: &DeploymentConfig) -> Self {
        let key = format!(
            "lunarbase:v6:{}:{:#x}",
            deployment.chain_id, deployment.core
        );
        Self {
            url: url.into(),
            key,
        }
    }

    /// Loads and decodes the only checkpoint value.
    pub async fn load(&self) -> Result<Option<Checkpoint>, CheckpointError> {
        let url = self.url.clone();
        let key = self.key.clone();
        let payload = tokio::task::spawn_blocking(move || {
            let client = redis::Client::open(url).map_err(redis_error)?;
            let mut connection = client
                .get_connection_with_timeout(REDIS_TIMEOUT)
                .map_err(redis_error)?;
            configure_connection(&connection)?;
            redis::cmd("GET")
                .arg(key)
                .query::<Option<Vec<u8>>>(&mut connection)
                .map_err(redis_error)
        })
        .await
        .map_err(|error| CheckpointError::Worker(error.to_string()))??;
        payload
            .map(|bytes| {
                serde_json::from_slice::<CheckpointDto>(checkpoint_json(&bytes)?)?.try_into()
            })
            .transpose()
    }

    /// Atomically stores only a strictly newer or identical checkpoint.
    pub async fn store(&self, checkpoint: &Checkpoint) -> Result<StoreOutcome, CheckpointError> {
        let json = serde_json::to_vec(&CheckpointDto::from(checkpoint))?;
        let order = checkpoint_order(&checkpoint.cursor);
        let mut payload = Vec::with_capacity(order.len() + 1 + json.len());
        payload.extend_from_slice(order.as_bytes());
        payload.push(b'\n');
        payload.extend_from_slice(&json);
        let url = self.url.clone();
        let key = self.key.clone();
        tokio::task::spawn_blocking(move || {
            let client = redis::Client::open(url).map_err(redis_error)?;
            let mut connection = client
                .get_connection_with_timeout(REDIS_TIMEOUT)
                .map_err(redis_error)?;
            configure_connection(&connection)?;
            let result = redis::cmd("EVAL")
                .arg(STORE_CHECKPOINT_LUA)
                .arg(1)
                .arg(key)
                .arg(payload)
                .arg(order)
                .query::<u8>(&mut connection)
                .map_err(redis_error)?;
            match result {
                1 => Ok(StoreOutcome::Stored),
                2 => Ok(StoreOutcome::Unchanged),
                0 => Ok(StoreOutcome::Stale),
                value => Err(CheckpointError::Redis(format!(
                    "unexpected checkpoint CAS result {value}"
                ))),
            }
        })
        .await
        .map_err(|error| CheckpointError::Worker(error.to_string()))?
    }
}

fn checkpoint_json(payload: &[u8]) -> Result<&[u8], CheckpointError> {
    if payload.first() == Some(&b'{') {
        return Ok(payload);
    }
    let separator = payload
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or_else(|| {
            CheckpointError::Invalid("stored checkpoint has no cursor-order separator".into())
        })?;
    let json = &payload[separator + 1..];
    if json.is_empty() {
        return Err(CheckpointError::Invalid(
            "stored checkpoint payload is empty".into(),
        ));
    }
    Ok(json)
}

fn checkpoint_order(cursor: &ChainCursor) -> String {
    let (block, transaction, log, sequence, sub_index) = cursor.event_order();
    let position_kind = u8::from(cursor.transaction_index.is_some() || cursor.log_index.is_some());
    let commitment = match cursor.commitment {
        Commitment::Realtime => 0,
        Commitment::Canonical => 1,
        Commitment::Finalized => 2,
    };
    format!(
        "{block:020}:{:020}:{position_kind}:{transaction:010}:{log:010}:{sequence:020}:{sub_index:010}:{commitment}",
        cursor.execution_block_number,
    )
}

fn configure_connection(connection: &redis::Connection) -> Result<(), CheckpointError> {
    connection
        .set_read_timeout(Some(REDIS_TIMEOUT))
        .map_err(redis_error)?;
    connection
        .set_write_timeout(Some(REDIS_TIMEOUT))
        .map_err(redis_error)
}

fn redis_error(error: redis::RedisError) -> CheckpointError {
    CheckpointError::Redis(error.to_string())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointDto {
    schema_version: u16,
    math_compatibility_version: String,
    expected_implementation: String,
    expected_implementation_code_hash: String,
    chain_id: u64,
    network: Network,
    core: String,
    deployment_block: u64,
    explicit_lane_assets: Vec<String>,
    cursor: CursorDto,
    state: StateDto,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CursorDto {
    chain_id: u64,
    block_number: u64,
    execution_block_number: u64,
    block_hash: Option<String>,
    transaction_index: Option<u32>,
    log_index: Option<u32>,
    source_sequence: Option<u64>,
    source_sub_index: Option<u32>,
    commitment: CommitmentDto,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum CommitmentDto {
    Realtime,
    Canonical,
    Finalized,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StateDto {
    cash: String,
    cash_reserve: u128,
    lanes: Vec<LaneDto>,
    blacklist_fee_multiplier: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LaneDto {
    asset: String,
    slot0: String,
    asset_reserve: u128,
    total_principal_amount: u128,
}

impl From<&Checkpoint> for CheckpointDto {
    fn from(checkpoint: &Checkpoint) -> Self {
        let mut lanes = checkpoint
            .state
            .lanes
            .iter()
            .map(|(asset, lane)| LaneDto {
                asset: address_hex(*asset),
                slot0: lane.slot0.to_string(),
                asset_reserve: lane.asset_reserve,
                total_principal_amount: lane.total_principal_amount,
            })
            .collect::<Vec<_>>();
        lanes.sort_by(|left, right| left.asset.cmp(&right.asset));
        let mut explicit_lane_assets = checkpoint
            .explicit_lane_assets
            .iter()
            .copied()
            .map(address_hex)
            .collect::<Vec<_>>();
        explicit_lane_assets.sort();
        Self {
            schema_version: checkpoint.schema_version,
            math_compatibility_version: checkpoint.math_compatibility_version.clone(),
            expected_implementation: address_hex(checkpoint.expected_implementation),
            expected_implementation_code_hash: hash_hex(
                checkpoint.expected_implementation_code_hash,
            ),
            chain_id: checkpoint.chain_id,
            network: checkpoint.network,
            core: address_hex(checkpoint.core),
            deployment_block: checkpoint.deployment_block,
            explicit_lane_assets,
            cursor: CursorDto::from(&checkpoint.cursor),
            state: StateDto {
                cash: address_hex(checkpoint.state.cash),
                cash_reserve: checkpoint.state.cash_reserve,
                lanes,
                blacklist_fee_multiplier: checkpoint.state.blacklist_fee_multiplier.to_string(),
            },
        }
    }
}

impl TryFrom<CheckpointDto> for Checkpoint {
    type Error = CheckpointError;

    fn try_from(dto: CheckpointDto) -> Result<Self, Self::Error> {
        let CheckpointDto {
            schema_version,
            math_compatibility_version,
            expected_implementation,
            expected_implementation_code_hash,
            chain_id,
            network,
            core,
            deployment_block,
            explicit_lane_assets,
            cursor,
            state,
        } = dto;
        let StateDto {
            cash,
            cash_reserve,
            lanes: lane_values,
            blacklist_fee_multiplier,
        } = state;

        let mut lanes = HashMap::with_capacity(lane_values.len());
        for lane in lane_values {
            let asset = parse_address(&lane.asset)?;
            if lanes
                .insert(
                    asset,
                    LaneState::new(
                        parse_u256(&lane.slot0)?,
                        lane.asset_reserve,
                        lane.total_principal_amount,
                    ),
                )
                .is_some()
            {
                return Err(CheckpointError::Invalid("duplicate lane asset".into()));
            }
        }

        let mut parsed_explicit_lanes = Vec::with_capacity(explicit_lane_assets.len());
        let mut unique_explicit_lanes = HashSet::with_capacity(explicit_lane_assets.len());
        for value in explicit_lane_assets {
            let asset = parse_address(&value)?;
            if !unique_explicit_lanes.insert(asset) {
                return Err(CheckpointError::Invalid(
                    "duplicate explicit lane asset".into(),
                ));
            }
            parsed_explicit_lanes.push(asset);
        }

        let checkpoint = Checkpoint {
            schema_version,
            math_compatibility_version,
            expected_implementation: parse_address(&expected_implementation)?,
            expected_implementation_code_hash: parse_hash(&expected_implementation_code_hash)?,
            chain_id,
            network,
            core: parse_address(&core)?,
            deployment_block,
            explicit_lane_assets: parsed_explicit_lanes,
            cursor: cursor.try_into()?,
            state: QuoteState {
                cash: parse_address(&cash)?,
                cash_reserve,
                lanes,
                blacklist_fee_multiplier: parse_u256(&blacklist_fee_multiplier)?,
            },
        };
        if !checkpoint.has_valid_structure() {
            return Err(CheckpointError::Invalid(
                "checkpoint violates structural state invariants".into(),
            ));
        }
        Ok(checkpoint)
    }
}

impl From<&ChainCursor> for CursorDto {
    fn from(cursor: &ChainCursor) -> Self {
        Self {
            chain_id: cursor.chain_id,
            block_number: cursor.block_number,
            execution_block_number: cursor.execution_block_number,
            block_hash: cursor.block_hash.map(hash_hex),
            transaction_index: cursor.transaction_index,
            log_index: cursor.log_index,
            source_sequence: cursor.source_sequence,
            source_sub_index: cursor.source_sub_index,
            commitment: match cursor.commitment {
                Commitment::Realtime => CommitmentDto::Realtime,
                Commitment::Canonical => CommitmentDto::Canonical,
                Commitment::Finalized => CommitmentDto::Finalized,
            },
        }
    }
}

impl TryFrom<CursorDto> for ChainCursor {
    type Error = CheckpointError;

    fn try_from(cursor: CursorDto) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: cursor.chain_id,
            block_number: cursor.block_number,
            execution_block_number: cursor.execution_block_number,
            block_hash: cursor.block_hash.as_deref().map(parse_hash).transpose()?,
            transaction_index: cursor.transaction_index,
            log_index: cursor.log_index,
            source_sequence: cursor.source_sequence,
            source_sub_index: cursor.source_sub_index,
            commitment: match cursor.commitment {
                CommitmentDto::Realtime => Commitment::Realtime,
                CommitmentDto::Canonical => Commitment::Canonical,
                CommitmentDto::Finalized => Commitment::Finalized,
            },
        })
    }
}

fn parse_address(value: &str) -> Result<Address, CheckpointError> {
    Address::from_str(value).map_err(|error| CheckpointError::Invalid(error.to_string()))
}

fn address_hex(value: Address) -> String {
    format!("{value:#x}")
}

fn parse_u256(value: &str) -> Result<U256, CheckpointError> {
    U256::from_str(value).map_err(|error| CheckpointError::Invalid(error.to_string()))
}

fn parse_hash(value: &str) -> Result<B256, CheckpointError> {
    B256::from_str(value).map_err(|error| CheckpointError::Invalid(error.to_string()))
}

fn hash_hex(value: B256) -> String {
    format!("{value:#x}")
}

#[cfg(test)]
#[path = "checkpoint_tests.rs"]
mod tests;
