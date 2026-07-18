//! Best-effort v3 Redis checkpoint acceleration.

use lunarbase_client_core::{ChainCursor, Checkpoint, Commitment, DeploymentConfig};
use lunarbase_math::{Address, FeeProfile, LaneState, QuoteState, B256, U256};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr, time::Duration};
use thiserror::Error;

const REDIS_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
/// One-key Redis store. `SET` is atomic and the key has no TTL.
pub struct RedisCheckpointStore {
    url: String,
    key: String,
}

#[derive(Debug, Error)]
/// Redis transport or checkpoint DTO failure.
pub enum CheckpointError {
    #[error("Redis: {0}")]
    Redis(String),
    #[error("checkpoint JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("checkpoint DTO: {0}")]
    Invalid(String),
    #[error("checkpoint worker: {0}")]
    Worker(String),
}

impl RedisCheckpointStore {
    /// Creates the v3 deployment-specific key.
    pub fn new(url: impl Into<String>, deployment: &DeploymentConfig) -> Self {
        Self {
            url: url.into(),
            key: format!(
                "lunarbase:v3:{}:{:#x}:{:#x}",
                deployment.chain_id, deployment.core, deployment.router
            ),
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
            redis::cmd("GET")
                .arg(key)
                .query::<Option<Vec<u8>>>(&mut connection)
                .map_err(redis_error)
        })
        .await
        .map_err(|error| CheckpointError::Worker(error.to_string()))??;
        payload
            .map(|bytes| serde_json::from_slice::<CheckpointDto>(&bytes)?.try_into())
            .transpose()
    }

    /// Atomically replaces the full checkpoint without expiration.
    pub async fn store(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let payload = serde_json::to_vec(&CheckpointDto::from(checkpoint))?;
        let url = self.url.clone();
        let key = self.key.clone();
        tokio::task::spawn_blocking(move || {
            let client = redis::Client::open(url).map_err(redis_error)?;
            let mut connection = client
                .get_connection_with_timeout(REDIS_TIMEOUT)
                .map_err(redis_error)?;
            redis::cmd("SET")
                .arg(key)
                .arg(payload)
                .query::<()>(&mut connection)
                .map_err(redis_error)
        })
        .await
        .map_err(|error| CheckpointError::Worker(error.to_string()))?
    }
}

fn redis_error(error: redis::RedisError) -> CheckpointError {
    CheckpointError::Redis(error.to_string())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointDto {
    schema_version: u16,
    math_compatibility_version: String,
    expected_runtime_code_hash: String,
    chain_id: u64,
    core: String,
    router: String,
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
    lanes: Vec<LaneDto>,
    fee_profile: FeeProfileDto,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LaneDto {
    asset: String,
    slot0: String,
    total_principal_amount: u128,
    slippage_k_bps: u32,
    block_delay: u8,
    exists: bool,
    paused: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeeProfileDto {
    whitelisted: bool,
    blacklist_fee_multiplier: String,
    partner_fee_bps: Vec<PartnerFeeDto>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PartnerFeeDto {
    asset: String,
    fee_bps: u32,
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
                total_principal_amount: lane.total_principal_amount,
                slippage_k_bps: lane.slippage_k_bps,
                block_delay: lane.block_delay,
                exists: lane.exists(),
                paused: lane.paused(),
            })
            .collect::<Vec<_>>();
        lanes.sort_by(|left, right| left.asset.cmp(&right.asset));
        let mut partner_fee_bps = checkpoint
            .state
            .fee_profile
            .partner_fee_bps
            .iter()
            .map(|(asset, fee_bps)| PartnerFeeDto {
                asset: address_hex(*asset),
                fee_bps: *fee_bps,
            })
            .collect::<Vec<_>>();
        partner_fee_bps.sort_by(|left, right| left.asset.cmp(&right.asset));
        Self {
            schema_version: checkpoint.schema_version,
            math_compatibility_version: checkpoint.math_compatibility_version.clone(),
            expected_runtime_code_hash: hash_hex(checkpoint.expected_runtime_code_hash),
            chain_id: checkpoint.chain_id,
            core: address_hex(checkpoint.core),
            router: address_hex(checkpoint.router),
            cursor: CursorDto::from(&checkpoint.cursor),
            state: StateDto {
                cash: address_hex(checkpoint.state.cash),
                lanes,
                fee_profile: FeeProfileDto {
                    whitelisted: checkpoint.state.fee_profile.whitelisted,
                    blacklist_fee_multiplier: checkpoint
                        .state
                        .fee_profile
                        .blacklist_fee_multiplier
                        .to_string(),
                    partner_fee_bps,
                },
            },
        }
    }
}

impl TryFrom<CheckpointDto> for Checkpoint {
    type Error = CheckpointError;

    fn try_from(dto: CheckpointDto) -> Result<Self, Self::Error> {
        let lanes = dto
            .state
            .lanes
            .into_iter()
            .map(|lane| {
                Ok((
                    parse_address(&lane.asset)?,
                    LaneState::new(
                        parse_u256(&lane.slot0)?,
                        lane.total_principal_amount,
                        lane.slippage_k_bps,
                        lane.block_delay,
                        lane.exists,
                        lane.paused,
                    ),
                ))
            })
            .collect::<Result<HashMap<_, _>, CheckpointError>>()?;
        let partner_fee_bps = dto
            .state
            .fee_profile
            .partner_fee_bps
            .into_iter()
            .map(|fee| Ok((parse_address(&fee.asset)?, fee.fee_bps)))
            .collect::<Result<HashMap<_, _>, CheckpointError>>()?;
        Ok(Checkpoint {
            schema_version: dto.schema_version,
            math_compatibility_version: dto.math_compatibility_version,
            expected_runtime_code_hash: parse_hash(&dto.expected_runtime_code_hash)?,
            chain_id: dto.chain_id,
            core: parse_address(&dto.core)?,
            router: parse_address(&dto.router)?,
            cursor: dto.cursor.try_into()?,
            state: QuoteState {
                cash: parse_address(&dto.state.cash)?,
                lanes,
                fee_profile: FeeProfile {
                    whitelisted: dto.state.fee_profile.whitelisted,
                    blacklist_fee_multiplier: parse_u256(
                        &dto.state.fee_profile.blacklist_fee_multiplier,
                    )?,
                    partner_fee_bps,
                },
            },
        })
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
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 64 {
        return Err(CheckpointError::Invalid("hash is not 32 bytes".into()));
    }
    let mut output = [0u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| CheckpointError::Invalid("hash is not hexadecimal".into()))?;
    }
    Ok(B256::new(output))
}

fn hash_hex(value: B256) -> String {
    format!("{value:#x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use lunarbase_client_core::{Commitment, Network, MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION};

    fn address(suffix: u8) -> Address {
        let mut bytes = [0u8; 20];
        bytes[19] = suffix;
        Address::new(bytes)
    }

    fn deployment() -> DeploymentConfig {
        DeploymentConfig {
            network: Network::Base,
            chain_id: 8453,
            core: address(1),
            router: address(2),
            expect_whitelisted: true,
            deployment_block: 10,
            expected_runtime_code_hash: B256::new([3; 32]),
            contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            http_rpc_url: "http://rpc".into(),
            realtime_source: "ws://stream".into(),
            explicit_lane_assets: vec![address(4)],
        }
    }

    fn checkpoint() -> Checkpoint {
        let mut state = QuoteState {
            cash: address(5),
            ..QuoteState::default()
        };
        state.lanes.insert(
            address(4),
            LaneState::new(U256::from(17), 18, 19, 20, true, true),
        );
        state.fee_profile.partner_fee_bps.insert(address(4), 21);
        Checkpoint {
            schema_version: SCHEMA_VERSION,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            expected_runtime_code_hash: B256::new([3; 32]),
            chain_id: 8453,
            core: address(1),
            router: address(2),
            cursor: ChainCursor::execution_block(
                8453,
                100,
                99,
                Some(B256::new([7; 32])),
                Commitment::Canonical,
            ),
            state,
        }
    }

    #[test]
    fn key_is_bound_to_v3_chain_core_and_router() {
        let store = RedisCheckpointStore::new("redis://localhost/", &deployment());
        assert_eq!(
            store.key,
            format!("lunarbase:v3:8453:{}:{}", address(1), address(2))
        );
    }

    #[test]
    fn json_dto_round_trip_preserves_compact_state() {
        let expected = checkpoint();
        let json = serde_json::to_vec(&CheckpointDto::from(&expected)).unwrap();
        let decoded: CheckpointDto = serde_json::from_slice(&json).unwrap();
        let actual = Checkpoint::try_from(decoded).unwrap();
        assert_eq!(actual, expected);
        let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(value["schemaVersion"], SCHEMA_VERSION);
    }
}
