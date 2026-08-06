//! Redis checkpoint persistence for faster restarts.

use lunarbase_client::model::{ChainCursor, Checkpoint, Commitment, DeploymentConfig, Network};
use lunarbase_math::{Address, B256, U256};
use lunarbase_math::{FeeProfile, LaneState, QuoteState};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::Duration,
};
use thiserror::Error;

const REDIS_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
/// One-key Redis store. `SET` is atomic and the key has no TTL.
pub struct RedisCheckpointStore {
    /// Redis connection URL used only by blocking checkpoint workers.
    url: String,
    /// Deployment- and schema-specific key containing the full checkpoint DTO.
    key: String,
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
        Self {
            url: url.into(),
            key: format!(
                "lunarbase:v5:{}:{:#x}:{:#x}",
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
            configure_connection(&connection)?;
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
            configure_connection(&connection)?;
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
    router: String,
    deployment_block: u64,
    expect_whitelisted: bool,
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
    fee_profile: FeeProfileDto,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LaneDto {
    asset: String,
    slot0: String,
    asset_reserve: u128,
    total_principal_amount: u128,
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
                asset_reserve: lane.asset_reserve,
                total_principal_amount: lane.total_principal_amount,
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
            router: address_hex(checkpoint.router),
            deployment_block: checkpoint.deployment_block,
            expect_whitelisted: checkpoint.expect_whitelisted,
            explicit_lane_assets,
            cursor: CursorDto::from(&checkpoint.cursor),
            state: StateDto {
                cash: address_hex(checkpoint.state.cash),
                cash_reserve: checkpoint.state.cash_reserve,
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
        let CheckpointDto {
            schema_version,
            math_compatibility_version,
            expected_implementation,
            expected_implementation_code_hash,
            chain_id,
            network,
            core,
            router,
            deployment_block,
            expect_whitelisted,
            explicit_lane_assets,
            cursor,
            state,
        } = dto;
        let StateDto {
            cash,
            cash_reserve,
            lanes: lane_values,
            fee_profile,
        } = state;
        let FeeProfileDto {
            whitelisted,
            blacklist_fee_multiplier,
            partner_fee_bps: partner_fee_values,
        } = fee_profile;

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

        let mut partner_fee_bps = HashMap::with_capacity(partner_fee_values.len());
        for fee in partner_fee_values {
            let asset = parse_address(&fee.asset)?;
            if partner_fee_bps.insert(asset, fee.fee_bps).is_some() {
                return Err(CheckpointError::Invalid(
                    "duplicate partner-fee asset".into(),
                ));
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
            router: parse_address(&router)?,
            deployment_block,
            expect_whitelisted,
            explicit_lane_assets: parsed_explicit_lanes,
            cursor: cursor.try_into()?,
            state: QuoteState {
                cash: parse_address(&cash)?,
                cash_reserve,
                lanes,
                fee_profile: FeeProfile {
                    whitelisted,
                    blacklist_fee_multiplier: parse_u256(&blacklist_fee_multiplier)?,
                    partner_fee_bps,
                },
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
mod tests {
    use crate::checkpoint::{CheckpointDto, CheckpointError, RedisCheckpointStore};
    use lunarbase_client::model::{
        ChainCursor, Checkpoint, Commitment, DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network,
        SCHEMA_VERSION,
    };
    use lunarbase_math::slot0::set_lane_slot0_exists;
    use lunarbase_math::{Address, B256, U256};
    use lunarbase_math::{LaneState, QuoteState};

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
            expected_implementation: address(3),
            expected_implementation_code_hash: B256::new([3; 32]),
            contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            explicit_lane_assets: vec![address(4)],
        }
    }

    fn checkpoint() -> Checkpoint {
        let mut state = QuoteState {
            cash: address(5),
            cash_reserve: 16,
            ..QuoteState::default()
        };
        state.lanes.insert(
            address(4),
            LaneState::new(set_lane_slot0_exists(U256::from(17), true), 18, 19),
        );
        state.fee_profile.partner_fee_bps.insert(address(4), 21);
        Checkpoint {
            schema_version: SCHEMA_VERSION,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            expected_implementation: address(3),
            expected_implementation_code_hash: B256::new([3; 32]),
            chain_id: 8453,
            network: Network::Base,
            core: address(1),
            router: address(2),
            deployment_block: 10,
            expect_whitelisted: true,
            explicit_lane_assets: vec![address(4)],
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
    fn key_is_bound_to_current_schema_chain_core_and_router() {
        let store = RedisCheckpointStore::new("redis://localhost/", &deployment());
        assert_eq!(
            store.key,
            format!("lunarbase:v5:8453:{}:{}", address(1), address(2))
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
        assert_eq!(value["network"], "Base");
        assert_eq!(value["deploymentBlock"], 10);
        assert_eq!(value["expectWhitelisted"], true);
        assert!(actual.has_valid_structure());
    }

    #[test]
    fn dto_rejects_duplicate_lane_and_partner_fee_assets() {
        let original = CheckpointDto::from(&checkpoint());

        let mut duplicate_lane = serde_json::to_value(&original).unwrap();
        let lane = duplicate_lane["state"]["lanes"][0].clone();
        duplicate_lane["state"]["lanes"]
            .as_array_mut()
            .unwrap()
            .push(lane);
        let dto: CheckpointDto = serde_json::from_value(duplicate_lane).unwrap();
        assert!(matches!(
            Checkpoint::try_from(dto),
            Err(CheckpointError::Invalid(message)) if message.contains("duplicate lane")
        ));

        let mut duplicate_fee = serde_json::to_value(CheckpointDto::from(&checkpoint())).unwrap();
        let fee = duplicate_fee["state"]["feeProfile"]["partnerFeeBps"][0].clone();
        duplicate_fee["state"]["feeProfile"]["partnerFeeBps"]
            .as_array_mut()
            .unwrap()
            .push(fee);
        let dto: CheckpointDto = serde_json::from_value(duplicate_fee).unwrap();
        assert!(matches!(
            Checkpoint::try_from(dto),
            Err(CheckpointError::Invalid(message)) if message.contains("duplicate partner-fee")
        ));
    }

    #[test]
    fn dto_rejects_state_that_violates_quote_invariants() {
        let mut invalid_fee = serde_json::to_value(CheckpointDto::from(&checkpoint())).unwrap();
        invalid_fee["state"]["feeProfile"]["partnerFeeBps"][0]["feeBps"] =
            serde_json::json!(1_000_001);
        let dto: CheckpointDto = serde_json::from_value(invalid_fee).unwrap();
        assert!(matches!(
            Checkpoint::try_from(dto),
            Err(CheckpointError::Invalid(message)) if message.contains("structural")
        ));

        let mut inactive_lane = serde_json::to_value(CheckpointDto::from(&checkpoint())).unwrap();
        inactive_lane["state"]["lanes"][0]["slot0"] = serde_json::json!("0");
        let dto: CheckpointDto = serde_json::from_value(inactive_lane).unwrap();
        assert!(matches!(
            Checkpoint::try_from(dto),
            Err(CheckpointError::Invalid(message)) if message.contains("structural")
        ));
    }
}
