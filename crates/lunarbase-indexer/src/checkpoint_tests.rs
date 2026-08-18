use super::{
    CheckpointDto, CheckpointError, RedisCheckpointStore, StoreOutcome, checkpoint_json,
    checkpoint_order,
};
use lunarbase_client::model::{
    ChainCursor, Checkpoint, Commitment, DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network,
    SCHEMA_VERSION,
};
use lunarbase_math::slot0::set_lane_slot0_exists;
use lunarbase_math::{Address, B256, FeeClass, LaneState, QuoteState, U256};

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
        fee_class: FeeClass::Whitelisted,
        verified_router: None,
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
    state.blacklist_fee_multiplier = U256::from(21);
    Checkpoint {
        schema_version: SCHEMA_VERSION,
        math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        expected_implementation: address(3),
        expected_implementation_code_hash: B256::new([3; 32]),
        chain_id: 8453,
        network: Network::Base,
        core: address(1),
        deployment_block: 10,
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
fn key_is_bound_to_current_schema_chain_and_core() {
    let store = RedisCheckpointStore::new("redis://localhost/", &deployment());
    assert_eq!(store.key, format!("lunarbase:v6:8453:{}", address(1)));
}

#[test]
fn stored_payload_accepts_legacy_json_and_prefixed_cas_values() {
    let json = serde_json::to_vec(&CheckpointDto::from(&checkpoint())).unwrap();
    assert_eq!(checkpoint_json(&json).unwrap(), json);

    let mut prefixed = checkpoint_order(&checkpoint().cursor).into_bytes();
    prefixed.push(b'\n');
    prefixed.extend_from_slice(&json);
    assert_eq!(checkpoint_json(&prefixed).unwrap(), json);
}

#[test]
fn checkpoint_order_is_fixed_width_and_monotonic() {
    let base = checkpoint().cursor;
    let mut promoted = base.clone();
    promoted.commitment = Commitment::Finalized;
    assert!(checkpoint_order(&promoted) > checkpoint_order(&base));

    let mut next_log = promoted.clone();
    next_log.transaction_index = Some(0);
    next_log.log_index = Some(0);
    assert!(checkpoint_order(&next_log) > checkpoint_order(&promoted));

    let mut next_block = base;
    next_block.block_number += 1;
    next_block.execution_block_number += 1;
    assert!(checkpoint_order(&next_block) > checkpoint_order(&next_log));
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
    assert_eq!(value["state"]["blacklistFeeMultiplier"], "21");
    assert!(value.get("feeClass").is_none());
    assert!(value.get("verifiedRouter").is_none());
    assert!(actual.has_valid_structure());
}

#[test]
fn dto_rejects_duplicate_lane_assets() {
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
}

#[test]
fn dto_rejects_state_that_violates_quote_invariants() {
    let mut invalid_multiplier = serde_json::to_value(CheckpointDto::from(&checkpoint())).unwrap();
    invalid_multiplier["state"]["blacklistFeeMultiplier"] = serde_json::json!("not-a-word");
    let dto: CheckpointDto = serde_json::from_value(invalid_multiplier).unwrap();
    assert!(matches!(
        Checkpoint::try_from(dto),
        Err(CheckpointError::Invalid(_))
    ));

    let mut inactive_lane = serde_json::to_value(CheckpointDto::from(&checkpoint())).unwrap();
    inactive_lane["state"]["lanes"][0]["slot0"] = serde_json::json!("0");
    let dto: CheckpointDto = serde_json::from_value(inactive_lane).unwrap();
    assert!(matches!(
        Checkpoint::try_from(dto),
        Err(CheckpointError::Invalid(message)) if message.contains("structural")
    ));
}

#[tokio::test]
async fn redis_checkpoint_cas_rejects_regressions_when_test_redis_is_configured() {
    let Ok(url) = std::env::var("LUNARBASE_TEST_REDIS_URL") else {
        return;
    };
    let mut identity = deployment();
    let suffix = std::process::id().to_be_bytes();
    let mut core = [0_u8; 20];
    core[16..].copy_from_slice(&suffix);
    identity.core = Address::new(core);
    let store = RedisCheckpointStore::new(url, &identity);
    let mut current = checkpoint();
    current.core = identity.core;
    assert_eq!(store.store(&current).await.unwrap(), StoreOutcome::Stored);
    assert_eq!(
        store.store(&current).await.unwrap(),
        StoreOutcome::Unchanged
    );

    let mut older = current.clone();
    older.cursor.block_number -= 1;
    older.cursor.execution_block_number -= 1;
    assert_eq!(store.store(&older).await.unwrap(), StoreOutcome::Stale);
    assert_eq!(store.load().await.unwrap().unwrap(), current);
}
