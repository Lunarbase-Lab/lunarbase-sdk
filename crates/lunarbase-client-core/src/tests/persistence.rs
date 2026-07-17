#[test]
fn binary_codec_round_trips_checkpoint_and_update() {
    let asset = address(2);
    let cursor = cursor(7);
    let checkpoint = Checkpoint {
        schema_version: SCHEMA_VERSION,
        math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        expected_runtime_code_hash: [9u8; 32],
        cursor: cursor.clone(),
        state: QuoteState {
            cash: address(1),
            lanes: [(
                asset,
                lunarbase_math::LaneState {
                    slot0: U256::MAX,
                    exists: true,
                    paused: false,
                    block_delay: 3,
                    slippage_k_bps: 42,
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        },
    };
    let encoded = encode_checkpoint(&checkpoint).unwrap();
    assert_eq!(decode_checkpoint(&encoded).unwrap(), checkpoint);

    let update = ChainUpdate::Head(cursor);
    assert_eq!(decode_update(&encode_update(&update)).unwrap(), update);
}

#[test]
fn in_memory_checkpoint_store_deduplicates_replayed_updates() {
    let checkpoint = Checkpoint {
        schema_version: SCHEMA_VERSION,
        math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        expected_runtime_code_hash: [0; 32],
        cursor: cursor(0),
        state: QuoteState {
            cash: address(1),
            ..Default::default()
        },
    };
    let update = ChainUpdate::Head(cursor(1));
    let mut store = InMemoryRedisStore::new(8);
    store
        .commit(checkpoint.clone(), vec![update.clone()])
        .unwrap();
    store.commit(checkpoint, vec![update]).unwrap();
    assert_eq!(store.updates(), vec![ChainUpdate::Head(cursor(1))]);
}

#[test]
fn redis_dedup_keys_share_the_checkpoint_hash_tag() {
    let namespace = RedisNamespace::new(8453, address(7));
    let key = crate::persistence::update_dedup_key(&namespace, &ChainUpdate::Head(cursor(1)));
    assert!(key.starts_with("lb:{8453:0x"));
}

#[test]
fn redis_store_rejects_an_unbounded_io_timeout() {
    let result = RedisCheckpointStore::connect_with_io_timeout(
        "redis://127.0.0.1/",
        RedisNamespace::new(8453, address(7)),
        8,
        60,
        Duration::ZERO,
    );
    assert!(result.is_err());
}

