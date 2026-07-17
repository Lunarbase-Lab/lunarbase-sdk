#[test]
fn versioned_normalized_replay_fixture_reaches_reducer_boundary() {
    fn decimal(value: &serde_json::Value, field: &str) -> u64 {
        value[field]
            .as_str()
            .unwrap_or_else(|| panic!("fixture field {field} is not a decimal string"))
            .parse()
            .unwrap_or_else(|_| panic!("fixture field {field} is not a u64"))
    }
    fn hash(value: &serde_json::Value, field: &str) -> Option<[u8; 32]> {
        value[field]
            .as_str()
            .map(|encoded| decode_fixed_hex32(encoded).unwrap())
    }
    fn fixture_cursor(value: &serde_json::Value) -> ChainCursor {
        ChainCursor {
            chain_id: decimal(value, "chainId"),
            block_number: decimal(value, "blockNumber"),
            block_hash: hash(value, "blockHash"),
            transaction_index: value["transactionIndex"]
                .as_str()
                .map(|_| decimal(value, "transactionIndex") as u32),
            log_index: value["logIndex"]
                .as_str()
                .map(|_| decimal(value, "logIndex") as u32),
            source_sequence: value["sourceSequence"]
                .as_str()
                .map(|_| decimal(value, "sourceSequence")),
            source_sub_index: value["sourceSubIndex"]
                .as_str()
                .map(|_| decimal(value, "sourceSubIndex") as u32),
            commitment: match value["commitment"].as_str().unwrap() {
                "Realtime" => Commitment::Realtime,
                "Canonical" => Commitment::Canonical,
                "Finalized" => Commitment::Finalized,
                other => panic!("unknown fixture commitment {other}"),
            },
        }
    }

    let mut reducer = QuoteReducer::new(QuoteState {
        cash: address(1),
        ..Default::default()
    });
    let mut updates = 0;
    for line in
        include_str!("../../../../fixtures/event-replay/monad-exec-events/normalized-updates.jsonl")
            .lines()
    {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        match value["kind"].as_str().unwrap() {
            "Head" => {
                let cursor = fixture_cursor(&value["cursor"]);
                if reducer.cursor().is_none() {
                    reducer.bootstrap(cursor);
                } else {
                    reducer.observe_head(cursor).unwrap();
                }
            }
            "Log" => {
                let cursor = fixture_cursor(&value["cursor"]);
                let update = ChainUpdate::Log(ContractLog {
                    address: Address::ZERO,
                    topics: vec![U256::ONE],
                    data: Vec::new(),
                    removed: false,
                    cursor,
                });
                if let ChainUpdate::Log(log) = update {
                    assert!(decode_core_event(&log).unwrap().is_none());
                }
            }
            "Gap" => {
                assert_eq!(
                    value["reason"].as_str(),
                    Some("Monad parser subscription gap; skipped=3")
                );
                reducer.mark_not_ready();
            }
            other => panic!("unknown normalized fixture kind {other}"),
        }
        updates += 1;
    }
    assert_eq!(updates, 4);
    assert_eq!(reducer.cursor().unwrap().commitment, Commitment::Canonical);
    assert!(!reducer.is_ready());

    let checkpoint = Checkpoint {
        schema_version: SCHEMA_VERSION,
        math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        expected_runtime_code_hash: [0; 32],
        cursor: ChainCursor {
            chain_id: 143,
            block_number: 700,
            block_hash: Some([0xaa; 32]),
            transaction_index: None,
            log_index: None,
            source_sequence: Some(1000),
            source_sub_index: None,
            commitment: Commitment::Realtime,
        },
        state: QuoteState {
            cash: address(1),
            ..Default::default()
        },
    };
    let encoded = encode_checkpoint(&checkpoint).unwrap();
    let encoded_hex = encoded
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        format!("0x{encoded_hex}"),
        "0x4c4251310002000000446c756e6172626173652d636f6e74726163747340323464623437623836366538313530613064393163666664383065666534396466383531373962353a6d6174682d76310000000000000000000000000000000000000000000000000000000000000000000000000000008f00000000000002bc01aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000100000000000003e8000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    );
}
