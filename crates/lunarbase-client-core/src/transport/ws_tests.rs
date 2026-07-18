use super::*;
use lunarbase_math::{Address, B256, U256};

#[test]
fn builds_standard_logs_subscription() {
    let address = "0x0000000000000000000000000000000000000001"
        .parse::<Address>()
        .unwrap();
    let request = subscription_request(
        1,
        &ContractFilter {
            address,
            topics: vec![B256::new(U256::ONE.to_be_bytes::<32>())],
        },
        "logs",
    );
    let value: Value = serde_json::from_str(&request).unwrap();
    assert_eq!(value["method"], "eth_subscribe");
    assert_eq!(value["params"][0], "logs");
    assert_eq!(value["params"][1]["address"], format!("{address:#x}"));
    assert_eq!(value["params"][1]["topics"][0], format!("0x{:064x}", 1));
}

#[test]
fn parses_heads_and_preserves_parent_hash() {
    let hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let parent = "0x2222222222222222222222222222222222222222222222222222222222222222";
    let value = json!({"number":"0x2a","hash":hash,"parentHash":parent});
    let head = parse_ws_head(&value, 42161).unwrap();
    assert_eq!(head.cursor.block_number, 42);
    assert_eq!(head.cursor.block_hash, Some(B256::new([0x11; 32])));
    assert_eq!(head.parent_hash, Some(B256::new([0x22; 32])));
    assert_eq!(head.cursor.commitment, Commitment::Realtime);
}

#[test]
fn rejects_invalid_head_hash_width() {
    let value = json!({"number":"0x2a","hash":"0x01"});
    assert!(parse_ws_head(&value, 42161).is_err());
}

#[test]
fn progressive_heads_accept_same_height_when_parent_is_stable() {
    let first = parse_ws_head(
        &json!({
            "number":"0x2a",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
        }),
        8453,
    )
    .unwrap();
    let second = parse_ws_head(
        &json!({
            "number":"0x2a",
            "hash": format!("0x{}", "33".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
        }),
        8453,
    )
    .unwrap();

    assert!(!head_discontinuity(&first, &second, true));
    assert!(head_discontinuity(&first, &second, false));
}
