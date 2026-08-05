//! Base pending-log wire-shape regression.

use lunarbase_client::model::Commitment;
use lunarbase_math::types::B256;
use lunarbase_source_evm::rpc::codec::parse_rpc_log;
use serde_json::json;

#[test]
fn parses_positioned_base_pending_log_with_zero_block_hash() {
    let value = json!({
        "address": "0x0000000000000000000000000000000000000001",
        "topics": [
            "0x1c61848d54083be4bfb8a26449add9f919cf1efd4ca608005f7f3f6aa0cef958",
            "0x0000000000000000000000001111111111111111111111111111111111111111"
        ],
        "data": "0x",
        "blockHash": format!("{:#x}", B256::ZERO),
        "blockNumber": "0x2a",
        "transactionHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transactionIndex": "0x3",
        "logIndex": "0x7",
        "removed": false
    });

    let log = parse_rpc_log(&value, 8453, Commitment::Realtime).unwrap();

    assert_eq!(log.cursor.block_number, 42);
    assert_eq!(log.cursor.block_hash, Some(B256::ZERO));
    assert_eq!(log.cursor.transaction_index, Some(3));
    assert_eq!(log.cursor.log_index, Some(7));
}
