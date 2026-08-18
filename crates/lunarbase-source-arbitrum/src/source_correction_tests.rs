use super::*;
use crate::tests::MockRpc;
use alloy_primitives::{Address, Bytes};
use lunarbase_client::model::BlockRef;
use serde_json::json;
use std::collections::HashMap;

#[tokio::test]
async fn correction_resolves_each_unique_nitro_context_once() {
    let ancestor_hash = B256::new([0x81; 32]);
    let old_hash = B256::new([0x82; 32]);
    let new_hash = B256::new([0x83; 32]);
    let mock = MockRpc::start(
        Vec::new(),
        HashMap::from([
            (
                "ancestor".into(),
                json!({
                    "number": "0xa",
                    "hash": format!("{ancestor_hash:#x}"),
                    "l1BlockNumber": "0x3e8"
                }),
            ),
            (
                "old".into(),
                json!({
                    "number": "0xb",
                    "hash": format!("{old_hash:#x}"),
                    "l1BlockNumber": "0x3e9"
                }),
            ),
            (
                "new".into(),
                json!({
                    "number": "0xb",
                    "hash": format!("{new_hash:#x}"),
                    "l1BlockNumber": "0x7d1"
                }),
            ),
        ]),
    )
    .await;
    let source = ArbitrumNitroSource::new(
        RpcHttpClient::new(&mock.url).unwrap(),
        "ws://127.0.0.1:1",
        42_161,
    );
    let ancestor = block(10, ancestor_hash, None);
    let old = block(11, old_hash, Some(ancestor_hash));
    let new = block(11, new_hash, Some(ancestor_hash));
    let replacement_logs = (0..2_000)
        .map(|log_index| ContractLog {
            address: Address::new([1; 20]),
            transaction_hash: Some(B256::new([0x84; 32])),
            topics: Vec::new(),
            data: Bytes::new(),
            removed: false,
            cursor: ChainCursor {
                log_index: Some(log_index),
                ..event_cursor(11, new_hash)
            },
        })
        .collect();
    let update = ChainUpdate::Correction(Box::new(ChainCorrection {
        common_ancestor: ancestor,
        old_tip: old.clone(),
        new_tip: new.clone(),
        old_branch: vec![old],
        new_branch: vec![new],
        replacement_logs,
    }));

    let ChainUpdate::Correction(correction) = source.enrich_update(update).await.unwrap() else {
        panic!("correction update expected");
    };
    assert!(
        correction
            .replacement_logs
            .iter()
            .all(|log| log.cursor.execution_block_number == 2_001)
    );
    assert_eq!(
        mock.calls()
            .iter()
            .filter(|call| call.method == "eth_getBlockByHash")
            .count(),
        3
    );
}

fn block(number: u64, hash: B256, parent_hash: Option<B256>) -> BlockRef {
    BlockRef::new(
        ChainCursor::block(42_161, number, Some(hash), Commitment::Realtime),
        parent_hash,
    )
}

fn event_cursor(number: u64, hash: B256) -> ChainCursor {
    let mut cursor = ChainCursor::block(42_161, number, Some(hash), Commitment::Realtime);
    cursor.transaction_index = Some(0);
    cursor.log_index = Some(0);
    cursor
}
