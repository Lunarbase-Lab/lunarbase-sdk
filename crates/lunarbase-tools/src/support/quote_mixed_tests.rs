use super::UpdateBus;
use futures_util::StreamExt;
use lunarbase_client::model::{ChainCursor, ChainUpdate, Commitment, ContractLog};
use lunarbase_math::{Address, B256, Bytes};

const BACKING_BYTES: usize = 1 << 20;

#[tokio::test]
async fn update_bus_detaches_visible_payload_from_shared_backing() {
    let bus = UpdateBus::new(2, 8192);
    let mut updates = bus.subscribe();
    let backing = Bytes::from(vec![0x5a; BACKING_BYTES]);
    let visible = backing.slice(BACKING_BYTES - 1..);
    let backing_tail = visible.as_ptr();
    let update = ChainUpdate::Log(ContractLog {
        address: Address::ZERO,
        transaction_hash: None,
        topics: Vec::new(),
        data: visible,
        removed: false,
        cursor: ChainCursor::block(1, 1, Some(B256::ZERO), Commitment::Realtime),
    });

    assert!(bus.publish(update));
    let received = updates.next().await.unwrap().unwrap();
    let ChainUpdate::Log(log) = received else {
        panic!("update bus changed the update kind");
    };

    assert_eq!(log.data.as_ref(), [0x5a]);
    assert_ne!(log.data.as_ptr(), backing_tail);
    assert_eq!(backing.len(), BACKING_BYTES);
}
