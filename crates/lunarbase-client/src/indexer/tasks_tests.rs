//! Queue-accounting tests for the source pump.

use super::send_update;
use crate::indexer::client_types::ClientRuntimeStats;
use crate::model::ChainUpdate;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, watch};

#[tokio::test]
async fn queue_depth_is_incremented_before_delivery() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (_cancel_sender, mut cancel) = watch::channel(false);
    let stats = ClientRuntimeStats::new(1, 1024);
    let update = ChainUpdate::Gap {
        cursor: None,
        reason: "counter-order test".into(),
    };

    let (sent, observed_depth) =
        tokio::join!(send_update(&sender, &mut cancel, update, &stats), async {
            let queued = receiver.recv().await.expect("update is delivered");
            let depth = stats.queue_depth.load(Ordering::Relaxed);
            let bytes = stats.queue_bytes.load(Ordering::Relaxed);
            assert!(bytes > 0);
            queued.dequeue(&stats);
            depth
        },);

    assert!(sent);
    assert_eq!(observed_depth, 1);
    assert_eq!(stats.queue_depth.load(Ordering::Relaxed), 0);
    assert_eq!(stats.queue_bytes.load(Ordering::Relaxed), 0);
}
