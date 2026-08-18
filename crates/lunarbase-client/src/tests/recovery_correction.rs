//! Recovery publication ordering for buffered optimistic corrections.

use super::optimistic_correction::{OLD_HASH, correction, lane_removed_log, old_tip};
use super::*;
use crate::indexer::errors::ClientRuntimeEvent;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_recovery_publishes_buffered_correction_only_after_the_atomic_swap() {
    let source = Arc::new(MockSource::new(None));
    let (observer_tx, mut observer_rx) = mpsc::channel(4);
    let client =
        ConnectedQuoteClient::connect_with_event_sink(config(), source.clone(), None, observer_tx)
            .await
            .unwrap();
    let mut events = client.subscribe_runtime_events();
    let snapshot_gate = Arc::new(Notify::new());
    source.set_snapshot_gate(snapshot_gate.clone());

    source.publish(ChainUpdate::Gap {
        cursor: None,
        reason: "force delayed recovery".into(),
    });
    wait_until(|| source.snapshot_calls.load(Ordering::Relaxed) >= 2).await;
    assert!(!client.is_ready());

    let old_log = lane_removed_log(101, OLD_HASH);
    source.publish(ChainUpdate::Log(old_log.clone()));
    source.publish(ChainUpdate::Head(old_tip()));
    source.publish(ChainUpdate::Correction(Box::new(correction(Vec::new()))));
    wait_until(|| client.runtime_stats().queue_depth >= 3).await;
    snapshot_gate.notify_one();

    let observed = tokio::time::timeout(Duration::from_secs(1), observer_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed, old_log);
    assert!(client.is_ready());
    assert_eq!(
        client.health().unwrap().cursor.unwrap().block_hash,
        Some(super::optimistic_correction::NEW_HASH)
    );
    assert!(client.quote(&request()).is_ok());

    let mut correction_notices = 0;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            ClientRuntimeEvent::CorrectionApplied {
                common_ancestor: 100,
                old_tip_hash: OLD_HASH,
                new_tip_hash: super::optimistic_correction::NEW_HASH,
                replacement_logs: 0,
            } => {
                assert!(client.is_ready());
                correction_notices += 1;
            }
            ClientRuntimeEvent::RecoveryCompleted => break,
            _ => {}
        }
    }
    assert_eq!(correction_notices, 1);
    assert_eq!(client.runtime_stats().corrections, 1);
    assert_eq!(client.runtime_stats().recoveries, 1);
    assert!(events.try_recv().is_err());
    client.shutdown().await;
}
