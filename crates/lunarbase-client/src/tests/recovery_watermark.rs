//! Failed-update coverage and bounded staging across canonical recovery retries.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lagging_snapshot_cannot_clear_an_explicit_gap_without_an_event_sink() {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source.clone(), None)
        .await
        .unwrap();
    wait_until(|| source.events.receiver_count() >= 1).await;

    source.publish(ChainUpdate::Gap {
        cursor: Some(cursor(105, Commitment::Canonical)),
        reason: "known gap boundary".into(),
    });
    wait_until(|| client.runtime_stats().recovery_failures >= 1).await;
    assert!(!client.is_ready());
    assert_eq!(client.runtime_stats().queue_depth, 1);
    assert!(client.runtime_stats().queue_bytes > 0);
    assert!(source.backfill_calls.load(Ordering::Relaxed) == 0);

    source.set_snapshot_block(105);
    wait_until(|| client.runtime_stats().recoveries >= 1).await;
    assert!(client.is_ready());
    assert_eq!(client.health().unwrap().cursor.unwrap().block_number, 105);
    assert_eq!(client.runtime_stats().queue_depth, 0);
    assert_eq!(client.runtime_stats().queue_bytes, 0);
    assert!(source.backfill_calls.load(Ordering::Relaxed) >= 1);
    client.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursorless_gap_uses_a_fresh_canonical_head_as_required_coverage() {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source.clone(), None)
        .await
        .unwrap();
    let snapshot_gate = Arc::new(Notify::new());
    source.set_snapshot_gate(snapshot_gate.clone());
    source.set_snapshot_block(105);
    wait_until(|| source.events.receiver_count() >= 1).await;

    source.publish(ChainUpdate::Gap {
        cursor: None,
        reason: "unknown gap boundary".into(),
    });
    wait_until(|| source.canonical_calls.load(Ordering::Relaxed) >= 1).await;
    wait_until(|| source.snapshot_calls.load(Ordering::Relaxed) >= 2).await;
    source.set_snapshot_block(100);
    snapshot_gate.notify_one();
    wait_until(|| client.runtime_stats().recovery_failures >= 1).await;
    assert!(!client.is_ready());
    assert!(source.canonical_calls.load(Ordering::Relaxed) >= 1);

    source.set_snapshot_block(105);
    wait_until(|| source.snapshot_calls.load(Ordering::Relaxed) >= 3).await;
    snapshot_gate.notify_one();
    wait_until(|| client.runtime_stats().recoveries >= 1).await;
    assert!(client.is_ready());
    assert_eq!(client.health().unwrap().cursor.unwrap().block_number, 105);
    client.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_and_buffered_updates_keep_their_budgets_until_install_succeeds() {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source.clone(), None)
        .await
        .unwrap();
    wait_until(|| source.events.receiver_count() >= 1).await;
    let snapshot_gate = Arc::new(Notify::new());
    source.set_snapshot_gate(snapshot_gate.clone());

    source.publish(ChainUpdate::Log(unknown_log(101)));
    source.publish(ChainUpdate::Gap {
        cursor: Some(cursor(105, Commitment::Canonical)),
        reason: "bounded staged handoff".into(),
    });
    wait_until(|| source.snapshot_calls.load(Ordering::Relaxed) >= 2).await;
    wait_until(|| source.subscribe_calls.load(Ordering::Relaxed) >= 2).await;
    source.publish(ChainUpdate::Log(
        super::optimistic_correction::lane_removed_log(106, B256::new([1; 32])),
    ));
    wait_until(|| client.runtime_stats().queue_depth >= 2).await;
    snapshot_gate.notify_one();

    wait_until(|| client.runtime_stats().recovery_failures >= 1).await;
    assert!(!client.is_ready());
    let staged = client.runtime_stats();
    assert!(staged.queue_depth >= 1);
    assert!(staged.queue_depth <= staged.queue_capacity);
    assert!(staged.queue_bytes > 0);
    assert!(staged.queue_bytes <= staged.queue_byte_capacity);

    source.set_snapshot_block(105);
    wait_until(|| source.snapshot_calls.load(Ordering::Relaxed) >= 3).await;
    snapshot_gate.notify_one();
    wait_until(|| client.runtime_stats().recoveries >= 1).await;
    assert!(client.is_ready());
    assert_eq!(client.health().unwrap().cursor.unwrap().block_number, 106);
    assert_eq!(client.runtime_stats().queue_depth, 0);
    assert_eq!(client.runtime_stats().queue_bytes, 0);
    client.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn higher_snapshot_cannot_bypass_a_noncanonical_finalized_floor() {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source.clone(), None)
        .await
        .unwrap();
    source.set_snapshot_block(105);
    source.checkpoint_valid.store(false, Ordering::Relaxed);
    wait_until(|| source.events.receiver_count() >= 1).await;

    source.publish(ChainUpdate::Gap {
        cursor: Some(cursor(105, Commitment::Canonical)),
        reason: "finalized ancestry proof".into(),
    });
    wait_until(|| client.runtime_stats().recovery_failures >= 1).await;
    assert!(!client.is_ready());
    assert!(source.validate_calls.load(Ordering::Relaxed) >= 1);

    source.checkpoint_valid.store(true, Ordering::Relaxed);
    wait_until(|| client.runtime_stats().recoveries >= 1).await;
    assert!(client.is_ready());
    assert_eq!(client.health().unwrap().cursor.unwrap().block_number, 105);
    client.shutdown().await;
}
