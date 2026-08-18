use super::*;

#[tokio::test]
async fn bootstrap_snapshot_is_bounded_and_cancels_the_subscription() {
    let gate = Arc::new(Notify::new());
    let source = Arc::new(MockSource::new(Some(gate)));
    let mut runtime = config();
    runtime.source_operation_timeout = Duration::from_millis(30);
    let started = Instant::now();

    let error = match ConnectedQuoteClient::connect(runtime, source.clone(), None).await {
        Ok(client) => {
            client.shutdown().await;
            panic!("blocked snapshot unexpectedly completed")
        }
        Err(error) => error,
    };

    assert!(error.to_string().contains("bootstrap snapshot exceeded"));
    assert!(started.elapsed() < Duration::from_secs(1));
    wait_until(|| source.events.receiver_count() == 0).await;
}

#[tokio::test]
async fn reducer_publication_updates_state_freshness_separately_from_ingestion() {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source.clone(), None)
        .await
        .unwrap();
    let initial = client.runtime_stats();
    assert_ne!(initial.last_state_update_unix_millis, 0);
    assert_eq!(initial.last_source_update_unix_millis, 0);

    source.publish(ChainUpdate::Head(BlockRef::new(
        cursor(101, Commitment::Realtime),
        None,
    )));
    wait_until(|| {
        client
            .health()
            .ok()
            .and_then(|health| health.cursor)
            .is_some_and(|cursor| cursor.block_number == 101)
    })
    .await;
    let advanced = client.runtime_stats();
    assert_ne!(advanced.last_source_update_unix_millis, 0);
    assert!(advanced.last_state_update_unix_millis >= initial.last_state_update_unix_millis);
    client.shutdown().await;
}

#[tokio::test]
async fn graceful_shutdown_returns_state_after_the_reducer_stops() {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source.clone(), None)
        .await
        .unwrap();
    source.publish(ChainUpdate::Head(BlockRef::new(
        cursor(101, Commitment::Realtime),
        None,
    )));
    wait_until(|| client.runtime_stats().last_source_update_unix_millis != 0).await;

    let checkpoint = client
        .shutdown_gracefully_with_checkpoint(Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        checkpoint.cursor.block_number, 100,
        "a realtime head cannot be persisted as complete before late logs arrive"
    );
    assert!(!client.is_ready());
    assert!(client.checkpoint().unwrap().is_none());
}
