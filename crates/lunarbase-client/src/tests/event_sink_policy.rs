//! Optional observer policy and reducer-priority acceptance tests.

use super::*;

#[tokio::test]
async fn full_event_observer_never_blocks_reducer_or_retries_dropped_log() {
    let source = Arc::new(MockSource::new(None));
    let (event_sender, mut event_receiver) = mpsc::channel(1);
    let client = ConnectedQuoteClient::connect_with_event_sink(
        config(),
        source.clone(),
        None,
        event_sender.clone(),
    )
    .await
    .unwrap();
    let filler = unknown_log(90);
    event_sender.send(filler.clone()).await.unwrap();

    source.publish(ChainUpdate::Log(lane_added_log(101)));
    wait_until(|| client.health().unwrap().cursor.unwrap().block_number == 101).await;
    wait_until(|| client.runtime_stats().event_observer_drops == 1).await;

    assert_eq!(event_receiver.recv().await.unwrap(), filler);
    assert!(event_receiver.try_recv().is_err());
    assert!(client.is_ready());
    client.shutdown().await;
}

#[tokio::test]
async fn event_observer_filters_logs_below_the_minimum_commitment() {
    let source = Arc::new(MockSource::new(None));
    let (event_sender, mut event_receiver) = mpsc::channel(2);
    let client = ConnectedQuoteClient::connect_with_event_sink_policy(
        config(),
        source.clone(),
        None,
        event_sender,
        CoreEventSinkPolicy {
            minimum_commitment: Commitment::Canonical,
        },
    )
    .await
    .unwrap();

    let realtime_log = unknown_log(101);
    source.publish(ChainUpdate::Log(realtime_log));
    source.publish(ChainUpdate::Head(cursor(102, Commitment::Realtime)));
    wait_until(|| client.health().unwrap().cursor.unwrap().block_number == 102).await;
    assert!(event_receiver.try_recv().is_err());

    let mut canonical_log = unknown_log(103);
    canonical_log.cursor.commitment = Commitment::Canonical;
    source.publish(ChainUpdate::Log(canonical_log.clone()));
    let delivered = tokio::time::timeout(Duration::from_secs(1), event_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered, canonical_log);
    client.shutdown().await;
}
fn lane_added_log(block: u64) -> ContractLog {
    let mut log = unknown_log(block);
    let mut asset_topic = [0_u8; 32];
    asset_topic[12..].copy_from_slice(ASSET.as_slice());
    log.topics = vec![TOPIC_LANE_ADDED, B256::new(asset_topic)];
    log
}
