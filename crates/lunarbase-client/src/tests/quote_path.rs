//! Quote hot-path isolation from source I/O.

use super::*;

#[tokio::test]
async fn quote_and_batch_never_call_the_source() {
    let source = Arc::new(MockSource::new(None));
    let client = ConnectedQuoteClient::connect(config(), source.clone(), None)
        .await
        .unwrap();
    let calls = source_calls(&source);
    let single = client.quote(&request()).unwrap();
    let batch = client
        .quote_many(&[request(), request(), request()])
        .unwrap();
    assert_eq!(batch.cursor, single.cursor);
    assert!(
        batch
            .outcomes
            .iter()
            .all(|outcome| outcome == &single.outcome)
    );
    assert_eq!(source_calls(&source), calls);
    let oversized = vec![request(); 257];
    assert!(matches!(
        client.quote_many(&oversized),
        Err(IndexerError::InvalidRequest(_))
    ));
    client.shutdown().await;
}

fn source_calls(source: &MockSource) -> [usize; 5] {
    [
        source.snapshot_calls.load(Ordering::Relaxed),
        source.backfill_calls.load(Ordering::Relaxed),
        source.subscribe_calls.load(Ordering::Relaxed),
        source.canonical_calls.load(Ordering::Relaxed),
        source.validate_calls.load(Ordering::Relaxed),
    ]
}
