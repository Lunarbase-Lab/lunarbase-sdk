use super::{PumpRuntime, send};
use crate::metrics::Metrics;
use alloy_primitives::{Address, B256, Bytes};
use lunarbase_client::model::{ChainCursor, ChainUpdate, Commitment, ContractLog};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc, watch};

#[tokio::test]
async fn oversized_update_becomes_a_terminal_gap_instead_of_disappearing() {
    let metrics = Arc::new(Metrics::new(2, 1024, 2, 1024));
    let (sender, mut receiver) = mpsc::channel(2);
    let (mut runtime, _shutdown_guard) = runtime(metrics, 1024);
    let update = ChainUpdate::Log(ContractLog {
        address: Address::new([1; 20]),
        transaction_hash: None,
        topics: vec![B256::new([2; 32])],
        data: Bytes::from(vec![3; 2048]),
        removed: false,
        cursor: cursor(1),
    });

    assert!(send(&sender, update, &mut runtime).await);
    let queued = receiver.recv().await.unwrap().into_inner();

    assert!(matches!(
        queued,
        ChainUpdate::Gap { reason, .. }
            if reason.contains("exceeded event-worker queue byte budget")
    ));
}

#[tokio::test]
async fn saturated_queue_backpressures_and_preserves_both_updates() {
    let metrics = Arc::new(Metrics::new(1, 1024, 1, 1024));
    let (sender, mut receiver) = mpsc::channel(1);
    let first_update = ChainUpdate::Head(cursor(1));
    let (mut runtime, shutdown_guard) = runtime(metrics.clone(), first_update.retained_bytes());
    assert!(send(&sender, first_update, &mut runtime).await);

    let second_sender = sender.clone();
    let second = tokio::spawn(async move {
        let _shutdown_guard = shutdown_guard;
        send(&second_sender, ChainUpdate::Head(cursor(2)), &mut runtime).await
    });
    tokio::task::yield_now().await;
    assert!(!second.is_finished());
    assert!(
        metrics
            .render()
            .contains("lunarbase_event_worker_queue_saturations_total 1")
    );

    let first = receiver.recv().await.unwrap().into_inner();
    assert!(second.await.unwrap());
    let second = receiver.recv().await.unwrap().into_inner();
    assert!(matches!(first, ChainUpdate::Head(cursor) if cursor.block_number == 1));
    assert!(matches!(second, ChainUpdate::Head(cursor) if cursor.block_number == 2));
}

fn runtime(metrics: Arc<Metrics>, byte_capacity: usize) -> (PumpRuntime, watch::Sender<bool>) {
    let (active, _) = watch::channel(false);
    let (shutdown_guard, shutdown) = watch::channel(false);
    (
        PumpRuntime {
            reconnect_delay: Duration::from_millis(1),
            stall_timeout: Duration::from_secs(1),
            active,
            shutdown,
            metrics,
            byte_budget: Arc::new(Semaphore::new(byte_capacity)),
            byte_capacity,
        },
        shutdown_guard,
    )
}

fn cursor(block_number: u64) -> ChainCursor {
    ChainCursor::block(
        8453,
        block_number,
        Some(B256::left_padding_from(&block_number.to_be_bytes())),
        Commitment::Realtime,
    )
}
