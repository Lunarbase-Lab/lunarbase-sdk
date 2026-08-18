use super::{PumpRuntime, consume, normalize_update, send, spawn};
use crate::metrics::Metrics;
use alloy_primitives::{Address, B256, Bytes};
use futures_util::stream;
use lunarbase_client::{
    bootstrap::BootstrapSnapshot,
    model::{
        BackfillRequest, BlockRef, ChainCursor, ChainUpdate, Checkpoint, Commitment,
        ContractFilter, ContractLog, DeploymentConfig, Network, SourceError,
    },
    source::{ChainDataSource, SourceStream},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Notify, Semaphore, mpsc, watch};

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
        ChainUpdate::Gap {
            cursor: Some(cursor),
            reason,
        } if cursor.block_number == 1
            && reason.contains("exceeded event-worker queue byte budget")
    ));
}

#[tokio::test]
async fn oversized_live_update_revokes_activity_before_admission() {
    let metrics = Arc::new(Metrics::new(2, 1024, 2, 1024));
    metrics.set_ready(true);
    let (sender, mut receiver) = mpsc::channel(2);
    let (active, active_rx) = watch::channel(true);
    let (shutdown_guard, shutdown) = watch::channel(false);
    let update = ChainUpdate::Log(ContractLog {
        address: Address::new([1; 20]),
        transaction_hash: None,
        topics: vec![B256::new([2; 32])],
        data: Bytes::from(vec![3; 2_048]),
        removed: false,
        cursor: cursor(105),
    });
    assert!(matches!(
        normalize_update(update.clone(), 1024),
        ChainUpdate::Gap { cursor: Some(cursor), .. } if cursor.block_number == 105
    ));
    let source: SourceStream = Box::pin(stream::iter([Ok(update)]));
    let mut runtime = PumpRuntime {
        reconnect_delay: Duration::from_millis(1),
        stall_timeout: Duration::from_secs(1),
        active,
        shutdown,
        metrics: metrics.clone(),
        byte_budget: Arc::new(Semaphore::new(1024)),
        byte_capacity: 1024,
    };

    assert!(consume(source, &sender, &mut runtime).await);
    assert!(!*active_rx.borrow());
    assert!(!metrics.is_ready());
    assert!(matches!(
        receiver.recv().await.unwrap().into_inner(),
        ChainUpdate::Gap { cursor: Some(cursor), .. } if cursor.block_number == 105
    ));
    drop(shutdown_guard);
}

#[tokio::test]
async fn queue_budget_retains_only_the_visible_prefix_slice() {
    let metrics = Arc::new(Metrics::new(1, 1024, 1, 1024));
    let (sender, mut receiver) = mpsc::channel(1);
    let (mut runtime, _shutdown_guard) = runtime(metrics.clone(), 1024);
    let backing = Bytes::from(vec![0x4d; 1 << 20]);
    let data = backing.slice(..1);
    drop(backing);
    let update = ChainUpdate::Log(ContractLog {
        address: Address::new([1; 20]),
        transaction_hash: None,
        topics: Vec::new(),
        data,
        removed: false,
        cursor: cursor(105),
    });

    assert!(send(&sender, update, &mut runtime).await);
    assert!(!metrics.queues_empty());
    let ChainUpdate::Log(log) = receiver.recv().await.unwrap().into_inner() else {
        panic!("small visible payload must remain a log");
    };
    assert_eq!(log.data.as_ref(), [0x4d]);
    let data: Vec<u8> = log.data.into();
    assert_eq!(data.capacity(), data.len());
}

#[tokio::test]
async fn saturated_queue_backpressures_and_preserves_both_updates() {
    let metrics = Arc::new(Metrics::new(1, 1024, 1, 1024));
    let (sender, mut receiver) = mpsc::channel(1);
    let first_update = ChainUpdate::Head(BlockRef::new(cursor(1), None));
    let (mut runtime, shutdown_guard) = runtime(metrics.clone(), first_update.retained_bytes());
    assert!(send(&sender, first_update, &mut runtime).await);

    let second_sender = sender.clone();
    let second = tokio::spawn(async move {
        let _shutdown_guard = shutdown_guard;
        send(
            &second_sender,
            ChainUpdate::Head(BlockRef::new(cursor(2), None)),
            &mut runtime,
        )
        .await
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
    assert!(matches!(first, ChainUpdate::Head(head) if head.cursor.block_number == 1));
    assert!(matches!(second, ChainUpdate::Head(head) if head.cursor.block_number == 2));
}

#[tokio::test]
async fn panicking_stream_revokes_activity_and_readiness_before_join() {
    let metrics = Arc::new(Metrics::new(1, 1024, 1, 1024));
    let panic_gate = Arc::new(Notify::new());
    let source = Arc::new(PanicSource {
        panic_gate: panic_gate.clone(),
    });
    let (sender, _receiver) = mpsc::channel(1);
    let (active, mut active_rx) = watch::channel(false);
    let (_shutdown_guard, shutdown) = watch::channel(false);
    let handle = spawn(
        source,
        ContractFilter {
            address: Address::new([1; 20]),
            topics: Vec::new(),
        },
        sender,
        PumpRuntime {
            reconnect_delay: Duration::from_millis(1),
            stall_timeout: Duration::from_secs(1),
            active,
            shutdown,
            metrics: metrics.clone(),
            byte_budget: Arc::new(Semaphore::new(1024)),
            byte_capacity: 1024,
        },
    );

    wait_for_activity(&mut active_rx, true).await;
    let stale_lease = metrics.source_lease().unwrap();
    metrics.set_ready(true);
    panic_gate.notify_one();
    wait_for_activity(&mut active_rx, false).await;

    assert!(!metrics.is_ready());
    assert!(metrics.source_lease().is_none());
    assert!(!metrics.publish_ready_if(stale_lease));
    assert!(handle.await.unwrap_err().is_panic());
}

struct PanicSource {
    panic_gate: Arc<Notify>,
}

impl ChainDataSource for PanicSource {
    fn network(&self) -> Network {
        Network::Base
    }

    async fn snapshot(
        &self,
        _deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        unreachable!("panic source only supports subscribe")
    }

    async fn backfill(&self, _request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        unreachable!("panic source only supports subscribe")
    }

    async fn subscribe(&self, _filter: ContractFilter) -> Result<SourceStream, SourceError> {
        let panic_gate = self.panic_gate.clone();
        Ok(Box::pin(stream::once(async move {
            panic_gate.notified().await;
            panic!("intentional panic while polling source stream");
            #[allow(unreachable_code)]
            Ok(ChainUpdate::Gap {
                cursor: None,
                reason: String::new(),
            })
        })))
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        unreachable!("panic source only supports subscribe")
    }

    async fn validate_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        unreachable!("panic source only supports subscribe")
    }
}

async fn wait_for_activity(active: &mut watch::Receiver<bool>, expected: bool) {
    while *active.borrow_and_update() != expected {
        active.changed().await.unwrap();
    }
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
