//! Deterministic quote-critical update pressure for mixed-load benchmarks.

use alloy_sol_types::SolEvent;
use futures_util::stream;
use lunarbase_client::{
    model::{ChainUpdate, Commitment, ContractLog, SourceError},
    prelude::{
        BackfillRequest, BootstrapSnapshot, ChainCursor, ChainDataSource, Checkpoint,
        ConnectedQuoteClient, ContractFilter, Network,
    },
    protocol::abi::core,
    source::SourceStream,
};
use lunarbase_math::{Address, B256, Bytes, U256};
use serde::Serialize;
use std::{
    future::{Future, ready},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, watch},
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};

#[derive(Clone, Debug)]
/// Broadcast transport used by the synthetic connected client.
pub struct UpdateBus {
    sender: broadcast::Sender<ChainUpdate>,
    max_update_bytes: usize,
}

impl UpdateBus {
    /// Creates a count-and-byte-bounded channel for fixed-size benchmark updates.
    pub fn new(capacity: usize, byte_capacity: usize) -> Self {
        assert!(capacity > 0 && byte_capacity >= capacity);
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            max_update_bytes: byte_capacity / capacity,
        }
    }

    /// Opens one source stream, mapping lag to a terminal continuity error.
    pub fn subscribe(&self) -> SourceStream {
        let receiver = self.sender.subscribe();
        Box::pin(stream::unfold(receiver, |mut receiver| async move {
            match receiver.recv().await {
                Ok(update) => Some((Ok(update), receiver)),
                Err(broadcast::error::RecvError::Lagged(skipped)) => Some((
                    Err(SourceError::Gap(format!(
                        "mixed benchmark source lagged by {skipped} updates"
                    ))),
                    receiver,
                )),
                Err(broadcast::error::RecvError::Closed) => None,
            }
        }))
    }

    fn publish(&self, update: ChainUpdate) -> bool {
        update.retained_bytes() <= self.max_update_bytes && self.sender.send(update).is_ok()
    }
}

#[derive(Clone, Debug)]
/// Connected-client source that combines one deterministic snapshot and update bus.
pub struct SyntheticSource {
    snapshot: BootstrapSnapshot,
    updates: UpdateBus,
}

impl SyntheticSource {
    /// Creates a source whose subscribers consume updates published through `updates`.
    pub fn new(snapshot: BootstrapSnapshot, updates: UpdateBus) -> Self {
        Self { snapshot, updates }
    }
}

impl ChainDataSource for SyntheticSource {
    fn network(&self) -> Network {
        Network::Base
    }

    fn snapshot(
        &self,
        _deployment: &lunarbase_client::model::DeploymentConfig,
    ) -> impl Future<Output = Result<BootstrapSnapshot, SourceError>> + Send {
        ready(Ok(self.snapshot.clone()))
    }

    fn backfill(
        &self,
        _request: BackfillRequest,
    ) -> impl Future<Output = Result<Vec<ContractLog>, SourceError>> + Send {
        ready(Ok(Vec::new()))
    }

    fn subscribe(
        &self,
        _filter: ContractFilter,
    ) -> impl Future<Output = Result<SourceStream, SourceError>> + Send {
        ready(Ok(self.updates.subscribe()))
    }

    fn canonical_head(&self) -> impl Future<Output = Result<ChainCursor, SourceError>> + Send {
        ready(Ok(self.snapshot.cursor.clone()))
    }

    fn validate_checkpoint(
        &self,
        _checkpoint: &Checkpoint,
    ) -> impl Future<Output = Result<bool, SourceError>> + Send {
        ready(Ok(true))
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
/// Update workload observed during one mixed timing run.
pub struct MixedLoadReport {
    /// Configured maximum publication rate.
    pub configured_events_per_second: u64,
    /// Updates accepted by the synthetic source channel.
    pub published_updates: u64,
    /// Updates published before the quote timing window ended.
    pub published_during_measurement: u64,
    /// Those updates already visible at the reducer cursor when timing ended.
    pub applied_during_measurement: u64,
    /// Published updates observed at the reducer cursor before reporting.
    pub applied_updates: u64,
    /// Measured publisher lifetime.
    pub duration_ns: u64,
}

/// Running mixed update publisher.
pub struct MixedPublisher {
    stop: watch::Sender<bool>,
    published: Arc<AtomicU64>,
    started: Instant,
    rate: u64,
    task: JoinHandle<()>,
}

impl MixedPublisher {
    /// Returns the updates published so far without stopping the producer.
    pub fn published_updates(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    /// Stops publication and returns stable integer counters.
    pub async fn finish(self) -> MixedLoadReport {
        let _ = self.stop.send(true);
        let _ = self.task.await;
        MixedLoadReport {
            configured_events_per_second: self.rate,
            published_updates: self.published.load(Ordering::Relaxed),
            published_during_measurement: 0,
            applied_during_measurement: 0,
            applied_updates: 0,
            duration_ns: u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        }
    }
}

/// Starts repeated `LaneUpdated` messages at a bounded deterministic rate.
pub fn spawn_mixed_publisher(
    bus: UpdateBus,
    core_address: Address,
    lane_asset: Address,
    lane_slot0: U256,
    cursor: lunarbase_client::model::ChainCursor,
    events_per_second: u64,
) -> MixedPublisher {
    let (stop, mut stopped) = watch::channel(false);
    let published = Arc::new(AtomicU64::new(0));
    let task_count = published.clone();
    let task = tokio::spawn(async move {
        let period = Duration::from_nanos(1_000_000_000_u64.div_ceil(events_per_second));
        let mut ticker = interval(period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Burst);
        let mut sequence = cursor.source_sequence.unwrap_or(0);
        loop {
            tokio::select! {
                biased;
                changed = stopped.changed() => {
                    if changed.is_err() || *stopped.borrow() {
                        return;
                    }
                }
                _ = ticker.tick() => {
                    sequence = sequence.saturating_add(1);
                    let update = lane_update(
                        core_address,
                        lane_asset,
                        lane_slot0,
                        &cursor,
                        sequence,
                    );
                    if !bus.publish(update) {
                        return;
                    }
                    task_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
    MixedPublisher {
        stop,
        published,
        started: Instant::now(),
        rate: events_per_second,
        task,
    }
}

/// Waits until the actual connected reducer publishes at least `expected`.
pub async fn wait_for_reducer_sequence(
    client: &ConnectedQuoteClient,
    expected: u64,
) -> Result<u64, String> {
    let started = Instant::now();
    loop {
        let health = client.health().map_err(|error| error.to_string())?;
        if !health.ready {
            return Err("mixed benchmark reducer became unready".into());
        }
        let actual = health
            .cursor
            .and_then(|cursor| cursor.source_sequence)
            .unwrap_or(0);
        if actual >= expected {
            return Ok(actual);
        }
        if started.elapsed() >= Duration::from_secs(2) {
            return Err(format!(
                "mixed benchmark reducer reached sequence {actual}, expected {expected}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn lane_update(
    core_address: Address,
    lane_asset: Address,
    lane_slot0: U256,
    cursor: &lunarbase_client::model::ChainCursor,
    sequence: u64,
) -> ChainUpdate {
    let mut asset_topic = [0_u8; 32];
    asset_topic[12..].copy_from_slice(lane_asset.as_slice());
    ChainUpdate::Log(ContractLog {
        address: core_address,
        transaction_hash: None,
        topics: vec![core::LaneUpdated::SIGNATURE_HASH, B256::new(asset_topic)],
        data: Bytes::copy_from_slice(&lane_slot0.to_be_bytes::<32>()),
        removed: false,
        cursor: lunarbase_client::model::ChainCursor {
            transaction_index: None,
            log_index: None,
            source_sequence: Some(sequence),
            source_sub_index: Some(0),
            commitment: Commitment::Realtime,
            ..cursor.clone()
        },
    })
}
