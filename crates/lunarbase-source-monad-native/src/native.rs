//! Linux shared-memory reader for the official Monad execution event ring.

use lunarbase_client::bootstrap::BootstrapSnapshot;
use lunarbase_client::model::{
    BackfillRequest, ChainCursor, Checkpoint, Commitment, ContractFilter, ContractLog,
    DeploymentConfig, MIN_UPDATE_QUEUE_BYTE_CAPACITY, Network, SourceError,
};
use lunarbase_client::source::{ChainDataSource, SourceStream};
use lunarbase_math::{Address, B256};
use lunarbase_source_evm::rpc::backend::RpcHttpBackend;
use lunarbase_source_evm::rpc::client::RpcHttpClient;
use lunarbase_source_evm::rpc::snapshot::RpcSnapshotProvider;
use monad_event_ring::{
    DecodedEventRing, EventDescriptorInfo, EventNextResult, EventPayloadResult, EventRingPath,
};
use monad_exec_events::{
    CommitStateBlockBuilder, ExecEvent, ExecEventDescriptorExt, ExecEventReaderExt, ExecEventRing,
    ExecEventType, ExecutedBlockBuilder,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{Semaphore, mpsc},
};

use lunarbase_source_monad::execution::{
    ExecutionEvent, ExecutionEventStream, ExecutionHead, ExecutionLog, MonadDeliveryMode,
    MonadExecutionNormalizer,
};

mod lifecycle;
mod queue;

use queue::{QueuedExecutionEvent, send_error, send_gap, send_result};

const MAX_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Native event-ring settings for a colocated Monad execution node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonadEventRingConfig {
    /// Event-ring file or hugetlbfs-resolved short name.
    pub event_ring_path: PathBuf,
    /// Only logs emitted by this Core contract enter the reducer.
    pub core: Address,
    /// Monad chain id attached to normalized cursors.
    pub chain_id: u64,
    /// Bounded handoff from the blocking shared-memory reader to Tokio.
    pub queue_bound: usize,
    /// Maximum retained bytes in the blocking-reader to Tokio handoff.
    pub queue_byte_bound: usize,
    /// Poll delay when the producer has not published another descriptor.
    pub poll_interval: Duration,
    /// Point in proposal lifecycle at which matching logs are published.
    pub delivery_mode: MonadDeliveryMode,
    /// Emits removal logs for branches abandoned after earlier publication.
    pub emit_removed_logs: bool,
}

impl MonadEventRingConfig {
    /// Validates identity and memory bounds before opening shared memory.
    pub fn validate(&self) -> Result<(), SourceError> {
        if self.chain_id == 0
            || self.core == Address::ZERO
            || self.queue_bound == 0
            || self.queue_byte_bound < MIN_UPDATE_QUEUE_BYTE_CAPACITY
            || self.queue_byte_bound > u32::MAX as usize
            || self.poll_interval.is_zero()
            || self.poll_interval > MAX_POLL_INTERVAL
        {
            return Err(SourceError::Unavailable(
                "Monad ring identity, count/byte queue, and poll bounds must be valid".into(),
            ));
        }
        Ok(())
    }
}

/// Production Monad source backed by the official native event-ring SDK.
pub struct MonadEventRingSource {
    /// Validated shared-memory identity, filtering, and resource bounds.
    config: MonadEventRingConfig,
    /// Finalized HTTP authority used for bootstrap and canonical recovery.
    canonical: RpcHttpBackend,
}

impl MonadEventRingSource {
    /// Creates a native lifecycle source plus RPC bootstrap/recovery backend.
    pub fn new(
        config: MonadEventRingConfig,
        rpc_endpoint: impl Into<String>,
    ) -> Result<Self, SourceError> {
        config.validate()?;
        let canonical = RpcHttpBackend::new(
            RpcHttpClient::new(rpc_endpoint).map_err(SourceError::from)?,
            Network::Monad,
            config.chain_id,
            "latest",
        );
        Ok(Self { config, canonical })
    }
}

impl ChainDataSource for MonadEventRingSource {
    fn network(&self) -> Network {
        Network::Monad
    }

    async fn snapshot(
        &self,
        deployment: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        RpcSnapshotProvider::new(
            self.canonical.rpc().clone(),
            self.canonical.snapshot_tag().to_owned(),
        )
        .snapshot(deployment)
        .await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.canonical.backfill(request).await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        if filter.address != self.config.core {
            return Err(SourceError::NetworkMismatch);
        }
        let raw = connect_event_ring(self.config.clone(), filter).await?;
        Ok(MonadExecutionNormalizer::new(self.config.chain_id).normalize_stream(raw))
    }

    async fn canonical_head(&self) -> Result<ChainCursor, SourceError> {
        self.canonical.snapshot_cursor(Network::Monad).await
    }

    async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        self.canonical.validate_checkpoint(checkpoint).await
    }
}

/// Opens the native ring and exposes raw execution lifecycle events.
pub async fn connect_event_ring(
    config: MonadEventRingConfig,
    filter: ContractFilter,
) -> Result<ExecutionEventStream, SourceError> {
    config.validate()?;
    let (sender, mut receiver) = mpsc::channel(config.queue_bound);
    let byte_budget = Arc::new(Semaphore::new(config.queue_byte_bound));
    let runtime = Handle::current();
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = cancelled.clone();
    let worker = thread::Builder::new()
        .name("lunarbase-monad-ring".into())
        .spawn(move || {
            read_ring(
                config,
                filter,
                sender,
                byte_budget,
                runtime,
                worker_cancelled,
            )
        })
        .map_err(|error| {
            SourceError::Unavailable(format!("spawn Monad event-ring reader: {error}"))
        })?;
    let guard = RingReaderGuard {
        cancelled,
        worker: Some(worker),
    };
    Ok(Box::pin(async_stream::stream! {
        let _guard = guard;
        while let Some(queued) = receiver.recv().await {
            let QueuedExecutionEvent { result, _byte_permit } = queued;
            yield result;
        }
    }))
}

fn read_ring(
    config: MonadEventRingConfig,
    filter: ContractFilter,
    sender: mpsc::Sender<QueuedExecutionEvent>,
    byte_budget: Arc<Semaphore>,
    runtime: Handle,
    cancelled: Arc<AtomicBool>,
) {
    let path = match EventRingPath::resolve(config.event_ring_path) {
        Ok(path) => path,
        Err(error) => {
            return send_error(
                &sender,
                &byte_budget,
                &runtime,
                config.queue_byte_bound,
                format!("resolve Monad event ring: {error}"),
            );
        }
    };
    let ring = match ExecEventRing::new(path) {
        Ok(ring) => ring,
        Err(error) => {
            return send_error(
                &sender,
                &byte_budget,
                &runtime,
                config.queue_byte_bound,
                format!("open Monad event ring: {error}"),
            );
        }
    };
    let mut reader = ring.create_reader();
    reader.consensus_prev(Some(ExecEventType::BlockStart));
    let mut block_log_index = 0u32;
    let mut lifecycle = (config.delivery_mode != MonadDeliveryMode::Realtime
        || config.emit_removed_logs)
        .then(|| CommitStateBlockBuilder::new(ExecutedBlockBuilder::new(false, false)));

    loop {
        if cancelled.load(Ordering::Acquire) {
            return;
        }
        let descriptor = match reader.next_descriptor() {
            EventNextResult::Gap => {
                send_gap(
                    &sender,
                    &byte_budget,
                    &runtime,
                    config.queue_byte_bound,
                    "Monad event-ring descriptor gap",
                );
                return;
            }
            EventNextResult::NotReady => {
                if sender.is_closed() || cancelled.load(Ordering::Acquire) {
                    return;
                }
                thread::sleep(config.poll_interval);
                continue;
            }
            EventNextResult::Ready(descriptor) => descriptor,
        };
        let info = descriptor.info();
        if let Some(builder) = lifecycle.as_mut() {
            let update = catch_unwind(AssertUnwindSafe(|| {
                builder.process_event_descriptor(&descriptor)
            }));
            match update {
                Err(_) => {
                    send_gap(
                        &sender,
                        &byte_budget,
                        &runtime,
                        config.queue_byte_bound,
                        "Monad official block builder rejected the lifecycle",
                    );
                    return;
                }
                Ok(Some(Ok(update))) => {
                    let events = match lifecycle::convert_update(
                        update,
                        info.seqno,
                        config.delivery_mode,
                        config.emit_removed_logs,
                        config.chain_id,
                        &filter,
                    ) {
                        Ok(events) => events,
                        Err(error) => {
                            let _ = send_result(
                                &sender,
                                &byte_budget,
                                &runtime,
                                config.queue_byte_bound,
                                Err(error),
                            );
                            return;
                        }
                    };
                    for event in events {
                        if !send_result(
                            &sender,
                            &byte_budget,
                            &runtime,
                            config.queue_byte_bound,
                            Ok(event),
                        ) {
                            return;
                        }
                    }
                }
                Ok(Some(Err(_))) => {
                    send_gap(
                        &sender,
                        &byte_budget,
                        &runtime,
                        config.queue_byte_bound,
                        "Monad official block builder rejected the event stream",
                    );
                    return;
                }
                Ok(None) => {}
            }
            if config.delivery_mode != MonadDeliveryMode::Realtime {
                continue;
            }
        }
        let block_number = descriptor.get_block_number();
        let event = match descriptor.try_read() {
            EventPayloadResult::Expired => {
                send_gap(
                    &sender,
                    &byte_budget,
                    &runtime,
                    config.queue_byte_bound,
                    "Monad event-ring payload expired",
                );
                return;
            }
            EventPayloadResult::Ready(event) => event,
        };
        match convert_event(
            event,
            info,
            block_number,
            config.chain_id,
            &filter,
            &mut block_log_index,
        ) {
            Ok(Some(event)) => {
                if !send_result(
                    &sender,
                    &byte_budget,
                    &runtime,
                    config.queue_byte_bound,
                    Ok(event),
                ) {
                    return;
                }
            }
            Ok(None) => {}
            Err(error) => {
                let _ = send_result(
                    &sender,
                    &byte_budget,
                    &runtime,
                    config.queue_byte_bound,
                    Err(error),
                );
                return;
            }
        }
    }
}

fn convert_event(
    event: ExecEvent,
    info: EventDescriptorInfo<monad_exec_events::ExecEventDecoder>,
    block_number: Option<u64>,
    chain_id: u64,
    filter: &ContractFilter,
    block_log_index: &mut u32,
) -> Result<Option<ExecutionEvent>, SourceError> {
    let sequence = info.seqno;
    Ok(match event {
        ExecEvent::RecordError(_) => Some(ExecutionEvent::Gap {
            cursor: None,
            reason: "Monad execution ring reported a dropped record".into(),
        }),
        ExecEvent::BlockStart(start) => {
            if start.chain_id.limbs != [chain_id, 0, 0, 0] {
                return Err(SourceError::NetworkMismatch);
            }
            *block_log_index = 0;
            Some(head(
                sequence,
                start.block_tag.block_number,
                None,
                Commitment::Realtime,
            ))
        }
        ExecEvent::BlockEnd(end) => Some(head(
            sequence,
            required_block_number(block_number)?,
            Some(B256::new(end.eth_block_hash.bytes)),
            Commitment::Realtime,
        )),
        ExecEvent::BlockQC(qc) => Some(head(
            sequence,
            qc.block_tag.block_number,
            None,
            Commitment::Canonical,
        )),
        ExecEvent::BlockFinalized(tag) => Some(head(
            sequence,
            tag.block_number,
            None,
            Commitment::Finalized,
        )),
        ExecEvent::BlockVerified(verified) => Some(head(
            sequence,
            verified.block_number,
            None,
            Commitment::Finalized,
        )),
        ExecEvent::BlockReject(_) => Some(ExecutionEvent::Gap {
            cursor: None,
            reason: "Monad execution proposal rejected; canonical recovery required".into(),
        }),
        ExecEvent::TxnLog {
            txn_index,
            txn_log,
            topic_bytes,
            data_bytes,
        } => {
            let log_index = *block_log_index;
            *block_log_index = block_log_index
                .checked_add(1)
                .ok_or_else(|| SourceError::Gap("Monad block log index overflow".into()))?;
            let address = Address::new(txn_log.address.bytes);
            let topics = decode_topics(&topic_bytes)?;
            if address != filter.address
                || (!filter.topics.is_empty()
                    && topics
                        .first()
                        .is_none_or(|topic| !filter.topics.contains(topic)))
            {
                return Ok(None);
            }
            Some(ExecutionEvent::Log(ExecutionLog {
                sequence,
                source_sub_index: txn_log.index,
                block_number: required_block_number(block_number)?,
                block_hash: None,
                transaction_index: u32::try_from(txn_index).map_err(|_| {
                    SourceError::Gap("Monad transaction index exceeds uint32".into())
                })?,
                log_index,
                address,
                topics,
                data: data_bytes.into_vec().into(),
                removed: false,
                commitment: Commitment::Realtime,
            }))
        }
        _ => None,
    })
}

fn decode_topics(bytes: &[u8]) -> Result<Vec<B256>, SourceError> {
    if !bytes.len().is_multiple_of(32) {
        return Err(SourceError::Gap(
            "Monad execution log topics are not aligned to bytes32".into(),
        ));
    }
    Ok(bytes.chunks_exact(32).map(B256::from_slice).collect())
}

fn required_block_number(block_number: Option<u64>) -> Result<u64, SourceError> {
    block_number.ok_or_else(|| SourceError::Gap("Monad execution event has no block number".into()))
}

fn head(
    sequence: u64,
    block_number: u64,
    block_hash: Option<B256>,
    commitment: Commitment,
) -> ExecutionEvent {
    ExecutionEvent::Head(ExecutionHead {
        sequence,
        block_number,
        block_hash,
        commitment,
    })
}

struct RingReaderGuard {
    cancelled: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Drop for RingReaderGuard {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            if let Ok(runtime) = Handle::try_current() {
                runtime.spawn_blocking(move || {
                    let _ = worker.join();
                });
            } else {
                let _ = thread::Builder::new()
                    .name("lunarbase-monad-ring-reaper".into())
                    .spawn(move || {
                        let _ = worker.join();
                    });
            }
        }
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
