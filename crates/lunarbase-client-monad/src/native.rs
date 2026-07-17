//! Linux shared-memory reader for the official Monad execution event ring.

use lunarbase_client_core::{
    BackfillRequest, BootstrapSnapshot, ChainCursor, ChainDataSource, Checkpoint, Commitment,
    ContractFilter, ContractLog, DeploymentConfig, Network, RpcHttpBackend, RpcHttpClient,
    RpcSnapshotProvider, SourceError, SourceStream,
};
use lunarbase_math::{Address, U256};
use monad_event_ring::{
    DecodedEventRing, EventDescriptorInfo, EventNextResult, EventPayloadResult, EventRingPath,
};
use monad_exec_events::{
    ExecEvent, ExecEventDescriptorExt, ExecEventReaderExt, ExecEventRing, ExecEventType,
};
use std::{path::PathBuf, thread, time::Duration};
use tokio::sync::mpsc;

use crate::{
    ExecutionEvent, ExecutionEventStream, ExecutionHead, ExecutionLog, MonadExecutionNormalizer,
};

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
    /// Poll delay when the producer has not published another descriptor.
    pub poll_interval: Duration,
}

impl MonadEventRingConfig {
    /// Validates identity and memory bounds before opening shared memory.
    pub fn validate(&self) -> Result<(), SourceError> {
        if self.chain_id == 0 || self.core == Address::ZERO || self.queue_bound == 0 {
            return Err(SourceError::Unavailable(
                "Monad ring chain, Core, and queue bound must be valid".into(),
            ));
        }
        Ok(())
    }
}

/// Production Monad source backed by the official native event-ring SDK.
pub struct MonadEventRingSource {
    config: MonadEventRingConfig,
    canonical: RpcHttpBackend,
}

impl MonadEventRingSource {
    /// Creates a native realtime source plus RPC bootstrap/recovery backend.
    pub fn new(
        config: MonadEventRingConfig,
        rpc_endpoint: impl Into<String>,
    ) -> Result<Self, SourceError> {
        config.validate()?;
        let canonical = RpcHttpBackend::new(
            RpcHttpClient::new(rpc_endpoint),
            Network::Monad,
            config.chain_id,
            "finalized",
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
    tokio::task::spawn_blocking(move || read_ring(config, filter, sender));
    Ok(Box::pin(async_stream::stream! {
        while let Some(event) = receiver.recv().await {
            yield event;
        }
    }))
}

fn read_ring(
    config: MonadEventRingConfig,
    filter: ContractFilter,
    sender: mpsc::Sender<Result<ExecutionEvent, SourceError>>,
) {
    let path = match EventRingPath::resolve(config.event_ring_path) {
        Ok(path) => path,
        Err(error) => return send_error(&sender, format!("resolve Monad event ring: {error}")),
    };
    let ring = match ExecEventRing::new(path) {
        Ok(ring) => ring,
        Err(error) => return send_error(&sender, format!("open Monad event ring: {error}")),
    };
    let mut reader = ring.create_reader();
    reader.consensus_prev(Some(ExecEventType::BlockStart));
    let mut block_log_index = 0u32;

    loop {
        let descriptor = match reader.next_descriptor() {
            EventNextResult::Gap => {
                send_gap(&sender, "Monad event-ring descriptor gap");
                return;
            }
            EventNextResult::NotReady => {
                if sender.is_closed() {
                    return;
                }
                thread::sleep(config.poll_interval);
                continue;
            }
            EventNextResult::Ready(descriptor) => descriptor,
        };
        let info = descriptor.info();
        let block_number = descriptor.get_block_number();
        let event = match descriptor.try_read() {
            EventPayloadResult::Expired => {
                send_gap(&sender, "Monad event-ring payload expired");
                return;
            }
            EventPayloadResult::Ready(event) => event,
        };
        let normalized = convert_event(event, info, block_number, &filter, &mut block_log_index);
        if let Some(event) = normalized {
            if sender.blocking_send(Ok(event)).is_err() {
                return;
            }
        }
    }
}

fn convert_event(
    event: ExecEvent,
    info: EventDescriptorInfo<monad_exec_events::ExecEventDecoder>,
    block_number: Option<u64>,
    filter: &ContractFilter,
    block_log_index: &mut u32,
) -> Option<ExecutionEvent> {
    let sequence = info.seqno;
    match event {
        ExecEvent::BlockStart(start) => {
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
            block_number?,
            Some(end.eth_block_hash.bytes),
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
            let address = Address(txn_log.address.bytes);
            let topics = decode_topics(&topic_bytes);
            if address != filter.address
                || (!filter.topics.is_empty()
                    && topics
                        .first()
                        .is_none_or(|topic| !filter.topics.contains(topic)))
            {
                return None;
            }
            let log_index = *block_log_index;
            *block_log_index = block_log_index.checked_add(1)?;
            Some(ExecutionEvent::Log(ExecutionLog {
                sequence,
                source_sub_index: txn_log.index,
                block_number: block_number?,
                block_hash: None,
                transaction_index: u32::try_from(txn_index).ok()?,
                log_index,
                address,
                topics,
                data: data_bytes.into_vec(),
                commitment: Commitment::Realtime,
            }))
        }
        _ => None,
    }
}

fn decode_topics(bytes: &[u8]) -> Vec<U256> {
    bytes
        .chunks_exact(32)
        .map(|chunk| U256::from_be_bytes::<32>(chunk.try_into().expect("exact topic word")))
        .collect()
}

fn head(
    sequence: u64,
    block_number: u64,
    block_hash: Option<[u8; 32]>,
    commitment: Commitment,
) -> ExecutionEvent {
    ExecutionEvent::Head(ExecutionHead {
        sequence,
        block_number,
        block_hash,
        commitment,
    })
}

fn send_gap(sender: &mpsc::Sender<Result<ExecutionEvent, SourceError>>, reason: &str) {
    let _ = sender.blocking_send(Ok(ExecutionEvent::Gap {
        cursor: None,
        reason: reason.into(),
    }));
}

fn send_error(sender: &mpsc::Sender<Result<ExecutionEvent, SourceError>>, reason: String) {
    let _ = sender.blocking_send(Err(SourceError::Unavailable(reason)));
}
