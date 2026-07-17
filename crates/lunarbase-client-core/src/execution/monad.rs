//! Monad execution-event ordering and runtime source.

use crate::{
    BackfillRequest, ChainCursor, ChainEventSource, ChainUpdate, ContractFilter, ContractLog,
    ExecutionEvent, ExecutionEventReader, ExecutionHead, ExecutionLog, Network, NormalizedBackend,
    SourceError, SourceStream,
};
use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use std::sync::Arc;

/// Tracks Monad execution-event sequence/sub-index pairs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MonadRingTracker {
    last_sequence: Option<u64>,
    last_sub_index: u32,
}

impl MonadRingTracker {
    /// Accepts a strictly contiguous sequence from a complete raw event ring.
    pub fn observe(&mut self, sequence: u64) -> Result<bool, SourceError> {
        self.observe_subindex(sequence, 0)
    }

    /// Accepts a contiguous sequence/sub-index pair and rejects missing events.
    pub fn observe_subindex(&mut self, sequence: u64, sub_index: u32) -> Result<bool, SourceError> {
        match self.last_sequence {
            None => {
                self.last_sequence = Some(sequence);
                self.last_sub_index = sub_index;
                Ok(true)
            }
            Some(last) if sequence == last && sub_index > self.last_sub_index => {
                self.last_sub_index = sub_index;
                Ok(true)
            }
            Some(last) if sequence == last => Ok(false),
            Some(last) if sequence == last.saturating_add(1) => {
                self.last_sequence = Some(sequence);
                self.last_sub_index = sub_index;
                Ok(true)
            }
            Some(_) => Err(SourceError::Gap(
                "Monad execution-event sequence gap".into(),
            )),
        }
    }

    /// Accepts sparse parser subscriptions that report ring gaps explicitly.
    pub fn observe_sparse(&mut self, sequence: u64, sub_index: u32) -> Result<bool, SourceError> {
        match self.last_sequence {
            None => {
                self.last_sequence = Some(sequence);
                self.last_sub_index = sub_index;
                Ok(true)
            }
            Some(last) if sequence < last => Err(SourceError::Gap(
                "Monad execution-event sequence regression".into(),
            )),
            Some(last) if sequence == last && sub_index <= self.last_sub_index => Ok(false),
            Some(last) if sequence == last => {
                self.last_sub_index = sub_index;
                Ok(true)
            }
            Some(_) => {
                self.last_sequence = Some(sequence);
                self.last_sub_index = sub_index;
                Ok(true)
            }
        }
    }

    /// Rewinds sequence tracking after an explicit parser or ring gap.
    pub fn rewind(&mut self) {
        self.last_sequence = None;
        self.last_sub_index = 0;
    }
}

/// Converts Monad execution events into the normalized runtime model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonadExecutionNormalizer {
    chain_id: u64,
    tracker: MonadRingTracker,
}

impl MonadExecutionNormalizer {
    /// Creates a normalizer for one Monad chain id.
    pub fn new(chain_id: u64) -> Self {
        Self {
            chain_id,
            tracker: MonadRingTracker::default(),
        }
    }

    /// Converts one execution event, suppressing duplicate log positions.
    pub fn normalize(&mut self, event: ExecutionEvent) -> Result<Option<ChainUpdate>, SourceError> {
        match event {
            ExecutionEvent::Head(head) => Ok(Some(self.normalize_head(head))),
            ExecutionEvent::Log(log) => self.normalize_log(log),
            ExecutionEvent::Gap { cursor, reason } => {
                self.tracker.rewind();
                Ok(Some(ChainUpdate::Gap { cursor, reason }))
            }
        }
    }

    /// Converts a filtered transaction log while preserving source ordering.
    pub fn normalize_log(&mut self, log: ExecutionLog) -> Result<Option<ChainUpdate>, SourceError> {
        if !self
            .tracker
            .observe_sparse(log.sequence, log.source_sub_index)?
        {
            return Ok(None);
        }
        Ok(Some(ChainUpdate::Log(ContractLog {
            address: log.address,
            topics: log.topics,
            data: log.data,
            removed: false,
            cursor: ChainCursor {
                chain_id: self.chain_id,
                block_number: log.block_number,
                block_hash: log.block_hash,
                transaction_index: Some(log.transaction_index),
                log_index: Some(log.log_index),
                source_sequence: Some(log.sequence),
                source_sub_index: Some(log.source_sub_index),
                commitment: log.commitment,
            },
        })))
    }

    /// Converts a Monad block lifecycle event into a normalized head.
    pub fn normalize_head(&self, head: ExecutionHead) -> ChainUpdate {
        ChainUpdate::Head(ChainCursor {
            chain_id: self.chain_id,
            block_number: head.block_number,
            block_hash: head.block_hash,
            transaction_index: None,
            log_index: None,
            source_sequence: Some(head.sequence),
            source_sub_index: None,
            commitment: head.commitment,
        })
    }

    /// Resets sequence tracking and emits a fail-closed gap update.
    pub fn normalize_gap(&mut self, reason: impl Into<String>) -> ChainUpdate {
        self.tracker.rewind();
        ChainUpdate::Gap {
            cursor: None,
            reason: reason.into(),
        }
    }
}

/// Universal Monad source engine parameterized by reader and canonical backend.
///
/// The same runtime can consume parser WebSockets today and a native
/// shared-memory/event-ring reader beside a Monad node later.
pub struct MonadExecutionEngine<R, B> {
    reader: Arc<R>,
    canonical: Arc<B>,
    chain_id: u64,
}

impl<R, B> MonadExecutionEngine<R, B> {
    /// Creates a Monad engine from an execution reader and recovery backend.
    pub fn new(reader: Arc<R>, canonical: Arc<B>, chain_id: u64) -> Self {
        Self {
            reader,
            canonical,
            chain_id,
        }
    }

    /// Returns the execution-event reader.
    pub fn reader(&self) -> &Arc<R> {
        &self.reader
    }

    /// Returns the canonical snapshot/backfill backend.
    pub fn canonical(&self) -> &Arc<B> {
        &self.canonical
    }
}

#[async_trait]
impl<R, B> ChainEventSource for MonadExecutionEngine<R, B>
where
    R: ExecutionEventReader + 'static,
    B: NormalizedBackend + 'static,
{
    fn network(&self) -> Network {
        Network::Monad
    }

    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        self.canonical.snapshot_cursor(Network::Monad).await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.canonical.backfill(request).await
    }

    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        let events = self.reader.subscribe_execution(filter).await?;
        let chain_id = self.chain_id;
        let output = stream! {
            futures_util::pin_mut!(events);
            let mut normalizer = MonadExecutionNormalizer::new(chain_id);
            while let Some(item) = events.next().await {
                match item {
                    Ok(event) => match normalizer.normalize(event) {
                        Ok(Some(update)) => yield Ok(update),
                        Ok(None) => {}
                        Err(error) => {
                            yield Ok(normalizer.normalize_gap(error.to_string()));
                            break;
                        }
                    },
                    Err(error) => {
                        yield Err(error);
                        break;
                    }
                }
            }
        };
        Ok(Box::pin(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Commitment, ExecutionEventStream};
    use async_trait::async_trait;
    use futures_util::{stream as futures_stream, StreamExt};
    use lunarbase_math::Address;

    struct TestReader;

    #[async_trait]
    impl ExecutionEventReader for TestReader {
        async fn subscribe_execution(
            &self,
            _filter: ContractFilter,
        ) -> Result<ExecutionEventStream, SourceError> {
            Ok(Box::pin(futures_stream::iter([
                Ok(ExecutionEvent::Head(ExecutionHead {
                    sequence: 100,
                    block_number: 7,
                    block_hash: Some([7; 32]),
                    commitment: Commitment::Realtime,
                })),
                Ok(ExecutionEvent::Log(ExecutionLog {
                    sequence: 104,
                    source_sub_index: 0,
                    block_number: 7,
                    block_hash: Some([7; 32]),
                    transaction_index: 1,
                    log_index: 2,
                    address: Address::ZERO,
                    topics: Vec::new(),
                    data: Vec::new(),
                    commitment: Commitment::Realtime,
                })),
            ])))
        }
    }

    struct TestCanonical;

    #[async_trait]
    impl NormalizedBackend for TestCanonical {
        async fn snapshot_cursor(&self, _network: Network) -> Result<ChainCursor, SourceError> {
            Ok(ChainCursor::block(
                143,
                7,
                Some([7; 32]),
                Commitment::Finalized,
            ))
        }

        async fn backfill(
            &self,
            _request: BackfillRequest,
        ) -> Result<Vec<ContractLog>, SourceError> {
            Ok(Vec::new())
        }

        async fn subscribe(
            &self,
            _network: Network,
            _filter: ContractFilter,
        ) -> Result<SourceStream, SourceError> {
            Err(SourceError::Unavailable("not used by engine test".into()))
        }
    }

    #[tokio::test]
    async fn universal_engine_normalizes_reader_events_once() {
        let engine = MonadExecutionEngine::new(Arc::new(TestReader), Arc::new(TestCanonical), 143);
        let stream = engine
            .subscribe(ContractFilter {
                address: Address::ZERO,
                topics: Vec::new(),
            })
            .await
            .unwrap();
        let updates = stream.collect::<Vec<_>>().await;
        assert!(matches!(updates[0], Ok(ChainUpdate::Head(_))));
        assert!(matches!(updates[1], Ok(ChainUpdate::Log(_))));
    }
}
