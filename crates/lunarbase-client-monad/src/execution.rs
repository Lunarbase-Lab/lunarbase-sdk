//! Monad execution-event model and normalization.

use lunarbase_client_core::{
    ChainCursor, ChainUpdate, Commitment, ContractLog, SourceError, SourceStream,
};
use lunarbase_math::{Address, B256, Bytes};

#[derive(Clone, Debug, Eq, PartialEq)]
/// Block lifecycle notification from the parser or native ring.
pub struct ExecutionHead {
    pub sequence: u64,
    pub block_number: u64,
    pub block_hash: Option<B256>,
    pub commitment: Commitment,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// EVM log before normalization into the common client model.
pub struct ExecutionLog {
    pub sequence: u64,
    pub source_sub_index: u32,
    pub block_number: u64,
    pub block_hash: Option<B256>,
    pub transaction_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
    pub commitment: Commitment,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Raw execution lifecycle item.
pub enum ExecutionEvent {
    Head(ExecutionHead),
    Log(ExecutionLog),
    Gap {
        cursor: Option<ChainCursor>,
        reason: String,
    },
}

/// Stream emitted by parser and native event-ring readers.
pub type ExecutionEventStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ExecutionEvent, SourceError>> + Send>>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
/// Tracks sparse parser or contiguous ring sequence positions.
pub struct MonadSequenceTracker {
    last_sequence: Option<u64>,
    last_sub_index: u32,
}

impl MonadSequenceTracker {
    /// Accepts sparse filtered logs while rejecting regression and duplicates.
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

    /// Resets ordering after an explicit parser/ring gap.
    pub fn rewind(&mut self) {
        self.last_sequence = None;
        self.last_sub_index = 0;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Converts parser or native ring events into common runtime updates.
pub struct MonadExecutionNormalizer {
    chain_id: u64,
    tracker: MonadSequenceTracker,
}

impl MonadExecutionNormalizer {
    /// Creates a normalizer for one Monad chain.
    pub fn new(chain_id: u64) -> Self {
        Self {
            chain_id,
            tracker: MonadSequenceTracker::default(),
        }
    }

    /// Converts one execution event and suppresses duplicate log positions.
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

    /// Converts one EVM log while preserving global ring ordering.
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
                execution_block_number: log.block_number,
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
            execution_block_number: head.block_number,
            block_hash: head.block_hash,
            transaction_index: None,
            log_index: None,
            source_sequence: Some(head.sequence),
            source_sub_index: None,
            commitment: head.commitment,
        })
    }

    /// Returns a normalized stream from raw execution events.
    pub fn normalize_stream(mut self, events: ExecutionEventStream) -> SourceStream {
        Box::pin(async_stream::stream! {
            futures_util::pin_mut!(events);
            use futures_util::StreamExt;
            while let Some(item) = events.next().await {
                match item {
                    Ok(event) => match self.normalize(event) {
                        Ok(Some(update)) => yield Ok(update),
                        Ok(None) => {}
                        Err(error) => {
                            yield Ok(ChainUpdate::Gap {
                                cursor: None,
                                reason: error.to_string(),
                            });
                            break;
                        }
                    },
                    Err(error) => {
                        yield Err(error);
                        break;
                    }
                }
            }
        })
    }
}
