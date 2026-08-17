//! Monad execution-event model and normalization.

use lunarbase_client::model::{
    BlockRef, ChainCursor, ChainUpdate, Commitment, ContractLog, SourceError,
};
use lunarbase_client::source::SourceStream;
use lunarbase_math::{Address, B256, Bytes};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Controls when proposal logs leave the Monad source.
pub enum MonadDeliveryMode {
    /// Emits matching logs as soon as their raw execution events arrive.
    #[default]
    Realtime,
    /// Emits matching logs in deterministic order after successful block execution.
    BlockOrdered,
    /// Emits only logs belonging to the proposal selected by consensus finality.
    Finalized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Block lifecycle notification from an execution-event source.
pub struct ExecutionHead {
    /// Monotonic source sequence.
    pub sequence: u64,
    /// EVM-visible Monad block height.
    pub block_number: u64,
    /// Block identifier supplied by the execution source, when available.
    pub block_hash: Option<B256>,
    /// Parent block or proposal identifier, when supplied by the source.
    pub parent_hash: Option<B256>,
    /// Lifecycle confidence represented by this notification.
    pub commitment: Commitment,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// EVM log before normalization into the common client model.
pub struct ExecutionLog {
    /// Monotonic source sequence.
    pub sequence: u64,
    /// Deterministic position inside one source sequence.
    pub source_sub_index: u32,
    /// EVM-visible block that executed the log.
    pub block_number: u64,
    /// Hash of the executing block, when supplied by the source.
    pub block_hash: Option<B256>,
    /// Transaction position within the executing block.
    pub transaction_index: u32,
    /// Log position within the executing block.
    pub log_index: u32,
    /// EVM contract that emitted the log.
    pub address: Address,
    /// Indexed event topics, including signature topic zero.
    pub topics: Vec<B256>,
    /// Unindexed ABI-encoded event payload.
    pub data: Bytes,
    /// Whether this log retracts a previously published proposal log.
    pub removed: bool,
    /// Lifecycle confidence inherited from the nearest block notification.
    pub commitment: Commitment,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Raw execution lifecycle item.
pub enum ExecutionEvent {
    /// Announces a Monad block lifecycle transition.
    Head(ExecutionHead),
    /// Carries one EVM log in execution order.
    Log(ExecutionLog),
    /// Announces that a previously published proposal was abandoned.
    Reorg {
        /// Last head belonging to the abandoned proposal.
        old_head: ExecutionHead,
        /// Replacement proposal selected by the observed lifecycle.
        new_head: ExecutionHead,
    },
    /// Reports lost, expired, or non-monotonic execution-event data.
    Gap {
        /// Last known normalized position, when the source can identify it.
        cursor: Option<ChainCursor>,
        /// Diagnostic reason requiring canonical recovery.
        reason: String,
    },
}

impl ExecutionEvent {
    /// Returns a conservative retained-memory charge for transport queues.
    pub fn retained_bytes(&self) -> usize {
        let dynamic = match self {
            Self::Log(log) => log
                .topics
                .len()
                .saturating_mul(std::mem::size_of::<B256>())
                .saturating_add(log.data.len()),
            Self::Gap { reason, .. } => reason.len(),
            Self::Head(_) | Self::Reorg { .. } => 0,
        };
        std::mem::size_of::<Self>().saturating_add(dynamic)
    }
}

/// Stream emitted by execution-event readers.
pub type ExecutionEventStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ExecutionEvent, SourceError>> + Send>>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
/// Tracks accepted source sequence positions.
pub struct MonadSequenceTracker {
    /// Latest source sequence accepted from the filtered stream.
    last_sequence: Option<u64>,
    /// Latest event position accepted within `last_sequence`.
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

    /// Resets ordering after an explicit source gap.
    pub fn rewind(&mut self) {
        self.last_sequence = None;
        self.last_sub_index = 0;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Converts execution events into common runtime updates.
pub struct MonadExecutionNormalizer {
    /// EIP-155 chain identifier attached to every normalized cursor.
    chain_id: u64,
    /// Duplicate and regression guard for source messages.
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
            ExecutionEvent::Reorg { old_head, new_head } => Ok(Some(ChainUpdate::Reorg {
                old_head: self.head_ref(old_head),
                new_head: self.head_ref(new_head),
            })),
            ExecutionEvent::Gap { cursor, reason } => {
                self.tracker.rewind();
                Ok(Some(ChainUpdate::Gap { cursor, reason }))
            }
        }
    }

    /// Converts one EVM log while preserving source ordering.
    pub fn normalize_log(&mut self, log: ExecutionLog) -> Result<Option<ChainUpdate>, SourceError> {
        if !self
            .tracker
            .observe_sparse(log.sequence, log.source_sub_index)?
        {
            return Ok(None);
        }
        Ok(Some(ChainUpdate::Log(ContractLog {
            address: log.address,
            transaction_hash: None,
            topics: log.topics,
            data: log.data,
            removed: log.removed,
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
        ChainUpdate::Head(self.head_ref(head))
    }

    fn head_ref(&self, head: ExecutionHead) -> BlockRef {
        BlockRef {
            cursor: ChainCursor {
                chain_id: self.chain_id,
                block_number: head.block_number,
                execution_block_number: head.block_number,
                block_hash: head.block_hash,
                transaction_index: None,
                log_index: None,
                source_sequence: Some(head.sequence),
                source_sub_index: None,
                commitment: head.commitment,
            },
            parent_hash: head.parent_hash,
        }
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
