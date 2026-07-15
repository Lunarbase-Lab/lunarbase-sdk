use crate::{
    BackfillRequest, ChainCursor, ChainUpdate, Commitment, ContractFilter, ContractLog, Network,
    QuoteEvent, SourceError,
};
use async_trait::async_trait;
use futures_core::Stream;
use lunarbase_math::{Address, U256};
use std::pin::Pin;
use std::sync::Arc;
pub type SourceStream = Pin<Box<dyn Stream<Item = Result<ChainUpdate, SourceError>> + Send>>;

/// Common source boundary. Network-specific adapters only normalize transport
/// semantics; consumers never branch on provider-specific payloads.
#[async_trait]
pub trait ChainEventSource: Send + Sync {
    fn network(&self) -> Network;
    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError>;
    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError>;
    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError>;
}

/// Transport adapter used by Base, Monad, and Arbitrum implementations. A
/// production transport supplies RPC/WS or a local sidecar; the state machine
/// remains identical across networks.
#[async_trait]
pub trait NormalizedBackend: Send + Sync {
    async fn snapshot_cursor(&self, network: Network) -> Result<ChainCursor, SourceError>;
    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError>;
    async fn subscribe(
        &self,
        network: Network,
        filter: ContractFilter,
    ) -> Result<SourceStream, SourceError>;
}

pub struct NetworkSource<B> {
    network: Network,
    backend: Arc<B>,
}

impl<B> NetworkSource<B> {
    pub fn new(network: Network, backend: Arc<B>) -> Self {
        Self { network, backend }
    }
}

#[async_trait]
impl<B: NormalizedBackend + 'static> ChainEventSource for NetworkSource<B> {
    fn network(&self) -> Network {
        self.network
    }
    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError> {
        self.backend.snapshot_cursor(self.network).await
    }
    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.backend.backfill(request).await
    }
    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError> {
        self.backend.subscribe(self.network, filter).await
    }
}

/// Tracks the Base Flashblocks provisional block boundary. A payload change
/// starts a new block; there is deliberately no fixed final flashblock index.
#[derive(Clone, Debug, Default)]
pub struct BaseFlashblocksTracker {
    payload_id: Option<[u8; 32]>,
    block_number: Option<u64>,
    latest_index: Option<u64>,
}

impl BaseFlashblocksTracker {
    pub fn observe(
        &mut self,
        payload_id: [u8; 32],
        block_number: u64,
        index: u64,
    ) -> Result<bool, SourceError> {
        if self.payload_id == Some(payload_id) {
            if self.block_number != Some(block_number) {
                return Err(SourceError::Gap("payload changed block context".into()));
            }
            if self.latest_index.is_some_and(|latest| index < latest) {
                return Err(SourceError::Gap("Flashblocks index regression".into()));
            }
            if self.latest_index == Some(index) {
                return Ok(false);
            }
            self.latest_index = Some(index);
            return Ok(false);
        }
        self.payload_id = Some(payload_id);
        self.block_number = Some(block_number);
        self.latest_index = Some(index);
        Ok(true)
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlashblockHeader {
    pub payload_id: [u8; 32],
    pub block_number: u64,
    pub block_hash: Option<[u8; 32]>,
    pub index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlashblockLog {
    pub header: FlashblockHeader,
    pub transaction_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<U256>,
    pub data: Vec<u8>,
    pub removed: bool,
}

/// Normalizes provider Flashblocks payloads into the common source model. A
/// payload may contain multiple logs at the same flashblock index; equal
/// indexes are therefore deduplicated by the tracker rather than treated as a
/// sequence gap.
#[derive(Clone, Debug, Default)]
pub struct BaseFlashblocksNormalizer {
    tracker: BaseFlashblocksTracker,
    chain_id: u64,
}

impl BaseFlashblocksNormalizer {
    pub fn new(chain_id: u64) -> Self {
        Self {
            tracker: BaseFlashblocksTracker::default(),
            chain_id,
        }
    }

    pub fn normalize_header(
        &mut self,
        header: FlashblockHeader,
    ) -> Result<Option<ChainUpdate>, SourceError> {
        if !self
            .tracker
            .observe(header.payload_id, header.block_number, header.index)?
        {
            return Ok(None);
        }
        Ok(Some(ChainUpdate::Head(ChainCursor {
            chain_id: self.chain_id,
            block_number: header.block_number,
            block_hash: header.block_hash,
            transaction_index: None,
            log_index: None,
            source_sequence: Some(header.index),
            source_sub_index: None,
            commitment: Commitment::Realtime,
        })))
    }

    pub fn normalize_log(&mut self, log: FlashblockLog) -> Result<Vec<ChainUpdate>, SourceError> {
        let header_update = self.normalize_header(log.header.clone())?;
        let mut updates = Vec::with_capacity(2);
        if let Some(update) = header_update {
            updates.push(update);
        }
        updates.push(ChainUpdate::Log(ContractLog {
            address: log.address,
            topics: log.topics,
            data: log.data,
            removed: log.removed,
            cursor: ChainCursor {
                chain_id: self.chain_id,
                block_number: log.header.block_number,
                block_hash: log.header.block_hash,
                transaction_index: Some(log.transaction_index),
                log_index: Some(log.log_index),
                source_sequence: Some(log.header.index),
                source_sub_index: Some(log.log_index),
                commitment: Commitment::Realtime,
            },
        }));
        Ok(updates)
    }

    pub fn reset(&mut self) {
        self.tracker.reset();
    }
}

/// Detects loss in the Monad execution-event ring. A gap is never silently
/// accepted; the caller must resnapshot and rewind the reader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MonadRingTracker {
    last_sequence: Option<u64>,
    last_sub_index: u32,
}

impl MonadRingTracker {
    pub fn observe(&mut self, sequence: u64) -> Result<bool, SourceError> {
        self.observe_subindex(sequence, 0)
    }

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

    /// A filtered `logs` subscription is sparse: the ring sequence also
    /// contains block, transaction, and non-log execution events. Use this
    /// monotonic mode for parser log notifications and reserve
    /// `observe_subindex` for a complete raw ring stream.
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
    pub fn rewind(&mut self) {
        self.last_sequence = None;
        self.last_sub_index = 0;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonadTxnLog {
    pub sequence: u64,
    pub source_sub_index: u32,
    pub block_number: u64,
    pub block_hash: Option<[u8; 32]>,
    pub transaction_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<U256>,
    pub data: Vec<u8>,
    pub commitment: Commitment,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonadExecutionNormalizer {
    chain_id: u64,
    tracker: MonadRingTracker,
}

impl MonadExecutionNormalizer {
    pub fn new(chain_id: u64) -> Self {
        Self {
            chain_id,
            tracker: MonadRingTracker::default(),
        }
    }

    pub fn normalize_txn_log(
        &mut self,
        log: MonadTxnLog,
    ) -> Result<Option<ChainUpdate>, SourceError> {
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

    pub fn normalize_gap(&mut self, reason: impl Into<String>) -> ChainUpdate {
        self.tracker.rewind();
        ChainUpdate::Gap {
            cursor: None,
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonadHead {
    pub sequence: u64,
    pub block_number: u64,
    pub block_hash: Option<[u8; 32]>,
    pub commitment: Commitment,
}

impl MonadExecutionNormalizer {
    pub fn normalize_head(&self, head: MonadHead) -> ChainUpdate {
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
}

/// Nitro supplies both an L2 RPC height and the EVM-visible parent-chain
/// number. Quote validity must use the latter when block delay is non-zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArbitrumExecutionContext {
    pub l2_block_number: u64,
    pub evm_parent_block_number: u64,
}

impl ArbitrumExecutionContext {
    pub fn execution_block_number(self) -> U256 {
        U256::from(self.evm_parent_block_number)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArbitrumHead {
    pub context: ArbitrumExecutionContext,
    pub block_hash: Option<[u8; 32]>,
    pub commitment: Commitment,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArbitrumNitroNormalizer {
    chain_id: u64,
}

impl ArbitrumNitroNormalizer {
    pub fn new(chain_id: u64) -> Self {
        Self { chain_id }
    }

    pub fn normalize_head(&self, head: ArbitrumHead) -> ChainUpdate {
        ChainUpdate::Head(ChainCursor {
            chain_id: self.chain_id,
            block_number: head.context.l2_block_number,
            block_hash: head.block_hash,
            transaction_index: None,
            log_index: None,
            source_sequence: Some(head.context.evm_parent_block_number),
            source_sub_index: None,
            commitment: head.commitment,
        })
    }

    pub fn normalize_log(&self, log: ContractLog) -> Result<ChainUpdate, SourceError> {
        if log.cursor.chain_id != self.chain_id {
            return Err(SourceError::NetworkMismatch);
        }
        // The log's block number is the executed Nitro L2 height. The source
        // retains the EVM-visible parent number in source_sequence when it is
        // available, so quote callers can use the dedicated head context.
        // Preserve canonical/finalized confidence when the log came from a
        // backfill; a normalizer must not downgrade it to realtime.
        Ok(ChainUpdate::Log(log))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProvisionalOverlay {
    base_cursor: Option<ChainCursor>,
    updates: Vec<(ChainCursor, QuoteEvent)>,
}

impl ProvisionalOverlay {
    pub fn begin(&mut self, base_cursor: ChainCursor) {
        self.base_cursor = Some(base_cursor);
        self.updates.clear();
    }

    pub fn push(&mut self, cursor: ChainCursor, event: QuoteEvent) {
        self.updates.push((cursor, event));
    }

    pub fn updates(&self) -> &[(ChainCursor, QuoteEvent)] {
        &self.updates
    }

    pub fn verify_canonical(
        &self,
        canonical: &[(ChainCursor, QuoteEvent)],
    ) -> Result<(), SourceError> {
        if self.updates != canonical {
            return Err(SourceError::Gap(
                "Flashblocks provisional overlay diverged from canonical logs".into(),
            ));
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        self.base_cursor = None;
        self.updates.clear();
    }
}

/// Base adapter: the backend is expected to map Flashblocks `pendingLogs` and
/// `newFlashblocks` into Realtime cursors and standard RPC into canonical logs.
pub type BaseFlashblocksSource<B> = NetworkSource<B>;
/// Monad adapter: the backend is expected to be the local execution-events
/// sidecar/ring reader and must return `Gap` on expired ring payloads.
pub type MonadExecutionEventsSource<B> = NetworkSource<B>;
/// Arbitrum adapter: the backend must consume executed Nitro logs, never raw
/// sequencer messages, and supply EVM-visible parent block context if delays
/// become non-zero.
pub type ArbitrumNitroSource<B> = NetworkSource<B>;
