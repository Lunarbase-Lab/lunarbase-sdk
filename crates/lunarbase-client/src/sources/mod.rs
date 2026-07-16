//! Normalized source boundary and network-specific transport adapters.
//!
//! The parent module defines the provider-independent update model and
//! normalizers. Child modules implement generic JSON-RPC WebSocket, Base
//! Flashblocks, and Arbitrum Nitro transports; all are re-exported here so the
//! crate-root API remains stable.

use crate::{
    BackfillRequest, ChainCursor, ChainUpdate, Commitment, ContractFilter, ContractLog, Network,
    QuoteEvent, SourceError,
};
use async_trait::async_trait;
use futures_core::Stream;
use lunarbase_math::{Address, U256};
use std::pin::Pin;
use std::sync::Arc;

mod flashblocks;
mod nitro;
mod rpc;
mod ws;

pub use flashblocks::*;
pub use nitro::*;
pub use rpc::*;
pub use ws::*;

pub type SourceStream = Pin<Box<dyn Stream<Item = Result<ChainUpdate, SourceError>> + Send>>;

/// Common source boundary. Network-specific adapters only normalize transport
/// semantics; consumers never branch on provider-specific payloads.
#[async_trait]
pub trait ChainEventSource: Send + Sync {
    /// Identifies the network family served by this source.
    fn network(&self) -> Network;
    /// Returns the authoritative source head used for snapshots and recovery.
    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError>;
    /// Backfills inclusive canonical logs for a filtered block range.
    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError>;
    /// Opens normalized realtime updates; unexpected termination is a gap.
    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError>;
}

/// Transport adapter used by Base, Monad, and Arbitrum implementations. A
/// production transport supplies RPC/WS or a local sidecar; the state machine
/// remains identical across networks.
#[async_trait]
pub trait NormalizedBackend: Send + Sync {
    /// Returns a network-specific block-tagged cursor.
    async fn snapshot_cursor(&self, network: Network) -> Result<ChainCursor, SourceError>;
    /// Reads canonical logs for recovery.
    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError>;
    /// Opens the provider-specific realtime transport.
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
    /// Wraps a specialized backend with the common source interface.
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

/// Construct the network-independent source facade from deployment config.
/// The backend itself remains specialized (Flashblocks, Monad sidecar, or
/// Nitro), while the reducer only receives the common `ChainEventSource`.
pub fn make_network_source<B: NormalizedBackend + 'static>(
    network: Network,
    backend: Arc<B>,
) -> NetworkSource<B> {
    NetworkSource::new(network, backend)
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
    /// Records a payload/index and reports whether it starts a new payload.
    /// Duplicate indexes are ignored; regressions or context changes are gaps.
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

    /// Clears payload history after canonical recovery.
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
    /// Creates a normalizer for one configured chain id.
    pub fn new(chain_id: u64) -> Self {
        Self {
            tracker: BaseFlashblocksTracker::default(),
            chain_id,
        }
    }

    /// Converts a Flashblock header into a realtime block-head update.
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

    /// Converts a Flashblock log into an ordered head/log update sequence.
    /// Multiple logs at one Flashblock index remain valid.
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

    /// Clears provisional payload tracking after a gap or sealed-block commit.
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
    /// Accepts a strictly contiguous global sequence for a complete raw ring.
    pub fn observe(&mut self, sequence: u64) -> Result<bool, SourceError> {
        self.observe_subindex(sequence, 0)
    }

    /// Accepts a sequence/sub-index pair, rejecting missing raw ring events.
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
    /// Rewinds sequence state after an explicit parser/ring gap.
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
    /// Creates a normalizer for Monad execution-event records.
    pub fn new(chain_id: u64) -> Self {
        Self {
            chain_id,
            tracker: MonadRingTracker::default(),
        }
    }

    /// Converts a filtered transaction log while preserving source ordering.
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

    /// Resets sequence tracking and emits a fail-closed gap update.
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
    /// Converts a parser head and preserves its commitment in the cursor.
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
    /// Returns the EVM-visible block number used by lane delay predicates.
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
    /// Creates a normalizer for one Arbitrum chain id.
    pub fn new(chain_id: u64) -> Self {
        Self { chain_id }
    }

    /// Converts an executed Nitro head while retaining parent-chain context.
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

    /// Validates that an executed/backfilled log belongs to this chain.
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
    /// Starts a new provisional sequence from a canonical base cursor.
    pub fn begin(&mut self, base_cursor: ChainCursor) {
        self.base_cursor = Some(base_cursor);
        self.updates.clear();
    }

    /// Appends one decoded provisional event in source order.
    pub fn push(&mut self, cursor: ChainCursor, event: QuoteEvent) {
        self.updates.push((cursor, event));
    }

    /// Returns the currently buffered provisional transitions.
    pub fn updates(&self) -> &[(ChainCursor, QuoteEvent)] {
        &self.updates
    }

    /// Verifies that canonical replay exactly matches the provisional sequence.
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

    /// Verify a sealed block and discard the provisional overlay only after
    /// the canonical event sequence matches byte-for-byte at the model level.
    /// The returned cursor is the last committed event, or the overlay base
    /// cursor when the canonical block contained no quote event.
    /// Verifies and commits a sealed canonical sequence, returning its cursor.
    pub fn commit_canonical(
        &mut self,
        canonical: &[(ChainCursor, QuoteEvent)],
    ) -> Result<Option<ChainCursor>, SourceError> {
        self.verify_canonical(canonical)?;
        let cursor = canonical
            .last()
            .map(|(cursor, _)| cursor.clone())
            .or_else(|| self.base_cursor.clone());
        self.clear();
        Ok(cursor)
    }

    /// Discard all provisional transitions after a source gap or reorg.
    /// Discards provisional transitions after a gap or reorg.
    pub fn discard(&mut self) {
        self.clear();
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
