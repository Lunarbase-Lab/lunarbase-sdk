//! Normalized source boundary shared by every network client.
//!
//! Network crates translate provider-specific payloads into [`ChainUpdate`].
//! The runtime depends only on this module, so reducer, persistence, and quote
//! code never need to branch on Base, Monad, or Arbitrum transport details.

use crate::{
    BackfillRequest, ChainCursor, ChainUpdate, ContractFilter, ContractLog, Network, QuoteEvent,
    SourceError,
};
use async_trait::async_trait;
use futures_core::Stream;
use std::pin::Pin;
use std::sync::Arc;

/// Boxed stream of normalized source updates.
pub type SourceStream = Pin<Box<dyn Stream<Item = Result<ChainUpdate, SourceError>> + Send>>;

/// Runtime-facing source implemented by every network client.
#[async_trait]
pub trait ChainEventSource: Send + Sync {
    /// Returns the network family served by this source.
    fn network(&self) -> Network;

    /// Returns the authoritative cursor used for snapshots and recovery.
    async fn snapshot_cursor(&self) -> Result<ChainCursor, SourceError>;

    /// Backfills inclusive canonical logs for a filtered block range.
    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError>;

    /// Opens normalized realtime updates.
    ///
    /// Unexpected transport termination must be represented as a gap before
    /// the stream ends so the runtime cannot continue from uncertain state.
    async fn subscribe(&self, filter: ContractFilter) -> Result<SourceStream, SourceError>;
}

/// Provider adapter used by the generic [`NetworkSource`] facade.
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

/// Adapts a network-specific backend to the runtime-facing source interface.
pub struct NetworkSource<B> {
    network: Network,
    backend: Arc<B>,
}

impl<B> NetworkSource<B> {
    /// Creates a source with an explicit network identity.
    pub fn new(network: Network, backend: Arc<B>) -> Self {
        Self { network, backend }
    }

    /// Returns the wrapped backend.
    pub fn backend(&self) -> &Arc<B> {
        &self.backend
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

/// Creates the generic source facade used by high-level client runtimes.
pub fn make_network_source<B: NormalizedBackend + 'static>(
    network: Network,
    backend: Arc<B>,
) -> NetworkSource<B> {
    NetworkSource::new(network, backend)
}

/// Holds provisional decoded events until canonical logs can be compared.
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

    /// Verifies that canonical replay exactly matches provisional transitions.
    pub fn verify_canonical(
        &self,
        canonical: &[(ChainCursor, QuoteEvent)],
    ) -> Result<(), SourceError> {
        if self.updates != canonical {
            return Err(SourceError::Gap(
                "provisional overlay diverged from canonical logs".into(),
            ));
        }
        Ok(())
    }

    /// Commits a matching canonical sequence and returns its final cursor.
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

    /// Discards all provisional transitions after a gap or reorg.
    pub fn discard(&mut self) {
        self.clear();
    }

    /// Clears the base cursor and all buffered transitions.
    pub fn clear(&mut self) {
        self.base_cursor = None;
        self.updates.clear();
    }
}
