//! Generic execution-event reader contract.

use crate::{ChainCursor, ContractFilter, SourceError};
use async_trait::async_trait;
use futures_core::Stream;
use lunarbase_math::{Address, U256};
use std::pin::Pin;

/// A block lifecycle notification emitted by an execution engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionHead {
    pub sequence: u64,
    pub block_number: u64,
    pub block_hash: Option<[u8; 32]>,
    pub commitment: crate::Commitment,
}

/// An EVM log emitted by an execution engine before source normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionLog {
    pub sequence: u64,
    pub source_sub_index: u32,
    pub block_number: u64,
    pub block_hash: Option<[u8; 32]>,
    pub transaction_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<U256>,
    pub data: Vec<u8>,
    pub commitment: crate::Commitment,
}

/// Raw execution lifecycle item returned by an [`ExecutionEventReader`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionEvent {
    Head(ExecutionHead),
    Log(ExecutionLog),
    Gap {
        cursor: Option<ChainCursor>,
        reason: String,
    },
}

/// Boxed stream produced by a colocated or remote execution-event reader.
pub type ExecutionEventStream =
    Pin<Box<dyn Stream<Item = Result<ExecutionEvent, SourceError>> + Send>>;

/// Deployment-specific reader for execution-engine events.
///
/// A parser WebSocket, shared-memory ring, or node plugin can implement this
/// trait without coupling its unsafe/native boundary to the quote runtime.
#[async_trait]
pub trait ExecutionEventReader: Send + Sync {
    /// Opens a filtered stream of raw execution lifecycle events.
    async fn subscribe_execution(
        &self,
        filter: ContractFilter,
    ) -> Result<ExecutionEventStream, SourceError>;
}
