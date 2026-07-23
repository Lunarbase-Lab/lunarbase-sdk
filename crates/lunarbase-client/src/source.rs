//! Unified bootstrap, recovery, and realtime source boundary.

use crate::bootstrap::BootstrapSnapshot;
use crate::model::{
    BackfillRequest, ChainCursor, ChainUpdate, Checkpoint, ContractFilter, ContractLog,
    DeploymentConfig, Network, SourceError,
};
use futures_core::Stream;
use std::{future::Future, pin::Pin};

/// Boxed stream of normalized source updates.
pub type SourceStream = Pin<Box<dyn Stream<Item = Result<ChainUpdate, SourceError>> + Send>>;

/// Complete data-source contract implemented by every network package.
pub trait ChainDataSource: Send + Sync {
    /// Returns the network family served by this source.
    fn network(&self) -> Network;

    /// Reads one coherent, code-hash-checked quote state snapshot.
    fn snapshot(
        &self,
        deployment: &DeploymentConfig,
    ) -> impl Future<Output = Result<BootstrapSnapshot, SourceError>> + Send;

    /// Backfills inclusive canonical logs for recovery.
    fn backfill(
        &self,
        request: BackfillRequest,
    ) -> impl Future<Output = Result<Vec<ContractLog>, SourceError>> + Send;

    /// Opens normalized realtime updates.
    fn subscribe(
        &self,
        filter: ContractFilter,
    ) -> impl Future<Output = Result<SourceStream, SourceError>> + Send;

    /// Returns the canonical cursor used as the recovery target.
    fn canonical_head(&self) -> impl Future<Output = Result<ChainCursor, SourceError>> + Send;

    /// Verifies that a checkpoint cursor is still on the canonical chain.
    fn validate_checkpoint(
        &self,
        checkpoint: &Checkpoint,
    ) -> impl Future<Output = Result<bool, SourceError>> + Send;
}
