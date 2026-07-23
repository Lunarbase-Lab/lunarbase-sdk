//! Canonical HTTP backend shared by realtime network adapters.

use crate::model::{
    BackfillRequest, ChainCursor, Checkpoint, Commitment, ContractLog, Network, SourceError,
};
use crate::protocol::proxy::{ERC1967_IMPLEMENTATION_SLOT, decode_implementation};
use crate::transport::rpc::client::RpcHttpClient;
use std::sync::Arc;

#[derive(Clone)]
/// Canonical HTTP backend used by all realtime network adapters.
pub struct RpcHttpBackend {
    /// Read-only Alloy provider used for explicit JSON-RPC operations.
    rpc: RpcHttpClient,
    /// Network family exposed through the common data-source interface.
    network: Network,
    /// EIP-155 chain identifier attached to normalized cursors.
    chain_id: u64,
    /// Explicit block tag used to make bootstrap reads coherent.
    snapshot_tag: Arc<str>,
}

impl RpcHttpBackend {
    /// Creates the HTTP-only backend used for canonical snapshots/backfills.
    pub fn new(
        rpc: RpcHttpClient,
        network: Network,
        chain_id: u64,
        snapshot_tag: impl Into<String>,
    ) -> Self {
        Self {
            rpc,
            network,
            chain_id,
            snapshot_tag: Arc::from(snapshot_tag.into()),
        }
    }

    /// Returns the underlying JSON-RPC client.
    pub fn rpc(&self) -> &RpcHttpClient {
        &self.rpc
    }

    /// Returns the network family configured for this backend.
    pub fn network(&self) -> Network {
        self.network
    }

    /// Returns the configured chain id.
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Returns the block tag used for coherent snapshots.
    pub fn snapshot_tag(&self) -> &str {
        &self.snapshot_tag
    }
}

impl RpcHttpBackend {
    /// Returns the configured canonical snapshot cursor.
    pub async fn snapshot_cursor(&self, network: Network) -> Result<ChainCursor, SourceError> {
        if network != self.network {
            return Err(SourceError::NetworkMismatch);
        }
        let commitment = if self.snapshot_tag.as_ref() == "finalized" {
            Commitment::Finalized
        } else {
            Commitment::Canonical
        };
        self.rpc
            .block_cursor(&self.snapshot_tag, self.chain_id, commitment)
            .await
            .map_err(Into::into)
    }

    /// Reads canonical logs for recovery.
    pub async fn backfill(
        &self,
        request: BackfillRequest,
    ) -> Result<Vec<ContractLog>, SourceError> {
        self.rpc
            .get_logs(&request, self.chain_id, Commitment::Canonical)
            .await
            .map_err(Into::into)
    }

    /// Verifies checkpoint canonicality and the ERC-1967 implementation identity.
    pub async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        let tag = format!("0x{:x}", checkpoint.cursor.block_number);
        let canonical = self
            .rpc
            .block_cursor(&tag, self.chain_id, Commitment::Canonical)
            .await?;
        let Some(block_hash) = canonical.block_hash else {
            return Ok(false);
        };
        if checkpoint.cursor.block_hash != Some(block_hash) {
            return Ok(false);
        }
        let implementation = decode_implementation(
            self.rpc
                .get_storage_at_hash(checkpoint.core, ERC1967_IMPLEMENTATION_SLOT, block_hash)
                .await?,
        );
        if implementation != Some(checkpoint.expected_implementation) {
            return Ok(false);
        }
        let code_hash = self
            .rpc
            .runtime_code_hash_at_hash(checkpoint.expected_implementation, block_hash)
            .await?;
        Ok(code_hash == checkpoint.expected_implementation_code_hash)
    }
}
