//! Canonical HTTP backend shared by realtime network sources.

use crate::rpc::client::RpcHttpClient;
use lunarbase_client::model::{
    BackfillRequest, BlockRef, ChainCursor, Checkpoint, Commitment, ContractLog, Network,
    SourceError,
};
use lunarbase_client::protocol::proxy::{ERC1967_IMPLEMENTATION_SLOT, decode_implementation};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Mutex;

#[cfg(test)]
use tokio::sync::{Notify, Semaphore};

#[derive(Default)]
struct ChainVerification {
    verified: AtomicBool,
    pending: AtomicUsize,
    singleflight: Mutex<()>,
}

impl ChainVerification {
    fn is_idle_and_verified(&self) -> bool {
        self.pending.load(Ordering::Acquire) == 0
            && self.verified.load(Ordering::Acquire)
            && self.pending.load(Ordering::Acquire) == 0
    }

    fn publish_ensure_success(&self) {
        if self.pending.load(Ordering::Acquire) == 0 {
            self.verified.store(true, Ordering::Release);
            if self.pending.load(Ordering::Acquire) != 0 {
                self.verified.store(false, Ordering::Release);
            }
        }
    }
}

struct PendingVerification<'a> {
    state: &'a ChainVerification,
}

impl<'a> PendingVerification<'a> {
    fn begin(state: &'a ChainVerification) -> Self {
        state.pending.fetch_add(1, Ordering::AcqRel);
        state.verified.store(false, Ordering::Release);
        Self { state }
    }

    fn publish_success_if_last(&self) {
        if self.state.pending.load(Ordering::Acquire) == 1 {
            self.state.verified.store(true, Ordering::Release);
            if self.state.pending.load(Ordering::Acquire) != 1 {
                self.state.verified.store(false, Ordering::Release);
            }
        }
    }
}

impl Drop for PendingVerification<'_> {
    fn drop(&mut self) {
        let previous = self.state.pending.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "pending verification count underflow");
    }
}

#[cfg(test)]
struct VerificationHook {
    started: Notify,
    proceed: Semaphore,
}

#[cfg(test)]
impl VerificationHook {
    fn new() -> Self {
        Self {
            started: Notify::new(),
            proceed: Semaphore::new(0),
        }
    }

    async fn pause(&self) {
        self.started.notify_one();
        self.proceed
            .acquire()
            .await
            .expect("verification hook remains open")
            .forget();
    }
}

#[derive(Clone)]
/// Canonical HTTP backend used by realtime network sources.
pub struct RpcHttpBackend {
    /// Read-only Alloy provider used for explicit JSON-RPC operations.
    rpc: RpcHttpClient,
    /// Network family exposed through the common data-source interface.
    network: Network,
    /// EIP-155 chain identifier attached to normalized cursors.
    chain_id: u64,
    /// Explicit block tag used to make bootstrap reads coherent.
    snapshot_tag: Arc<str>,
    /// Shared verification session used by every clone of this HTTP backend.
    chain_verification: Arc<ChainVerification>,
    #[cfg(test)]
    verification_hook: Option<Arc<VerificationHook>>,
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
            chain_verification: Arc::new(ChainVerification::default()),
            #[cfg(test)]
            verification_hook: None,
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

    /// Rechecks the independent HTTP endpoint even when this backend was
    /// verified previously. Realtime subscribe/reconnect boundaries use this
    /// method so an endpoint change cannot inherit an older session result.
    pub(crate) async fn verify_chain_id(&self) -> Result<(), SourceError> {
        let pending = PendingVerification::begin(&self.chain_verification);
        let _guard = self.chain_verification.singleflight.lock().await;
        self.verify_chain_id_locked().await?;
        pending.publish_success_if_last();
        Ok(())
    }

    /// Verifies a standalone canonical operation once per shared backend
    /// session. The fast path performs only atomic checks and no RPC.
    async fn ensure_chain_id(&self) -> Result<(), SourceError> {
        if self.chain_verification.is_idle_and_verified() {
            return Ok(());
        }
        let _guard = self.chain_verification.singleflight.lock().await;
        if self.chain_verification.is_idle_and_verified() {
            return Ok(());
        }
        self.verify_chain_id_locked().await?;
        self.chain_verification.publish_ensure_success();
        Ok(())
    }

    async fn verify_chain_id_locked(&self) -> Result<(), SourceError> {
        #[cfg(test)]
        if let Some(hook) = self.verification_hook.as_ref() {
            hook.pause().await;
        }
        let actual = self.rpc.chain_id().await?;
        if actual != self.chain_id {
            return Err(SourceError::Unavailable(format!(
                "HTTP RPC chain id mismatch: expected {}, got {actual}",
                self.chain_id
            )));
        }
        Ok(())
    }
}

impl RpcHttpBackend {
    /// Returns the configured canonical snapshot cursor.
    pub async fn snapshot_cursor(&self, network: Network) -> Result<ChainCursor, SourceError> {
        if network != self.network {
            return Err(SourceError::NetworkMismatch);
        }
        self.ensure_chain_id().await?;
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

    /// Returns the configured canonical block with parent linkage.
    pub(crate) async fn snapshot_block_ref(
        &self,
        network: Network,
    ) -> Result<BlockRef, SourceError> {
        if network != self.network {
            return Err(SourceError::NetworkMismatch);
        }
        self.ensure_chain_id().await?;
        let commitment = if self.snapshot_tag.as_ref() == "finalized" {
            Commitment::Finalized
        } else {
            Commitment::Canonical
        };
        self.rpc
            .block_ref(&self.snapshot_tag, self.chain_id, commitment)
            .await
            .map_err(Into::into)
    }

    /// Reads canonical logs for recovery.
    pub async fn backfill(
        &self,
        request: BackfillRequest,
    ) -> Result<Vec<ContractLog>, SourceError> {
        self.ensure_chain_id().await?;
        let commitment = if self.snapshot_tag.as_ref() == "finalized" {
            Commitment::Finalized
        } else {
            Commitment::Canonical
        };
        self.rpc
            .get_logs(&request, self.chain_id, commitment)
            .await
            .map_err(Into::into)
    }

    /// Verifies checkpoint canonicality and the ERC-1967 implementation identity.
    pub async fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<bool, SourceError> {
        if checkpoint.chain_id != self.chain_id || checkpoint.cursor.chain_id != self.chain_id {
            return Ok(false);
        }
        self.ensure_chain_id().await?;
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

#[cfg(test)]
#[path = "backend_tests.rs"]
mod verification_tests;
