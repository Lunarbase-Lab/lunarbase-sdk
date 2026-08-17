//! Bounded fork resolution primitives for durable EVM event delivery.
//!
//! The quote plane deliberately does not own this window. Durable event
//! workers seed it from their block journal and invoke the HTTP resolver only
//! after a parent-link discontinuity.

use crate::rpc::backend::RpcHttpBackend;
use lunarbase_client::model::{BlockRef, Commitment, SourceError};
use lunarbase_math::B256;
use std::{collections::VecDeque, mem::size_of};
use thiserror::Error;

/// Count and retained-byte bounds for an unfinalized canonical window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForkWindowLimits {
    /// Maximum number of retained headers, including the finalized boundary.
    pub max_blocks: usize,
    /// Maximum bytes charged to retained headers.
    pub max_bytes: usize,
}

impl Default for ForkWindowLimits {
    fn default() -> Self {
        Self {
            max_blocks: 4096,
            max_bytes: 2 * 1024 * 1024,
        }
    }
}

/// Fail-closed fork-window or resolution error.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ForkError {
    /// A required block hash, parent hash, or coherent block identity is absent.
    #[error("incomplete or invalid block identity: {0}")]
    InvalidIdentity(String),
    /// A block does not extend the retained canonical tip.
    #[error("block is disconnected from the retained canonical window")]
    Disconnected,
    /// The configured count or byte budget is zero or too small.
    #[error("invalid canonical window limits")]
    InvalidLimits,
    /// The retained header count would exceed its hard limit.
    #[error("canonical window block budget exceeded")]
    BlockBudget,
    /// The retained header bytes would exceed their hard limit.
    #[error("canonical window byte budget exceeded")]
    ByteBudget,
    /// No common ancestor exists inside the retained window.
    #[error("common ancestor is outside the retained canonical window")]
    AncestorOutsideWindow,
    /// Resolving either branch would exceed the configured maximum depth.
    #[error("fork resolution depth exceeded")]
    DepthExceeded,
    /// A branch attempts to replace already-finalized history.
    #[error("fork conflicts with the finalized watermark")]
    FinalizedConflict,
    /// A resolution was computed against another canonical tip.
    #[error("fork resolution is stale")]
    StaleResolution,
    /// The exact block lookup failed.
    #[error(transparent)]
    Source(#[from] SourceError),
}

/// Deterministic correction plan computed without mutating canonical state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkResolution {
    /// Last block shared by the abandoned and replacement branches.
    pub common_ancestor: BlockRef,
    /// Canonical tip against which this resolution was computed.
    pub old_tip: BlockRef,
    /// Proposed replacement tip.
    pub new_tip: BlockRef,
    /// Abandoned blocks in ascending height order, excluding the ancestor.
    pub old_branch: Vec<BlockRef>,
    /// Replacement blocks in ascending height order, excluding the ancestor.
    pub new_branch: Vec<BlockRef>,
}

impl ForkResolution {
    /// Returns true when both tips identify the same retained block.
    pub fn is_noop(&self) -> bool {
        self.old_branch.is_empty() && self.new_branch.is_empty()
    }
}

/// Amortized O(1) canonical header window without a per-block auxiliary index.
#[derive(Clone, Debug)]
pub struct CanonicalWindow {
    blocks: VecDeque<BlockRef>,
    finalized: Option<BlockRef>,
    limits: ForkWindowLimits,
    retained_bytes: usize,
}

impl CanonicalWindow {
    /// Creates an empty bounded window without preallocating its maximum size.
    pub fn new(limits: ForkWindowLimits) -> Result<Self, ForkError> {
        if limits.max_blocks == 0 || limits.max_bytes < block_charge() {
            return Err(ForkError::InvalidLimits);
        }
        Ok(Self {
            blocks: VecDeque::new(),
            finalized: None,
            limits,
            retained_bytes: 0,
        })
    }

    /// Number of retained blocks.
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Returns whether the window is empty.
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Conservative retained-byte charge used for admission control.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Current canonical tip, if seeded.
    pub fn tip(&self) -> Option<&BlockRef> {
        self.blocks.back()
    }

    /// Highest finalized boundary retained by this window.
    pub fn finalized(&self) -> Option<&BlockRef> {
        self.finalized.as_ref()
    }

    /// Retained blocks from oldest to newest.
    pub fn blocks(&self) -> impl DoubleEndedIterator<Item = &BlockRef> {
        self.blocks.iter()
    }

    /// Appends one contiguous block. Exact duplicate tips are ignored.
    pub fn push_head(&mut self, block: BlockRef) -> Result<bool, ForkError> {
        validate_complete(&block)?;
        if let Some(tip) = self.tip() {
            validate_same_chain(tip, &block)?;
            if tip == &block {
                return Ok(false);
            }
            if block.cursor.block_number != tip.cursor.block_number.saturating_add(1)
                || block.parent_hash != tip.cursor.block_hash
            {
                return Err(ForkError::Disconnected);
            }
        }
        self.preflight(self.len().saturating_add(1))?;
        self.blocks.push_back(block);
        self.retained_bytes = self.retained_bytes.saturating_add(block_charge());
        Ok(true)
    }

    /// Replaces a same-height progressive tip sharing the same parent.
    pub fn replace_progressive_tip(&mut self, block: BlockRef) -> Result<(), ForkError> {
        validate_complete(&block)?;
        let Some(tip) = self.tip() else {
            return Err(ForkError::Disconnected);
        };
        validate_same_chain(tip, &block)?;
        if self
            .finalized
            .as_ref()
            .is_some_and(|finalized| finalized.cursor.block_number >= block.cursor.block_number)
        {
            return Err(ForkError::FinalizedConflict);
        }
        if tip.cursor.block_number != block.cursor.block_number
            || tip.parent_hash != block.parent_hash
        {
            return Err(ForkError::Disconnected);
        }
        *self.blocks.back_mut().expect("tip checked above") = block;
        Ok(())
    }

    /// Advances finality and prunes only history strictly older than the boundary.
    pub fn advance_finalized(&mut self, block: BlockRef) -> Result<(), ForkError> {
        validate_complete(&block)?;
        if block.cursor.commitment != Commitment::Finalized {
            return Err(ForkError::InvalidIdentity(
                "finalized watermark lacks finalized commitment".into(),
            ));
        }
        if let Some(previous) = &self.finalized {
            validate_same_chain(previous, &block)?;
            if block.cursor.block_number < previous.cursor.block_number
                || (block.cursor.block_number == previous.cursor.block_number
                    && block.cursor.block_hash != previous.cursor.block_hash)
            {
                return Err(ForkError::FinalizedConflict);
            }
        }
        let hash = required_hash(&block)?;
        let Some(index) = self.position(hash) else {
            return Err(ForkError::AncestorOutsideWindow);
        };
        let retained = &self.blocks[index];
        validate_same_chain(retained, &block)?;
        if retained.cursor.block_number != block.cursor.block_number
            || retained.cursor.execution_block_number != block.cursor.execution_block_number
            || retained.parent_hash != block.parent_hash
        {
            return Err(ForkError::InvalidIdentity(
                "finalized block does not match retained identity".into(),
            ));
        }
        self.blocks.drain(..index);
        self.retained_bytes = self.blocks.len().saturating_mul(block_charge());
        self.blocks[0] = block.clone();
        self.finalized = Some(block);
        Ok(())
    }

    /// Atomically switches the in-memory view after durable correction commits.
    pub fn apply_resolution(&mut self, resolution: &ForkResolution) -> Result<(), ForkError> {
        let Some(tip) = self.tip() else {
            return Err(ForkError::StaleResolution);
        };
        if tip != &resolution.old_tip {
            return Err(ForkError::StaleResolution);
        }
        let ancestor_hash = required_hash(&resolution.common_ancestor)?;
        let Some(ancestor_index) = self.position(ancestor_hash) else {
            return Err(ForkError::StaleResolution);
        };
        if self.blocks[ancestor_index] != resolution.common_ancestor
            || !self
                .blocks
                .iter()
                .skip(ancestor_index + 1)
                .eq(&resolution.old_branch)
        {
            return Err(ForkError::StaleResolution);
        }
        self.validate_replacement(resolution)?;
        let next_len = ancestor_index
            .saturating_add(1)
            .saturating_add(resolution.new_branch.len());
        self.preflight(next_len)?;

        self.blocks.truncate(ancestor_index + 1);
        self.blocks.extend(resolution.new_branch.iter().cloned());
        self.retained_bytes = self.blocks.len().saturating_mul(block_charge());
        Ok(())
    }

    fn validate_replacement(&self, resolution: &ForkResolution) -> Result<(), ForkError> {
        if self.finalized.as_ref().is_some_and(|finalized| {
            resolution.common_ancestor.cursor.block_number < finalized.cursor.block_number
        }) {
            return Err(ForkError::FinalizedConflict);
        }
        let mut parent = &resolution.common_ancestor;
        for block in &resolution.new_branch {
            validate_complete(block)?;
            validate_same_chain(parent, block)?;
            if block.cursor.block_number != parent.cursor.block_number.saturating_add(1)
                || block.parent_hash != parent.cursor.block_hash
            {
                return Err(ForkError::Disconnected);
            }
            parent = block;
        }
        if parent != &resolution.new_tip {
            return Err(ForkError::StaleResolution);
        }
        Ok(())
    }

    fn preflight(&self, blocks: usize) -> Result<(), ForkError> {
        if blocks > self.limits.max_blocks {
            return Err(ForkError::BlockBudget);
        }
        if blocks.saturating_mul(block_charge()) > self.limits.max_bytes {
            return Err(ForkError::ByteBudget);
        }
        Ok(())
    }

    fn position(&self, hash: B256) -> Option<usize> {
        self.blocks
            .iter()
            .position(|block| block.cursor.block_hash == Some(hash))
    }
}

/// Bounded exact-hash walker used only after a head-link discontinuity.
#[derive(Clone)]
pub struct ForkResolver {
    backend: RpcHttpBackend,
    max_depth: usize,
}

impl ForkResolver {
    /// Creates a resolver with a strict maximum depth on both branches.
    pub fn new(backend: RpcHttpBackend, max_depth: usize) -> Result<Self, ForkError> {
        if max_depth == 0 {
            return Err(ForkError::InvalidLimits);
        }
        Ok(Self { backend, max_depth })
    }

    /// Resolves the backend's configured canonical tip with parent linkage.
    pub async fn canonical_tip(&self) -> Result<BlockRef, ForkError> {
        self.backend
            .snapshot_block_ref(self.backend.network())
            .await
            .map_err(Into::into)
    }

    /// Resolves one exact block hash with network-specific execution context.
    pub async fn block_ref_by_hash(
        &self,
        block_hash: B256,
        commitment: Commitment,
    ) -> Result<BlockRef, ForkError> {
        self.backend
            .block_ref_by_hash(block_hash, commitment)
            .await
            .map_err(Into::into)
    }

    /// Resolves the provider's finalized watermark with parent linkage.
    pub async fn finalized_tip(&self) -> Result<BlockRef, ForkError> {
        self.backend
            .block_ref_at_tag("finalized", Commitment::Finalized)
            .await
            .map_err(Into::into)
    }

    /// Computes a correction plan without mutating the retained window.
    pub async fn resolve(
        &self,
        window: &CanonicalWindow,
        new_tip: BlockRef,
    ) -> Result<ForkResolution, ForkError> {
        validate_complete(&new_tip)?;
        let Some(old_tip) = window.tip().cloned() else {
            return Err(ForkError::AncestorOutsideWindow);
        };
        validate_same_chain(&old_tip, &new_tip)?;
        if new_tip.cursor.chain_id != self.backend.chain_id() {
            return Err(ForkError::InvalidIdentity(
                "resolver chain id mismatch".into(),
            ));
        }
        if old_tip == new_tip {
            return Ok(ForkResolution {
                common_ancestor: old_tip.clone(),
                old_tip: old_tip.clone(),
                new_tip: old_tip,
                old_branch: Vec::new(),
                new_branch: Vec::new(),
            });
        }
        if window.finalized().is_some_and(|finalized| {
            new_tip.cursor.block_number <= finalized.cursor.block_number
                && new_tip.cursor.block_hash != finalized.cursor.block_hash
        }) {
            return Err(ForkError::FinalizedConflict);
        }

        let mut descending = vec![new_tip.clone()];
        let (common_ancestor, ancestor_index) = loop {
            let child = descending.last().expect("new tip seeded above");
            let parent_hash = required_parent_hash(child)?;
            if let Some(index) = window.position(parent_hash) {
                let ancestor = window.blocks[index].clone();
                if child.cursor.block_number != ancestor.cursor.block_number.saturating_add(1) {
                    return Err(ForkError::Disconnected);
                }
                break (ancestor, index);
            }
            if descending.len() >= self.max_depth {
                return Err(ForkError::DepthExceeded);
            }
            if window.finalized().is_some_and(|finalized| {
                child.cursor.block_number <= finalized.cursor.block_number.saturating_add(1)
            }) {
                return Err(ForkError::FinalizedConflict);
            }
            if child.cursor.block_number == 0 {
                return Err(ForkError::AncestorOutsideWindow);
            }
            let parent = self
                .backend
                .block_ref_by_hash(parent_hash, child.cursor.commitment)
                .await?;
            validate_complete(&parent)?;
            if parent.cursor.block_number.saturating_add(1) != child.cursor.block_number {
                return Err(ForkError::Disconnected);
            }
            descending.push(parent);
        };

        let old_branch = window
            .blocks
            .iter()
            .skip(ancestor_index + 1)
            .cloned()
            .collect::<Vec<_>>();
        if old_branch.len() > self.max_depth {
            return Err(ForkError::DepthExceeded);
        }
        descending.reverse();
        Ok(ForkResolution {
            common_ancestor,
            old_tip,
            new_tip,
            old_branch,
            new_branch: descending,
        })
    }
}

const fn block_charge() -> usize {
    size_of::<BlockRef>()
}

fn required_hash(block: &BlockRef) -> Result<B256, ForkError> {
    block
        .cursor
        .block_hash
        .ok_or_else(|| ForkError::InvalidIdentity("block hash is absent".into()))
}

fn required_parent_hash(block: &BlockRef) -> Result<B256, ForkError> {
    block
        .parent_hash
        .ok_or_else(|| ForkError::InvalidIdentity("parent hash is absent".into()))
}

fn validate_complete(block: &BlockRef) -> Result<(), ForkError> {
    required_hash(block)?;
    required_parent_hash(block)?;
    if block.cursor.transaction_index.is_some() || block.cursor.log_index.is_some() {
        return Err(ForkError::InvalidIdentity(
            "block reference contains event coordinates".into(),
        ));
    }
    Ok(())
}

fn validate_same_chain(left: &BlockRef, right: &BlockRef) -> Result<(), ForkError> {
    if left.cursor.chain_id != right.cursor.chain_id {
        return Err(ForkError::InvalidIdentity("chain id mismatch".into()));
    }
    Ok(())
}
