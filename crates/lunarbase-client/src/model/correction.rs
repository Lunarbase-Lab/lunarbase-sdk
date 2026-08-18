//! Bounded optimistic branch corrections shared by sources and quote clients.

use super::{BlockRef, ChainUpdate, Commitment, ContractLog, SourceError};
use alloy_primitives::Keccak256;
use lunarbase_math::B256;

/// Maximum number of blocks retained in either side of one correction.
pub const MAX_CORRECTION_BRANCH_BLOCKS: usize = 128;
/// Maximum number of replacement Core logs carried by one correction.
pub const MAX_CORRECTION_LOGS: usize = 8_192;
/// Maximum conservatively charged bytes retained by one correction update.
pub const MAX_CORRECTION_RETAINED_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
/// Complete, bounded replacement for one optimistic fork.
///
/// `replacement_logs` is the complete ordered Core-log set for `new_branch`,
/// not an incremental add/remove list. A client with the advertised ancestor
/// can therefore restore its pre-fork state and deterministically replay the
/// replacement without publishing an intermediate state.
pub struct ChainCorrection {
    /// Last block shared by the abandoned and replacement branches.
    pub common_ancestor: BlockRef,
    /// Last block observed on the abandoned branch.
    pub old_tip: BlockRef,
    /// Last block included in the replacement branch.
    pub new_tip: BlockRef,
    /// Complete contiguous abandoned branch after `common_ancestor`.
    pub old_branch: Vec<BlockRef>,
    /// Complete contiguous replacement branch after `common_ancestor`.
    pub new_branch: Vec<BlockRef>,
    /// Complete ordered Core logs for `new_branch`.
    pub replacement_logs: Vec<ContractLog>,
}

impl ChainCorrection {
    /// Returns the retained-memory charge for queue budgeting.
    ///
    /// Ingestion APIs normalize replacement payloads before retaining this
    /// value, making each payload's visible length its exact owned allocation.
    pub fn retained_bytes(&self) -> usize {
        let branches = self
            .old_branch
            .capacity()
            .saturating_add(self.new_branch.capacity())
            .saturating_mul(std::mem::size_of::<BlockRef>());
        let logs = self
            .replacement_logs
            .capacity()
            .saturating_mul(std::mem::size_of::<ContractLog>())
            .saturating_add(self.replacement_logs.iter().fold(0_usize, |bytes, log| {
                bytes.saturating_add(
                    log.retained_bytes()
                        .saturating_sub(std::mem::size_of::<ContractLog>()),
                )
            }));
        std::mem::size_of::<Self>()
            .saturating_add(branches)
            .saturating_add(logs)
    }

    /// Returns a compact deterministic identity for exact replay detection.
    pub(crate) fn fingerprint(&self) -> B256 {
        correction_fingerprint(self)
    }

    /// Validates branch identity, continuity, ordering, and canonical size bounds.
    ///
    /// The byte bound describes the tightly owned form installed by ingestion
    /// APIs; callers retaining a correction directly should first call
    /// [`Self::normalize_for_retention`].
    pub fn validate(&self) -> Result<(), SourceError> {
        if self.old_branch.len() > MAX_CORRECTION_BRANCH_BLOCKS
            || self.new_branch.len() > MAX_CORRECTION_BRANCH_BLOCKS
            || self.replacement_logs.len() > MAX_CORRECTION_LOGS
            || self
                .retained_bytes()
                .saturating_add(std::mem::size_of::<ChainUpdate>())
                > MAX_CORRECTION_RETAINED_BYTES
        {
            return Err(SourceError::Gap(
                "optimistic correction exceeded its block, log, or byte budget".into(),
            ));
        }
        let chain_id = self.common_ancestor.cursor.chain_id;
        if self
            .old_branch
            .iter()
            .any(|block| block.cursor.commitment == Commitment::Finalized)
        {
            return Err(SourceError::Gap(
                "optimistic correction cannot replace finalized blocks".into(),
            ));
        }
        if chain_id == 0
            || self.old_tip.cursor.chain_id != chain_id
            || self.new_tip.cursor.chain_id != chain_id
        {
            return Err(SourceError::Gap(
                "optimistic correction crosses chain identities".into(),
            ));
        }
        validate_block(&self.common_ancestor, chain_id, "common ancestor")?;
        validate_block(&self.old_tip, chain_id, "old tip")?;
        validate_block(&self.new_tip, chain_id, "new tip")?;
        if same_block_hash(&self.old_tip, &self.new_tip) {
            return Err(SourceError::Gap(
                "optimistic correction cannot replace a block with itself".into(),
            ));
        }
        validate_repeated_hashes(self)?;
        validate_branch(
            &self.common_ancestor,
            &self.old_tip,
            &self.old_branch,
            "old",
        )?;
        validate_branch(
            &self.common_ancestor,
            &self.new_tip,
            &self.new_branch,
            "new",
        )?;
        validate_replacement_logs(self, chain_id)
    }
}

fn same_block_hash(left: &BlockRef, right: &BlockRef) -> bool {
    left.cursor.chain_id == right.cursor.chain_id
        && left.cursor.block_number == right.cursor.block_number
        && left.cursor.block_hash.is_some()
        && left.cursor.block_hash == right.cursor.block_hash
}

fn validate_repeated_hashes(correction: &ChainCorrection) -> Result<(), SourceError> {
    for (index, left) in correction_blocks(correction).enumerate() {
        let Some(left_hash) = left.cursor.block_hash else {
            continue;
        };
        for right in correction_blocks(correction).skip(index.saturating_add(1)) {
            if right.cursor.block_hash == Some(left_hash)
                && (right.cursor.chain_id != left.cursor.chain_id
                    || right.cursor.block_number != left.cursor.block_number
                    || right.cursor.execution_block_number != left.cursor.execution_block_number
                    || right.parent_hash != left.parent_hash)
            {
                return Err(SourceError::Gap(
                    "optimistic correction reuses a block hash with conflicting identity".into(),
                ));
            }
        }
    }
    Ok(())
}

fn correction_blocks(correction: &ChainCorrection) -> impl Iterator<Item = &BlockRef> {
    std::iter::once(&correction.common_ancestor)
        .chain(std::iter::once(&correction.old_tip))
        .chain(std::iter::once(&correction.new_tip))
        .chain(correction.old_branch.iter())
        .chain(correction.new_branch.iter())
}

fn validate_branch(
    ancestor: &BlockRef,
    tip: &BlockRef,
    branch: &[BlockRef],
    label: &str,
) -> Result<(), SourceError> {
    let ancestor_hash = required_hash(ancestor, "common ancestor")?;
    if branch.is_empty() {
        if tip != ancestor {
            return Err(invalid_branch(label));
        }
        return Ok(());
    }
    if branch.len() as u64
        != tip
            .cursor
            .block_number
            .saturating_sub(ancestor.cursor.block_number)
        || branch.last() != Some(tip)
    {
        return Err(invalid_branch(label));
    }

    let mut parent_number = ancestor.cursor.block_number;
    let mut parent_hash = ancestor_hash;
    for block in branch {
        validate_block(block, ancestor.cursor.chain_id, label)?;
        let block_hash = required_hash(block, label)?;
        if block.cursor.chain_id != ancestor.cursor.chain_id
            || block.cursor.block_number != parent_number.saturating_add(1)
            || block.parent_hash != Some(parent_hash)
        {
            return Err(invalid_branch(label));
        }
        parent_number = block.cursor.block_number;
        parent_hash = block_hash;
    }
    Ok(())
}

fn validate_replacement_logs(
    correction: &ChainCorrection,
    chain_id: u64,
) -> Result<(), SourceError> {
    let mut previous_order = None;
    let mut branch_index = 0_usize;
    for log in &correction.replacement_logs {
        if log.removed || log.cursor.chain_id != chain_id {
            return Err(SourceError::Gap(
                "optimistic correction contains an invalid replacement log".into(),
            ));
        }
        let has_transaction_index = log.cursor.transaction_index.is_some();
        let has_log_index = log.cursor.log_index.is_some();
        if has_transaction_index != has_log_index
            || (!has_transaction_index && log.cursor.source_sequence.is_none())
        {
            return Err(SourceError::Gap(
                "optimistic correction log has no deterministic ordering identity".into(),
            ));
        }
        while correction
            .new_branch
            .get(branch_index)
            .is_some_and(|block| block.cursor.block_number < log.cursor.block_number)
        {
            branch_index += 1;
        }
        let Some(block) = correction.new_branch.get(branch_index) else {
            return Err(SourceError::Gap(
                "optimistic correction log is outside its replacement branch".into(),
            ));
        };
        if block.cursor.block_number != log.cursor.block_number
            || log.cursor.block_hash.is_none()
            || log.cursor.block_hash != block.cursor.block_hash
            || log.cursor.execution_block_number != block.cursor.execution_block_number
            || log.cursor.commitment != block.cursor.commitment
        {
            return Err(SourceError::Gap(
                "optimistic correction log has inconsistent block identity".into(),
            ));
        }
        let order = log.cursor.event_order();
        if previous_order.is_some_and(|previous| order <= previous) {
            return Err(SourceError::Gap(
                "optimistic correction logs are duplicate or unordered".into(),
            ));
        }
        previous_order = Some(order);
    }
    Ok(())
}

fn validate_block(block: &BlockRef, chain_id: u64, label: &str) -> Result<(), SourceError> {
    if block.cursor.chain_id != chain_id {
        return Err(SourceError::Gap(
            "optimistic correction crosses chain identities".into(),
        ));
    }
    if block.cursor.transaction_index.is_some() || block.cursor.log_index.is_some() {
        return Err(SourceError::Gap(format!(
            "optimistic correction {label} is not block-level"
        )));
    }
    required_hash(block, label).map(|_| ())
}

fn correction_fingerprint(correction: &ChainCorrection) -> B256 {
    let mut hasher = Keccak256::new();
    hasher.update(b"lunarbase:chain-correction:fingerprint:v1");
    hash_block(&mut hasher, &correction.common_ancestor);
    hash_block(&mut hasher, &correction.old_tip);
    hash_block(&mut hasher, &correction.new_tip);
    hash_u64(&mut hasher, correction.old_branch.len() as u64);
    for block in &correction.old_branch {
        hash_block(&mut hasher, block);
    }
    hash_u64(&mut hasher, correction.new_branch.len() as u64);
    for block in &correction.new_branch {
        hash_block(&mut hasher, block);
    }
    hash_u64(&mut hasher, correction.replacement_logs.len() as u64);
    for log in &correction.replacement_logs {
        hash_log(&mut hasher, log);
    }
    hasher.finalize()
}

fn hash_block(hasher: &mut Keccak256, block: &BlockRef) {
    hash_cursor(hasher, &block.cursor);
    hash_optional_hash(hasher, block.parent_hash);
}

fn hash_log(hasher: &mut Keccak256, log: &ContractLog) {
    hasher.update(log.address.as_slice());
    hash_optional_hash(hasher, log.transaction_hash);
    hash_u64(hasher, log.topics.len() as u64);
    for topic in &log.topics {
        hasher.update(topic.as_slice());
    }
    hash_u64(hasher, log.data.len() as u64);
    hasher.update(log.data.as_ref());
    hasher.update([u8::from(log.removed)]);
    hash_cursor(hasher, &log.cursor);
}

fn hash_cursor(hasher: &mut Keccak256, cursor: &super::ChainCursor) {
    hash_u64(hasher, cursor.chain_id);
    hash_u64(hasher, cursor.block_number);
    hash_u64(hasher, cursor.execution_block_number);
    hash_optional_hash(hasher, cursor.block_hash);
    hash_optional_u32(hasher, cursor.transaction_index);
    hash_optional_u32(hasher, cursor.log_index);
}

fn hash_optional_hash(hasher: &mut Keccak256, value: Option<B256>) {
    hasher.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        hasher.update(value.as_slice());
    }
}

fn hash_optional_u32(hasher: &mut Keccak256, value: Option<u32>) {
    hasher.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        hasher.update(value.to_be_bytes());
    }
}

fn hash_u64(hasher: &mut Keccak256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn required_hash(block: &BlockRef, label: &str) -> Result<B256, SourceError> {
    block
        .cursor
        .block_hash
        .filter(|hash| *hash != B256::ZERO)
        .ok_or_else(|| SourceError::Gap(format!("optimistic correction {label} has no block hash")))
}

fn invalid_branch(label: &str) -> SourceError {
    SourceError::Gap(format!(
        "optimistic correction {label} branch is not contiguous"
    ))
}
