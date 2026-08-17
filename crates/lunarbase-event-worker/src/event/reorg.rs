//! Stable identities and bounded materialization for fork corrections.

use super::{
    DurableEvent, DurableHead, EventError, RECORD_ID_DOMAIN, REORG_ID_DOMAIN, append_bytes,
    cursor_order, encode_cursor, encode_id,
};
use alloy_primitives::{Address, B256, keccak256};
use lunarbase_client::model::{BlockRef, ContractLog};
use lunarbase_source_evm::fork::ForkResolution;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(crate) struct CorrectionBlock {
    pub block_hash: String,
    pub block_number: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ReorgCorrection {
    pub reorg_id: String,
    pub begin_record_id: String,
    pub commit_record_id: String,
    pub chain_id: String,
    pub core: String,
    pub ancestor: BlockRef,
    pub old_tip: BlockRef,
    pub new_tip: BlockRef,
    pub finalized: BlockRef,
    pub old_blocks: Vec<CorrectionBlock>,
    pub new_heads: Vec<DurableHead>,
    pub new_events: Vec<DurableEvent>,
    pub cursor_json: String,
    pub cursor_order: String,
}

impl ReorgCorrection {
    pub(crate) fn new(
        resolution: &ForkResolution,
        finalized: BlockRef,
        logs: Vec<ContractLog>,
        core: Address,
    ) -> Result<Self, EventError> {
        let ancestor_hash = required_hash(&resolution.common_ancestor)?;
        let old_tip_hash = required_hash(&resolution.old_tip)?;
        let new_tip_hash = required_hash(&resolution.new_tip)?;
        required_hash(&finalized)?;
        let reorg_id = reorg_id(
            resolution.old_tip.cursor.chain_id,
            core,
            ancestor_hash,
            old_tip_hash,
            new_tip_hash,
        );
        let allowed = resolution
            .new_branch
            .iter()
            .map(|block| Ok((block.cursor.block_number, required_hash(block)?)))
            .collect::<Result<BTreeMap<_, _>, EventError>>()?;
        let mut new_events = Vec::with_capacity(logs.len());
        let mut previous_order = None;
        for log in logs {
            let hash = log
                .cursor
                .block_hash
                .ok_or(EventError::StableIdentity("block hash is absent"))?;
            if allowed.get(&log.cursor.block_number) != Some(&hash) {
                return Err(EventError::StableIdentity(
                    "replacement log is outside the resolved branch",
                ));
            }
            let order = log.cursor.event_order();
            if previous_order.is_some_and(|previous| previous >= order) {
                return Err(EventError::StableIdentity(
                    "replacement logs are not in strict event order",
                ));
            }
            previous_order = Some(order);
            let mut event = DurableEvent::from_log(&log)?;
            event.record_id = lifecycle_record_id(&event.logical_log_id, &reorg_id, "applied");
            new_events.push(event);
        }
        let new_heads = resolution
            .new_branch
            .iter()
            .map(|block| DurableHead::from_block(block, core))
            .collect::<Result<Vec<_>, _>>()?;
        let mut recovery_cursor = resolution.new_tip.cursor.clone();
        recovery_cursor.transaction_index = Some(u32::MAX);
        recovery_cursor.log_index = Some(u32::MAX);
        let recovery_json = encode_cursor(&recovery_cursor, &format!("{core:#x}"))?;
        let recovery_order = cursor_order(&recovery_cursor);
        let old_blocks = resolution
            .old_branch
            .iter()
            .map(|block| {
                Ok(CorrectionBlock {
                    block_hash: format!("{:#x}", required_hash(block)?),
                    block_number: block.cursor.block_number.to_string(),
                })
            })
            .collect::<Result<Vec<_>, EventError>>()?;
        Ok(Self {
            begin_record_id: control_record_id(&reorg_id, "begin"),
            commit_record_id: control_record_id(&reorg_id, "commit"),
            reorg_id,
            chain_id: resolution.old_tip.cursor.chain_id.to_string(),
            core: format!("{core:#x}"),
            ancestor: resolution.common_ancestor.clone(),
            old_tip: resolution.old_tip.clone(),
            new_tip: resolution.new_tip.clone(),
            finalized,
            old_blocks,
            new_heads,
            new_events,
            cursor_json: recovery_json,
            cursor_order: recovery_order,
        })
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.reorg_id
            .len()
            .saturating_add(self.begin_record_id.len())
            .saturating_add(self.commit_record_id.len())
            .saturating_add(self.cursor_json.len())
            .saturating_add(self.cursor_order.len())
            .saturating_add(
                self.old_blocks
                    .iter()
                    .map(|block| block.block_hash.len() + block.block_number.len())
                    .sum::<usize>(),
            )
            .saturating_add(
                self.new_heads
                    .iter()
                    .map(DurableHead::retained_bytes)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.new_events
                    .iter()
                    .map(DurableEvent::retained_bytes)
                    .sum::<usize>(),
            )
            .saturating_add(std::mem::size_of::<Self>())
    }

    pub(crate) fn lifecycle_record_id(&self, logical_log_id: &str, operation: &str) -> String {
        lifecycle_record_id(logical_log_id, &self.reorg_id, operation)
    }

    pub(crate) fn control_fields(
        &self,
        reverted: usize,
    ) -> Result<Vec<(&'static str, String)>, EventError> {
        let ancestor_hash = required_hash(&self.ancestor)?;
        let old_tip_hash = required_hash(&self.old_tip)?;
        let old_tip_parent = required_parent(&self.old_tip)?;
        let new_tip_hash = required_hash(&self.new_tip)?;
        let new_tip_parent = required_parent(&self.new_tip)?;
        let finalized_hash = required_hash(&self.finalized)?;
        Ok(vec![
            ("chainId", self.chain_id.clone()),
            ("core", self.core.clone()),
            ("reorgId", self.reorg_id.clone()),
            (
                "ancestorBlockNumber",
                self.ancestor.cursor.block_number.to_string(),
            ),
            (
                "ancestorExecutionBlockNumber",
                self.ancestor.cursor.execution_block_number.to_string(),
            ),
            ("ancestorBlockHash", format!("{ancestor_hash:#x}")),
            (
                "oldTipBlockNumber",
                self.old_tip.cursor.block_number.to_string(),
            ),
            (
                "oldTipExecutionBlockNumber",
                self.old_tip.cursor.execution_block_number.to_string(),
            ),
            ("oldTipBlockHash", format!("{old_tip_hash:#x}")),
            ("oldTipParentHash", format!("{old_tip_parent:#x}")),
            (
                "newTipBlockNumber",
                self.new_tip.cursor.block_number.to_string(),
            ),
            (
                "newTipExecutionBlockNumber",
                self.new_tip.cursor.execution_block_number.to_string(),
            ),
            ("newTipBlockHash", format!("{new_tip_hash:#x}")),
            ("newTipParentHash", format!("{new_tip_parent:#x}")),
            (
                "finalizedBlockNumber",
                self.finalized.cursor.block_number.to_string(),
            ),
            ("finalizedBlockHash", format!("{finalized_hash:#x}")),
            ("revertedLogCount", reverted.to_string()),
            ("appliedLogCount", self.new_events.len().to_string()),
        ])
    }
}

fn reorg_id(chain_id: u64, core: Address, ancestor: B256, old_tip: B256, new_tip: B256) -> String {
    let mut preimage = [0_u8; REORG_ID_DOMAIN.len() + 8 + 20 + 96];
    let mut offset = 0;
    append_bytes(&mut preimage, &mut offset, REORG_ID_DOMAIN);
    append_bytes(&mut preimage, &mut offset, &chain_id.to_be_bytes());
    append_bytes(&mut preimage, &mut offset, core.as_slice());
    append_bytes(&mut preimage, &mut offset, ancestor.as_slice());
    append_bytes(&mut preimage, &mut offset, old_tip.as_slice());
    append_bytes(&mut preimage, &mut offset, new_tip.as_slice());
    encode_id(keccak256(preimage))
}

fn lifecycle_record_id(logical_log_id: &str, reorg_id: &str, operation: &str) -> String {
    let mut preimage = Vec::with_capacity(
        RECORD_ID_DOMAIN.len() + logical_log_id.len() + reorg_id.len() + operation.len(),
    );
    preimage.extend_from_slice(RECORD_ID_DOMAIN);
    preimage.extend_from_slice(logical_log_id.as_bytes());
    preimage.extend_from_slice(reorg_id.as_bytes());
    preimage.extend_from_slice(operation.as_bytes());
    encode_id(keccak256(preimage))
}

fn control_record_id(reorg_id: &str, operation: &str) -> String {
    let mut preimage =
        Vec::with_capacity(RECORD_ID_DOMAIN.len() + reorg_id.len() + operation.len());
    preimage.extend_from_slice(RECORD_ID_DOMAIN);
    preimage.extend_from_slice(reorg_id.as_bytes());
    preimage.extend_from_slice(operation.as_bytes());
    encode_id(keccak256(preimage))
}

fn required_hash(block: &BlockRef) -> Result<B256, EventError> {
    block.cursor.block_hash.ok_or(EventError::HeadIdentity)
}

fn required_parent(block: &BlockRef) -> Result<B256, EventError> {
    block.parent_hash.ok_or(EventError::HeadIdentity)
}
