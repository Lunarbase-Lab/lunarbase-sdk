//! Conversion from the official Monad commit-state block builder.

use lunarbase_client::model::{Commitment, ContractFilter, SourceError};
use lunarbase_math::{Address, B256};
use lunarbase_source_monad::execution::{
    ExecutionEvent, ExecutionHead, ExecutionLog, MonadDeliveryMode,
};
use monad_exec_events::{BlockCommitState, CommitStateBlockUpdate, ExecutedBlock};
use std::sync::Arc;

pub(super) fn convert_update(
    update: CommitStateBlockUpdate,
    sequence: u64,
    delivery: MonadDeliveryMode,
    emit_removed_logs: bool,
    chain_id: u64,
    filter: &ContractFilter,
) -> Result<Vec<ExecutionEvent>, SourceError> {
    validate_chain(&update.block, chain_id)?;
    let commitment = commitment(update.state, delivery);
    let mut output = Vec::new();
    if emit_removed_logs
        && delivery != MonadDeliveryMode::Finalized
        && update.state == BlockCommitState::Finalized
    {
        for abandoned in update.abandoned {
            validate_chain(&abandoned, chain_id)?;
            output.extend(block_logs(
                &abandoned,
                sequence,
                Commitment::Canonical,
                true,
                filter,
            )?);
            output.push(ExecutionEvent::Reorg {
                old_head: block_head(&abandoned, sequence, Commitment::Canonical),
                new_head: block_head(&update.block, sequence, Commitment::Finalized),
            });
        }
    }
    let publish_logs = matches!(
        (delivery, update.state),
        (MonadDeliveryMode::BlockOrdered, BlockCommitState::Proposed)
            | (MonadDeliveryMode::Finalized, BlockCommitState::Finalized)
    );
    if publish_logs {
        output.extend(block_logs(
            &update.block,
            sequence,
            commitment,
            false,
            filter,
        )?);
    }
    let publish_head = match delivery {
        MonadDeliveryMode::Realtime => false,
        MonadDeliveryMode::BlockOrdered => true,
        MonadDeliveryMode::Finalized => matches!(
            update.state,
            BlockCommitState::Finalized | BlockCommitState::Verified
        ),
    };
    if publish_head {
        output.push(ExecutionEvent::Head(block_head(
            &update.block,
            sequence,
            commitment,
        )));
    }
    Ok(output)
}

fn validate_chain(block: &ExecutedBlock, chain_id: u64) -> Result<(), SourceError> {
    if block.start.chain_id.limbs == [chain_id, 0, 0, 0] {
        Ok(())
    } else {
        Err(SourceError::NetworkMismatch)
    }
}

fn commitment(state: BlockCommitState, delivery: MonadDeliveryMode) -> Commitment {
    match state {
        BlockCommitState::Finalized | BlockCommitState::Verified => Commitment::Finalized,
        BlockCommitState::Proposed if delivery == MonadDeliveryMode::BlockOrdered => {
            Commitment::Canonical
        }
        BlockCommitState::Proposed | BlockCommitState::Voted => Commitment::Canonical,
    }
}

fn block_logs(
    block: &ExecutedBlock,
    sequence: u64,
    commitment: Commitment,
    removed: bool,
    filter: &ContractFilter,
) -> Result<Vec<ExecutionEvent>, SourceError> {
    let block_number = block.start.block_tag.block_number;
    let block_hash = B256::new(block.end.eth_block_hash.bytes);
    let mut output = Vec::new();
    let mut log_index = 0_u32;
    for (transaction_index, transaction) in block.txns.iter().enumerate() {
        for log in &transaction.logs {
            let current_log_index = log_index;
            log_index = log_index
                .checked_add(1)
                .ok_or_else(|| SourceError::Gap("Monad block log index exceeds uint32".into()))?;
            let address = Address::new(log.address.bytes);
            if address != filter.address {
                continue;
            }
            if !filter.topics.is_empty()
                && log
                    .topic
                    .first()
                    .map(|topic| B256::new(topic.bytes))
                    .is_none_or(|topic| !filter.topics.contains(&topic))
            {
                continue;
            }
            let topics = log
                .topic
                .iter()
                .map(|topic| B256::new(topic.bytes))
                .collect::<Vec<_>>();
            let transaction_index = u32::try_from(transaction_index)
                .map_err(|_| SourceError::Gap("Monad transaction index exceeds uint32".into()))?;
            output.push(ExecutionEvent::Log(ExecutionLog {
                sequence,
                source_sub_index: current_log_index,
                block_number,
                block_hash: Some(block_hash),
                transaction_index,
                log_index: current_log_index,
                address,
                topics,
                data: log.data.to_vec().into(),
                removed,
                commitment,
            }));
        }
    }
    Ok(output)
}

fn block_head(block: &Arc<ExecutedBlock>, sequence: u64, commitment: Commitment) -> ExecutionHead {
    ExecutionHead {
        sequence,
        block_number: block.start.block_tag.block_number,
        block_hash: Some(B256::new(block.end.eth_block_hash.bytes)),
        commitment,
    }
}
