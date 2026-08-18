//! Proposal branch-switch emission kept separate from the main lifecycle state machine.

use super::{B256, Commitment, ExecutionEvent, ProposalLifecycle, head, materialize_log};

impl ProposalLifecycle {
    pub(super) fn switch_branch(
        &self,
        sequence: u64,
        id: B256,
        block_number: u64,
        block_hash: Option<B256>,
        parent_hash: Option<B256>,
        commitment: Commitment,
        output: &mut Vec<ExecutionEvent>,
    ) {
        let Some(previous_id) = self.published.get(&block_number).copied() else {
            return;
        };
        if previous_id == id {
            return;
        }
        let Some(previous) = self.proposals.get(&previous_id) else {
            return;
        };
        if self.emit_removed_logs {
            output.extend(previous.logs.iter().cloned().map(|log| {
                ExecutionEvent::Log(materialize_log(
                    log,
                    Some(sequence),
                    previous.block_hash,
                    previous
                        .published_commitment
                        .unwrap_or(Commitment::Realtime),
                    true,
                ))
            }));
        }
        output.push(ExecutionEvent::Reorg {
            old_head: head(
                previous.last_sequence,
                block_number,
                Some(previous.block_hash),
                Some(previous.parent_hash),
                previous
                    .published_commitment
                    .unwrap_or(Commitment::Realtime),
            ),
            new_head: head(sequence, block_number, block_hash, parent_hash, commitment),
        });
    }
}
