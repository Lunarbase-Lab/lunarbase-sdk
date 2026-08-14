//! Compact proposal lifecycle tracking for durable raw parser records.

use crate::execution::{ExecutionEvent, ExecutionLog, MonadDeliveryMode};
use lunarbase_client::model::{Commitment, ContractFilter, SourceError};
use lunarbase_math::{Address, B256};
use std::collections::HashMap;

mod decode;
mod output;
pub(crate) use decode::RawExecRecord;
use decode::{LifecycleInput, decode_record};
use output::{head, materialize_log};

#[derive(Clone, Copy, Debug)]
pub(super) struct LifecycleLimits {
    pub max_proposals: usize,
    pub max_logs: usize,
    pub max_bytes: usize,
}

#[derive(Debug)]
pub(super) struct ProposalLifecycle {
    chain_id: u64,
    delivery: MonadDeliveryMode,
    emit_removed_logs: bool,
    filter: ContractFilter,
    limits: LifecycleLimits,
    synchronized: bool,
    active: Option<ActiveProposal>,
    proposals: HashMap<B256, Proposal>,
    by_height: HashMap<u64, Vec<B256>>,
    published: HashMap<u64, B256>,
    pending_logs: usize,
    pending_bytes: usize,
}

#[derive(Debug)]
struct ActiveProposal {
    id: B256,
    block_number: u64,
    next_log_index: u32,
    logs: Vec<ExecutionLog>,
    log_bytes: usize,
    published_commitment: Option<Commitment>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProposalState {
    Proposed,
    Voted,
    Finalized,
}

#[derive(Debug)]
struct Proposal {
    block_number: u64,
    block_hash: B256,
    last_sequence: u64,
    state: ProposalState,
    logs: Vec<ExecutionLog>,
    log_bytes: usize,
    published_commitment: Option<Commitment>,
}

impl ProposalLifecycle {
    pub(super) fn new(
        chain_id: u64,
        delivery: MonadDeliveryMode,
        emit_removed_logs: bool,
        filter: ContractFilter,
        limits: LifecycleLimits,
    ) -> Self {
        Self {
            chain_id,
            delivery,
            emit_removed_logs,
            filter,
            limits,
            synchronized: false,
            active: None,
            proposals: HashMap::new(),
            by_height: HashMap::new(),
            published: HashMap::new(),
            pending_logs: 0,
            pending_bytes: 0,
        }
    }

    pub(super) fn process(
        &mut self,
        record: RawExecRecord,
    ) -> Result<Vec<ExecutionEvent>, SourceError> {
        let sequence = record.sequence;
        let block_number = record.block_number;
        let input = decode_record(record, self.chain_id, &self.filter, !self.synchronized)?;
        if !self.synchronized {
            if matches!(&input, LifecycleInput::Fatal) {
                return Err(SourceError::Gap(
                    "Monad execution ring reported a dropped record".into(),
                ));
            }
            if !matches!(&input, LifecycleInput::Start { .. }) {
                return Ok(Vec::new());
            }
            self.synchronized = true;
        }
        match input {
            LifecycleInput::Start { id, block_number } => self.start(sequence, id, block_number),
            LifecycleInput::End { block_hash } => self.end(sequence, block_number, block_hash),
            LifecycleInput::Qc { id, block_number } => self.qc(sequence, id, block_number),
            LifecycleInput::Finalized { id, block_number } => {
                self.finalized(sequence, id, block_number)
            }
            LifecycleInput::Verified { block_number } => self.verified(sequence, block_number),
            LifecycleInput::Fatal => Err(SourceError::Gap(
                "Monad execution ring reported a dropped record".into(),
            )),
            LifecycleInput::Reject => Err(SourceError::Gap(
                "Monad execution rejected an active proposal".into(),
            )),
            LifecycleInput::SkippedLog => {
                self.next_log_index(block_number)?;
                Ok(Vec::new())
            }
            LifecycleInput::Log {
                transaction_index,
                source_sub_index,
                address,
                topics,
                data,
            } => self.log(
                sequence,
                block_number,
                transaction_index,
                source_sub_index,
                address,
                topics,
                data,
            ),
            LifecycleInput::Ignore => Ok(Vec::new()),
        }
    }

    fn start(
        &mut self,
        sequence: u64,
        id: B256,
        block_number: u64,
    ) -> Result<Vec<ExecutionEvent>, SourceError> {
        if self.active.is_some() {
            return Err(SourceError::Gap(
                "Monad started a proposal before the previous execution ended".into(),
            ));
        }
        if self.proposals.len() >= self.limits.max_proposals {
            return Err(SourceError::Gap(
                "Monad pending proposal budget exceeded".into(),
            ));
        }
        let mut output = Vec::new();
        let published_commitment =
            (self.delivery == MonadDeliveryMode::Realtime).then_some(Commitment::Realtime);
        if published_commitment.is_some() {
            self.switch_branch(
                sequence,
                id,
                block_number,
                None,
                Commitment::Realtime,
                &mut output,
            );
            output.push(ExecutionEvent::Head(head(
                sequence,
                block_number,
                None,
                Commitment::Realtime,
            )));
            self.published.insert(block_number, id);
        }
        self.active = Some(ActiveProposal {
            id,
            block_number,
            next_log_index: 0,
            logs: Vec::new(),
            log_bytes: 0,
            published_commitment,
        });
        Ok(output)
    }

    fn log(
        &mut self,
        sequence: u64,
        record_block: Option<u64>,
        transaction_index: u32,
        source_sub_index: u32,
        address: Address,
        topics: Vec<B256>,
        data: Vec<u8>,
    ) -> Result<Vec<ExecutionEvent>, SourceError> {
        let log_index = self.next_log_index(record_block)?;
        let active = self.active.as_mut().expect("active proposal checked above");
        let commitment = active.published_commitment.unwrap_or(Commitment::Realtime);
        let log = ExecutionLog {
            sequence,
            source_sub_index,
            block_number: active.block_number,
            block_hash: None,
            transaction_index,
            log_index,
            address,
            topics,
            data: data.into(),
            removed: false,
            commitment,
        };
        if self.delivery == MonadDeliveryMode::Realtime && !self.emit_removed_logs {
            return Ok(vec![ExecutionEvent::Log(log)]);
        }
        let bytes = log
            .data
            .len()
            .saturating_add(log.topics.len().saturating_mul(32));
        if self.pending_logs >= self.limits.max_logs
            || self.pending_bytes.saturating_add(bytes) > self.limits.max_bytes
        {
            return Err(SourceError::Gap(
                "Monad pending log memory budget exceeded".into(),
            ));
        }
        let realtime_output = (self.delivery == MonadDeliveryMode::Realtime)
            .then(|| ExecutionEvent::Log(log.clone()));
        active.logs.push(log);
        active.log_bytes = active.log_bytes.saturating_add(bytes);
        self.pending_logs += 1;
        self.pending_bytes += bytes;
        Ok(realtime_output.into_iter().collect())
    }

    fn next_log_index(&mut self, record_block: Option<u64>) -> Result<u32, SourceError> {
        let active = self.active.as_mut().ok_or_else(|| {
            SourceError::Gap("Monad log arrived outside an active proposal".into())
        })?;
        if record_block != Some(active.block_number) {
            return Err(SourceError::Gap(
                "Monad log block number disagrees with its active proposal".into(),
            ));
        }
        let log_index = active.next_log_index;
        active.next_log_index = active
            .next_log_index
            .checked_add(1)
            .ok_or_else(|| SourceError::Gap("Monad block log index overflow".into()))?;
        Ok(log_index)
    }

    fn end(
        &mut self,
        sequence: u64,
        record_block: Option<u64>,
        block_hash: B256,
    ) -> Result<Vec<ExecutionEvent>, SourceError> {
        let mut active = self.active.take().ok_or_else(|| {
            SourceError::Gap("Monad block end arrived without an active proposal".into())
        })?;
        if record_block != Some(active.block_number) {
            return Err(SourceError::Gap(
                "Monad block end disagrees with its active proposal".into(),
            ));
        }
        let id = active.id;
        let block_number = active.block_number;
        for log in &mut active.logs {
            log.block_hash = Some(block_hash);
        }
        if self.proposals.contains_key(&id) {
            return Err(SourceError::Gap(
                "Monad completed the same proposal twice".into(),
            ));
        }
        self.proposals.insert(
            id,
            Proposal {
                block_number,
                block_hash,
                last_sequence: sequence,
                state: ProposalState::Proposed,
                logs: active.logs,
                log_bytes: active.log_bytes,
                published_commitment: active.published_commitment,
            },
        );
        self.by_height.entry(block_number).or_default().push(id);
        match self.delivery {
            MonadDeliveryMode::BlockOrdered => self.publish(sequence, id, Commitment::Canonical),
            MonadDeliveryMode::Realtime => Ok(vec![ExecutionEvent::Head(head(
                sequence,
                block_number,
                Some(block_hash),
                active.published_commitment.unwrap_or(Commitment::Realtime),
            ))]),
            MonadDeliveryMode::Finalized => Ok(Vec::new()),
        }
    }

    fn qc(
        &mut self,
        sequence: u64,
        id: B256,
        block_number: u64,
    ) -> Result<Vec<ExecutionEvent>, SourceError> {
        let Some(proposal) = self.proposals.get_mut(&id) else {
            return Ok(Vec::new());
        };
        if proposal.block_number != block_number {
            return Err(SourceError::Gap(
                "Monad QC proposal identity mismatch".into(),
            ));
        }
        if proposal.state != ProposalState::Proposed {
            return Ok(Vec::new());
        }
        proposal.state = ProposalState::Voted;
        proposal.last_sequence = sequence;
        if self.delivery == MonadDeliveryMode::Finalized {
            Ok(Vec::new())
        } else {
            self.publish(sequence, id, Commitment::Canonical)
        }
    }

    fn finalized(
        &mut self,
        sequence: u64,
        id: B256,
        block_number: u64,
    ) -> Result<Vec<ExecutionEvent>, SourceError> {
        let Some(proposal) = self.proposals.get_mut(&id) else {
            return Ok(Vec::new());
        };
        if proposal.block_number != block_number || proposal.state == ProposalState::Finalized {
            return Err(SourceError::Gap(
                "Monad finalized proposal lifecycle is inconsistent".into(),
            ));
        }
        proposal.state = ProposalState::Finalized;
        proposal.last_sequence = sequence;
        let output = self.publish(sequence, id, Commitment::Finalized)?;
        let candidates = self
            .by_height
            .get_mut(&block_number)
            .ok_or_else(|| SourceError::Gap("Monad finalized height has no proposals".into()))?;
        let abandoned = candidates
            .iter()
            .copied()
            .filter(|candidate| candidate != &id)
            .collect::<Vec<_>>();
        candidates.clear();
        candidates.push(id);
        for abandoned_id in abandoned {
            self.remove_proposal(abandoned_id);
        }
        self.published.insert(block_number, id);
        Ok(output)
    }

    fn verified(
        &mut self,
        sequence: u64,
        block_number: u64,
    ) -> Result<Vec<ExecutionEvent>, SourceError> {
        let Some(candidates) = self.by_height.remove(&block_number) else {
            return Ok(Vec::new());
        };
        if candidates.len() != 1 {
            return Err(SourceError::Gap(
                "Monad verified height has competing proposals".into(),
            ));
        }
        let id = candidates[0];
        let proposal = self
            .proposals
            .get(&id)
            .ok_or_else(|| SourceError::Gap("Monad verified proposal is missing".into()))?;
        if proposal.state != ProposalState::Finalized {
            return Err(SourceError::Gap(
                "Monad verified a proposal before finalization".into(),
            ));
        }
        let head = head(
            sequence,
            proposal.block_number,
            Some(proposal.block_hash),
            Commitment::Finalized,
        );
        self.remove_proposal(id);
        self.published.remove(&block_number);
        Ok(vec![ExecutionEvent::Head(head)])
    }

    fn publish(
        &mut self,
        sequence: u64,
        id: B256,
        commitment: Commitment,
    ) -> Result<Vec<ExecutionEvent>, SourceError> {
        let proposal = self.proposals.get(&id).ok_or_else(|| {
            SourceError::Gap("Monad lifecycle update references an unknown proposal".into())
        })?;
        let block_number = proposal.block_number;
        let block_hash = proposal.block_hash;
        let branch_changed = self.published.get(&block_number).copied() != Some(id);
        let mut output = Vec::new();
        self.switch_branch(
            sequence,
            id,
            block_number,
            Some(block_hash),
            commitment,
            &mut output,
        );
        let proposal = self.proposals.get_mut(&id).expect("proposal checked above");
        let should_apply = proposal.published_commitment.is_none() || branch_changed;
        let retain = self.emit_removed_logs && commitment != Commitment::Finalized;
        if retain {
            if should_apply {
                output.extend(proposal.logs.iter().cloned().map(|log| {
                    ExecutionEvent::Log(materialize_log(log, None, block_hash, commitment, false))
                }));
            }
        } else {
            for log in proposal.logs.drain(..) {
                if should_apply {
                    output.push(ExecutionEvent::Log(materialize_log(
                        log, None, block_hash, commitment, false,
                    )));
                }
                self.pending_logs = self.pending_logs.saturating_sub(1);
            }
            self.pending_bytes = self.pending_bytes.saturating_sub(proposal.log_bytes);
            proposal.log_bytes = 0;
        }
        proposal.last_sequence = sequence;
        proposal.published_commitment = Some(commitment);
        output.push(ExecutionEvent::Head(head(
            sequence,
            block_number,
            Some(block_hash),
            commitment,
        )));
        self.published.insert(block_number, id);
        Ok(output)
    }

    fn switch_branch(
        &self,
        sequence: u64,
        id: B256,
        block_number: u64,
        block_hash: Option<B256>,
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
                previous
                    .published_commitment
                    .unwrap_or(Commitment::Realtime),
            ),
            new_head: head(sequence, block_number, block_hash, commitment),
        });
    }

    fn remove_proposal(&mut self, id: B256) {
        if let Some(proposal) = self.proposals.remove(&id) {
            self.pending_logs = self.pending_logs.saturating_sub(proposal.logs.len());
            self.pending_bytes = self.pending_bytes.saturating_sub(proposal.log_bytes);
        }
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
