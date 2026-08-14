//! Official Monad ABI decoding for parser-delivered raw descriptors.

use lunarbase_client::model::{ContractFilter, SourceError};
use lunarbase_math::{Address, B256, Bytes};
use monad_event_ring::{EventDecoder, EventDescriptorInfo};
use monad_exec_events::{ExecEventDecoder, ExecEventRef, ExecEventRingFlowInfo};
use std::panic::{AssertUnwindSafe, catch_unwind};

#[derive(Clone, Debug)]
pub(crate) struct RawExecRecord {
    pub sequence: u64,
    pub source_sequence: u64,
    pub timestamp_ns: u64,
    pub block_number: Option<u64>,
    pub event_type_id: u16,
    pub event_name: String,
    pub flow_block_seqno: u64,
    pub flow_txn_index: Option<usize>,
    pub flow_account_index: u64,
    pub payload: Bytes,
}

#[derive(Debug)]
pub(super) enum LifecycleInput {
    Start {
        id: B256,
        block_number: u64,
    },
    End {
        block_hash: B256,
    },
    Qc {
        id: B256,
        block_number: u64,
    },
    Finalized {
        id: B256,
        block_number: u64,
    },
    Verified {
        block_number: u64,
    },
    Fatal,
    Reject,
    SkippedLog,
    Log {
        transaction_index: u32,
        source_sub_index: u32,
        address: Address,
        topics: Vec<B256>,
        data: Vec<u8>,
    },
    Ignore,
}

pub(super) fn decode_record(
    record: RawExecRecord,
    chain_id: u64,
    filter: &ContractFilter,
    seeking_start: bool,
) -> Result<LifecycleInput, SourceError> {
    let expected_name = expected_event_name(record.event_type_id)
        .ok_or_else(|| SourceError::Gap("Monad record has an unknown ABI event type".into()))?;
    if expected_name != record.event_name {
        return Err(SourceError::Gap(
            "Monad event name disagrees with its ABI event type".into(),
        ));
    }
    if seeking_start && !matches!(record.event_type_id, 1 | 2) {
        return Ok(LifecycleInput::Ignore);
    }
    if !matches!(record.event_type_id, 1 | 2 | 3 | 6 | 7 | 8 | 9 | 18) {
        return Ok(LifecycleInput::Ignore);
    }
    let info = EventDescriptorInfo::<ExecEventDecoder> {
        seqno: record.source_sequence,
        event_type: record.event_type_id,
        record_epoch_nanos: record.timestamp_ns,
        flow_info: ExecEventRingFlowInfo {
            block_seqno: record.flow_block_seqno,
            txn_idx: record.flow_txn_index,
            account_idx: record.flow_account_index,
        },
    };
    let input = catch_unwind(AssertUnwindSafe(|| {
        decode_payload(
            ExecEventDecoder::raw_to_event_ref(info, record.payload.as_ref()),
            chain_id,
            filter,
        )
    }))
    .map_err(|_| SourceError::Gap("Monad raw execution payload failed ABI decoding".into()))??;
    validate_record_block(record.block_number, &input)?;
    Ok(input)
}

fn decode_payload(
    decoded: ExecEventRef<'_>,
    chain_id: u64,
    filter: &ContractFilter,
) -> Result<LifecycleInput, SourceError> {
    Ok(match decoded {
        ExecEventRef::BlockStart(start) => {
            if start.chain_id.limbs != [chain_id, 0, 0, 0] {
                return Err(SourceError::NetworkMismatch);
            }
            LifecycleInput::Start {
                id: B256::new(start.block_tag.id.bytes),
                block_number: start.block_tag.block_number,
            }
        }
        ExecEventRef::BlockEnd(end) => LifecycleInput::End {
            block_hash: B256::new(end.eth_block_hash.bytes),
        },
        ExecEventRef::BlockQC(qc) => LifecycleInput::Qc {
            id: B256::new(qc.block_tag.id.bytes),
            block_number: qc.block_tag.block_number,
        },
        ExecEventRef::BlockFinalized(tag) => LifecycleInput::Finalized {
            id: B256::new(tag.id.bytes),
            block_number: tag.block_number,
        },
        ExecEventRef::BlockVerified(verified) => LifecycleInput::Verified {
            block_number: verified.block_number,
        },
        ExecEventRef::RecordError(_) => LifecycleInput::Fatal,
        ExecEventRef::BlockReject(_) => LifecycleInput::Reject,
        ExecEventRef::TxnLog {
            txn_index,
            txn_log,
            topic_bytes,
            data_bytes,
        } => {
            validate_topic_bytes(topic_bytes)?;
            let address = Address::new(txn_log.address.bytes);
            if address != filter.address || !topic_allowed(topic_bytes, filter) {
                LifecycleInput::SkippedLog
            } else {
                LifecycleInput::Log {
                    transaction_index: u32::try_from(txn_index).map_err(|_| {
                        SourceError::Gap("Monad transaction index exceeds uint32".into())
                    })?,
                    source_sub_index: txn_log.index,
                    address,
                    topics: decode_topics(topic_bytes)?,
                    data: data_bytes.to_vec(),
                }
            }
        }
        _ => {
            return Err(SourceError::Gap(
                "Monad ABI decoder returned an unexpected lifecycle event".into(),
            ));
        }
    })
}

fn validate_record_block(
    block_number: Option<u64>,
    input: &LifecycleInput,
) -> Result<(), SourceError> {
    let decoded = match input {
        LifecycleInput::Start { block_number, .. }
        | LifecycleInput::Qc { block_number, .. }
        | LifecycleInput::Finalized { block_number, .. }
        | LifecycleInput::Verified { block_number } => Some(*block_number),
        _ => block_number,
    };
    if block_number.is_some() && decoded != block_number {
        return Err(SourceError::Gap(
            "Monad record block number disagrees with decoded payload".into(),
        ));
    }
    Ok(())
}

fn decode_topics(bytes: &[u8]) -> Result<Vec<B256>, SourceError> {
    validate_topic_bytes(bytes)?;
    Ok(bytes.chunks_exact(32).map(B256::from_slice).collect())
}

fn validate_topic_bytes(bytes: &[u8]) -> Result<(), SourceError> {
    if !bytes.len().is_multiple_of(32) {
        return Err(SourceError::Gap(
            "Monad execution log topics are not aligned to bytes32".into(),
        ));
    }
    Ok(())
}

fn topic_allowed(topic_bytes: &[u8], filter: &ContractFilter) -> bool {
    filter.topics.is_empty()
        || topic_bytes
            .get(..32)
            .map(B256::from_slice)
            .is_some_and(|topic| filter.topics.contains(&topic))
}

fn expected_event_name(event_type: u16) -> Option<&'static str> {
    Some(match event_type {
        1 => "RECORD_ERROR",
        2 => "BLOCK_START",
        3 => "BLOCK_REJECT",
        4 => "BLOCK_PERF_EVM_ENTER",
        5 => "BLOCK_PERF_EVM_EXIT",
        6 => "BLOCK_END",
        7 => "BLOCK_QC",
        8 => "BLOCK_FINALIZED",
        9 => "BLOCK_VERIFIED",
        10 => "TXN_HEADER_START",
        11 => "TXN_ACCESS_LIST_ENTRY",
        12 => "TXN_AUTH_LIST_ENTRY",
        13 => "TXN_HEADER_END",
        14 => "TXN_REJECT",
        15 => "TXN_PERF_EVM_ENTER",
        16 => "TXN_PERF_EVM_EXIT",
        17 => "TXN_EVM_OUTPUT",
        18 => "TXN_LOG",
        19 => "TXN_CALL_FRAME",
        20 => "TXN_END",
        21 => "ACCOUNT_ACCESS_LIST_HEADER",
        22 => "ACCOUNT_ACCESS",
        23 => "STORAGE_ACCESS",
        24 => "EVM_ERROR",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_official_event_names_are_pinned() {
        assert_eq!(expected_event_name(1), Some("RECORD_ERROR"));
        assert_eq!(expected_event_name(18), Some("TXN_LOG"));
        assert_eq!(expected_event_name(24), Some("EVM_ERROR"));
        assert_eq!(expected_event_name(0), None);
        assert_eq!(expected_event_name(25), None);
    }
}
