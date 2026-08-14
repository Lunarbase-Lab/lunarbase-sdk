use super::*;

const BLOCK: u64 = 77;

fn key(byte: u8) -> B256 {
    B256::new([byte; 32])
}

fn filter() -> ContractFilter {
    ContractFilter {
        address: Address::new([0x11; 20]),
        topics: vec![key(0x22)],
    }
}

fn lifecycle(delivery: MonadDeliveryMode) -> ProposalLifecycle {
    lifecycle_with_removals(delivery, false)
}

fn lifecycle_with_removals(
    delivery: MonadDeliveryMode,
    emit_removed_logs: bool,
) -> ProposalLifecycle {
    ProposalLifecycle::new(
        143,
        delivery,
        emit_removed_logs,
        filter(),
        LifecycleLimits {
            max_proposals: 8,
            max_logs: 8,
            max_bytes: 1024,
        },
    )
}

#[test]
fn published_competing_branch_emits_removals_before_reorg() {
    let mut lifecycle = lifecycle_with_removals(MonadDeliveryMode::BlockOrdered, true);
    lifecycle.start(1, key(1), BLOCK).unwrap();
    matching_log(&mut lifecycle, 2, 0xaa);
    lifecycle.end(3, Some(BLOCK), key(0xa1)).unwrap();

    lifecycle.start(4, key(2), BLOCK).unwrap();
    matching_log(&mut lifecycle, 5, 0xbb);
    let switched = lifecycle.end(6, Some(BLOCK), key(0xb2)).unwrap();
    assert!(matches!(
        switched.as_slice(),
        [
            ExecutionEvent::Log(removed),
            ExecutionEvent::Reorg { .. },
            ExecutionEvent::Log(applied),
            ExecutionEvent::Head(_)
        ] if removed.removed
            && removed.data.as_ref() == [0xaa]
            && !applied.removed
            && applied.data.as_ref() == [0xbb]
    ));
}

fn matching_log(lifecycle: &mut ProposalLifecycle, sequence: u64, byte: u8) -> Vec<ExecutionEvent> {
    lifecycle
        .log(
            sequence,
            Some(BLOCK),
            2,
            0,
            filter().address,
            vec![key(0x22)],
            vec![byte],
        )
        .unwrap()
}

#[test]
fn finalized_mode_publishes_only_selected_proposal_and_releases_budgets() {
    let mut lifecycle = lifecycle(MonadDeliveryMode::Finalized);
    assert!(lifecycle.start(1, key(1), BLOCK).unwrap().is_empty());
    assert!(matching_log(&mut lifecycle, 2, 0xaa).is_empty());
    assert!(lifecycle.end(3, Some(BLOCK), key(0xa1)).unwrap().is_empty());
    assert!(lifecycle.start(4, key(2), BLOCK).unwrap().is_empty());
    assert!(matching_log(&mut lifecycle, 5, 0xbb).is_empty());
    assert!(lifecycle.end(6, Some(BLOCK), key(0xb2)).unwrap().is_empty());

    let output = lifecycle.finalized(7, key(2), BLOCK).unwrap();
    assert_eq!(output.len(), 2);
    assert!(matches!(
        &output[0],
        ExecutionEvent::Log(log)
            if log.data.as_ref() == [0xbb]
                && log.block_hash == Some(key(0xb2))
                && log.commitment == Commitment::Finalized
    ));
    assert!(matches!(
        &output[1],
        ExecutionEvent::Head(head)
            if head.block_hash == Some(key(0xb2))
                && head.commitment == Commitment::Finalized
    ));
    assert_eq!(lifecycle.proposals.len(), 1);
    assert_eq!(lifecycle.pending_logs, 0);
    assert_eq!(lifecycle.pending_bytes, 0);
    assert_eq!(lifecycle.verified(8, BLOCK).unwrap().len(), 1);
    assert!(lifecycle.proposals.is_empty());
}

#[test]
fn block_ordered_mode_announces_competing_proposal_reorg_before_logs() {
    let mut lifecycle = lifecycle(MonadDeliveryMode::BlockOrdered);
    lifecycle.start(1, key(1), BLOCK).unwrap();
    matching_log(&mut lifecycle, 2, 0xaa);
    let first = lifecycle.end(3, Some(BLOCK), key(0xa1)).unwrap();
    assert!(matches!(
        first.as_slice(),
        [ExecutionEvent::Log(_), ExecutionEvent::Head(_)]
    ));

    lifecycle.start(4, key(2), BLOCK).unwrap();
    matching_log(&mut lifecycle, 5, 0xbb);
    let second = lifecycle.end(6, Some(BLOCK), key(0xb2)).unwrap();
    assert!(matches!(
        second.as_slice(),
        [
            ExecutionEvent::Reorg { .. },
            ExecutionEvent::Log(_),
            ExecutionEvent::Head(_)
        ]
    ));
}

#[test]
fn realtime_mode_never_buffers_matching_payloads() {
    let mut lifecycle = lifecycle(MonadDeliveryMode::Realtime);
    let start = lifecycle.start(1, key(1), BLOCK).unwrap();
    assert!(matches!(start.as_slice(), [ExecutionEvent::Head(_)]));
    let output = matching_log(&mut lifecycle, 2, 0xaa);
    assert!(matches!(output.as_slice(), [ExecutionEvent::Log(_)]));
    assert_eq!(lifecycle.pending_logs, 0);
    assert_eq!(lifecycle.pending_bytes, 0);
}

#[test]
fn realtime_event_worker_retracts_an_abandoned_published_log() {
    let mut lifecycle = lifecycle_with_removals(MonadDeliveryMode::Realtime, true);
    lifecycle.start(1, key(1), BLOCK).unwrap();
    assert!(matches!(
        matching_log(&mut lifecycle, 2, 0xaa).as_slice(),
        [ExecutionEvent::Log(log)] if !log.removed
    ));
    lifecycle.end(3, Some(BLOCK), key(0xa1)).unwrap();

    let switched = lifecycle.start(4, key(2), BLOCK).unwrap();
    assert!(matches!(
        switched.as_slice(),
        [
            ExecutionEvent::Log(removed),
            ExecutionEvent::Reorg { .. },
            ExecutionEvent::Head(_)
        ] if removed.removed && removed.data.as_ref() == [0xaa]
    ));
}

#[test]
fn pending_log_byte_budget_fails_closed() {
    let mut lifecycle = ProposalLifecycle::new(
        143,
        MonadDeliveryMode::Finalized,
        false,
        filter(),
        LifecycleLimits {
            max_proposals: 2,
            max_logs: 2,
            max_bytes: 32,
        },
    );
    lifecycle.start(1, key(1), BLOCK).unwrap();
    let error = lifecycle
        .log(
            2,
            Some(BLOCK),
            0,
            0,
            filter().address,
            vec![key(0x22)],
            vec![0; 1],
        )
        .unwrap_err();
    assert!(error.to_string().contains("memory budget"));
}
