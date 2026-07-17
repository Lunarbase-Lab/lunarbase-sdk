#[test]
fn provisional_overlay_commits_only_after_canonical_match() {
    let asset = address(2);
    let mut overlay = ProvisionalOverlay::default();
    overlay.begin(cursor(0));
    let event = QuoteEvent::LaneAdded { asset };
    overlay.push(cursor(1), event.clone());
    let canonical = vec![(cursor(1), event)];
    assert_eq!(
        overlay.commit_canonical(&canonical).unwrap(),
        Some(cursor(1))
    );
    assert!(overlay.updates().is_empty());

    overlay.begin(cursor(2));
    overlay.push(cursor(3), QuoteEvent::SwapExecuted);
    overlay.discard();
    assert!(overlay.updates().is_empty());
}

#[test]
#[cfg(feature = "monad")]
fn monad_filtered_logs_allow_sparse_global_sequences_but_reject_regression() {
    let mut tracker = MonadRingTracker::default();
    assert!(tracker.observe_sparse(100, 0).unwrap());
    assert!(tracker.observe_sparse(104, 0).unwrap());
    assert!(!tracker.observe_sparse(104, 0).unwrap());
    assert!(tracker.observe_sparse(104, 1).unwrap());
    assert!(matches!(
        tracker.observe_sparse(103, 0),
        Err(SourceError::Gap(_))
    ));
}

#[test]
fn heads_promote_commitment_without_regressing_event_cursor() {
    let mut reducer = QuoteReducer::new(QuoteState::default());
    let mut event_cursor = cursor(3);
    event_cursor.block_hash = Some([7u8; 32]);
    reducer.bootstrap(event_cursor.clone());
    reducer
        .observe_head(ChainCursor {
            chain_id: 8453,
            block_number: event_cursor.block_number,
            block_hash: Some([7u8; 32]),
            transaction_index: None,
            log_index: None,
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Finalized,
        })
        .unwrap();
    assert_eq!(reducer.cursor().unwrap().commitment, Commitment::Finalized);
    assert_eq!(reducer.cursor().unwrap().log_index, event_cursor.log_index);

    reducer
        .observe_head(ChainCursor::block(
            8453,
            9,
            Some([8u8; 32]),
            Commitment::Realtime,
        ))
        .unwrap();
    assert_eq!(reducer.cursor().unwrap().block_number, 10);
    assert_eq!(reducer.cursor().unwrap().commitment, Commitment::Finalized);
}
