use super::*;

#[test]
fn canonical_log_position_ignores_transport_sub_index() {
    let mut reducer = QuoteReducer::new(
        snapshot(100).state,
        lunarbase_math::FeeClass::Whitelisted,
        None,
    );
    reducer.bootstrap(cursor(100, Commitment::Finalized));
    let mut first = event_cursor(101, OLD_HASH, 0);
    first.source_sequence = Some(1);
    first.source_sub_index = Some(1);
    reducer
        .apply(
            first,
            crate::model::QuoteEvent::LaneRemoved { asset: ASSET },
        )
        .unwrap();
    let mut cloned = reducer.clone();
    let mut duplicate = event_cursor(101, OLD_HASH, 0);
    duplicate.source_sequence = Some(2);
    duplicate.source_sub_index = Some(2);
    cloned
        .apply(
            duplicate,
            crate::model::QuoteEvent::LaneRemoved { asset: ASSET },
        )
        .unwrap();
    assert!(!cloned.state().lanes.contains_key(&ASSET));

    let mut altered = event_cursor(101, OLD_HASH, 0);
    altered.source_sequence = Some(3);
    altered.source_sub_index = Some(3);
    assert_eq!(
        reducer.apply(
            altered,
            crate::model::QuoteEvent::LaneAdded { asset: ASSET },
        ),
        Err(ReducerError::EventPayloadMismatch)
    );
    assert!(!reducer.state().lanes.contains_key(&ASSET));

    reducer
        .rewind_head(cursor(100, Commitment::Finalized))
        .unwrap();
    reducer
        .apply(
            event_cursor(101, NEW_HASH, 0),
            crate::model::QuoteEvent::LaneAdded { asset: ASSET },
        )
        .unwrap();
    assert!(reducer.state().lanes.contains_key(&ASSET));
}

#[test]
fn late_log_after_newer_head_uses_separate_event_ordering() {
    let mut state = snapshot(100).state;
    assert!(state.lanes.contains_key(&ASSET));
    let mut reducer = QuoteReducer::new(
        std::mem::take(&mut state),
        lunarbase_math::FeeClass::Whitelisted,
        None,
    );
    reducer.bootstrap(cursor(100, Commitment::Finalized));
    let mut newer_head = cursor(102, Commitment::Realtime);
    newer_head.block_hash = Some(B256::new([0x12; 32]));
    newer_head.source_sequence = Some(10);
    reducer.observe_head(newer_head).unwrap();

    let mut late = cursor(101, Commitment::Realtime);
    late.block_hash = Some(OLD_HASH);
    late.transaction_index = Some(0);
    late.log_index = Some(0);
    late.source_sequence = Some(11);
    reducer
        .apply(late, crate::model::QuoteEvent::LaneRemoved { asset: ASSET })
        .unwrap();

    let published = reducer.cursor().unwrap();
    assert_eq!(published.block_number, 102);
    assert_eq!(published.execution_block_number, 102);
    assert_eq!(published.source_sequence, Some(11));
    assert!(!reducer.state().lanes.contains_key(&ASSET));

    let mut regressed = cursor(100, Commitment::Realtime);
    regressed.transaction_index = Some(1);
    regressed.log_index = Some(0);
    assert_eq!(
        reducer.apply(
            regressed,
            crate::model::QuoteEvent::LaneRemoved { asset: ASSET },
        ),
        Err(ReducerError::CursorRegression)
    );
}

#[test]
fn progressive_head_cannot_relabel_applied_state_to_another_hash() {
    let mut reducer = QuoteReducer::new(
        snapshot(100).state,
        lunarbase_math::FeeClass::Whitelisted,
        None,
    );
    reducer.bootstrap(cursor(100, Commitment::Finalized));
    let mut old_event = event_cursor(101, OLD_HASH, 0);
    old_event.source_sequence = Some(1);
    reducer
        .apply(
            old_event,
            crate::model::QuoteEvent::LaneRemoved { asset: ASSET },
        )
        .unwrap();

    let mut replacement_head = cursor_with_hash(101, NEW_HASH, Commitment::Realtime);
    replacement_head.source_sequence = Some(2);
    assert_eq!(
        reducer.observe_head(replacement_head),
        Err(ReducerError::BlockHashMismatch)
    );
    assert_eq!(reducer.cursor().unwrap().block_hash, Some(OLD_HASH));
    assert!(!reducer.state().lanes.contains_key(&ASSET));
}

#[test]
fn newly_identified_head_cannot_relabel_hashless_applied_state() {
    let mut reducer = QuoteReducer::new(
        snapshot(100).state,
        lunarbase_math::FeeClass::Whitelisted,
        None,
    );
    reducer.bootstrap(cursor(100, Commitment::Finalized));
    let mut event = event_cursor(101, OLD_HASH, 0);
    event.block_hash = None;
    event.source_sequence = Some(1);
    reducer
        .apply(
            event,
            crate::model::QuoteEvent::LaneRemoved { asset: ASSET },
        )
        .unwrap();

    let mut head = cursor_with_hash(101, NEW_HASH, Commitment::Realtime);
    head.source_sequence = Some(2);
    assert_eq!(
        reducer.observe_head(head),
        Err(ReducerError::BlockHashMismatch)
    );
    assert_eq!(reducer.cursor().unwrap().block_hash, None);
}

#[test]
fn same_height_event_hash_presence_requires_a_gap() {
    let mut reducer = QuoteReducer::new(
        snapshot(100).state,
        lunarbase_math::FeeClass::Whitelisted,
        None,
    );
    reducer.bootstrap(cursor(100, Commitment::Finalized));
    let mut first = event_cursor(101, OLD_HASH, 0);
    first.block_hash = None;
    first.source_sequence = Some(1);
    reducer
        .apply(
            first,
            crate::model::QuoteEvent::LaneRemoved { asset: ASSET },
        )
        .unwrap();
    let mut identified = event_cursor(101, OLD_HASH, 1);
    identified.source_sequence = Some(2);
    assert_eq!(
        reducer.apply(
            identified,
            crate::model::QuoteEvent::LaneRemoved { asset: ASSET },
        ),
        Err(ReducerError::BlockHashMismatch)
    );

    let mut unsequenced = QuoteReducer::new(
        snapshot(100).state,
        lunarbase_math::FeeClass::Whitelisted,
        None,
    );
    unsequenced.bootstrap(cursor(100, Commitment::Finalized));
    let mut first = event_cursor(101, OLD_HASH, 0);
    first.block_hash = None;
    unsequenced
        .apply(
            first,
            crate::model::QuoteEvent::LaneRemoved { asset: ASSET },
        )
        .unwrap();
    let mut second = event_cursor(101, OLD_HASH, 1);
    second.block_hash = None;
    assert_eq!(
        unsequenced.apply(
            second,
            crate::model::QuoteEvent::LaneRemoved { asset: ASSET },
        ),
        Err(ReducerError::BlockHashMismatch)
    );
}
