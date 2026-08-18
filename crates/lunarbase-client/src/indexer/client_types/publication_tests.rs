use super::{PendingCorrectionAdmission, SharedQuoteState};
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::IndexerError;
use crate::model::{
    ChainCursor, Commitment, DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network,
};
use lunarbase_math::{Address, B256, FeeClass, QuoteState};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

#[test]
fn held_reader_snapshot_does_not_block_atomic_publication() {
    let shared = shared_state();
    let held = shared.load_indexer().unwrap();
    let (generation, candidate) = shared.indexer_candidate().unwrap();

    let retired = shared
        .publish_indexer_if_generation(generation, candidate)
        .unwrap()
        .expect("matching generation publishes");
    let current = shared.load_indexer().unwrap();

    assert!(Arc::ptr_eq(&held, &retired));
    assert!(!Arc::ptr_eq(&held, &current));
    assert_eq!(shared.publication_generation(), 1);
    drop(current);
    drop(retired);
    drop(held);
}

#[test]
fn competing_publishers_allow_exactly_one_generation_match() {
    let shared = Arc::new(shared_state());
    let (generation_a, candidate_a) = shared.indexer_candidate().unwrap();
    let (generation_b, candidate_b) = shared.indexer_candidate().unwrap();
    assert_eq!(generation_a, generation_b);

    let barrier = Arc::new(Barrier::new(3));
    let first = {
        let shared = Arc::clone(&shared);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            shared
                .publish_indexer_if_generation(generation_a, candidate_a)
                .unwrap()
                .is_some()
        })
    };
    let second = {
        let shared = Arc::clone(&shared);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            shared
                .publish_indexer_if_generation(generation_b, candidate_b)
                .unwrap()
                .is_some()
        })
    };

    barrier.wait();
    let successes = usize::from(first.join().unwrap()) + usize::from(second.join().unwrap());
    assert_eq!(successes, 1);
    assert_eq!(shared.publication_generation(), 1);
}

#[test]
fn publication_progress_holds_quote_admission_until_completion() {
    let shared = Arc::new(shared_state());
    assert!(shared.publish_available_if(shared.availability_token()));
    let publication = shared.availability.begin_publication().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::sync_channel(1);
    let waiter = {
        let shared = Arc::clone(&shared);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            sender.send(shared.quote_path_available(1)).unwrap();
        })
    };

    barrier.wait();
    assert!(receiver.recv_timeout(Duration::from_millis(10)).is_err());
    assert!(shared.availability.complete_publication(publication));
    assert!(receiver.recv_timeout(Duration::from_secs(1)).unwrap());
    waiter.join().unwrap();
}

#[test]
fn pending_correction_admission_blocks_quotes_and_drop_fails_closed() {
    let shared = Arc::new(shared_state());
    assert!(shared.publish_available_if(shared.availability_token()));
    let admission =
        PendingCorrectionAdmission::begin(Arc::clone(&shared)).expect("ready state is leased");
    let token = admission.token();
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::sync_channel(1);
    let waiter = {
        let shared = Arc::clone(&shared);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            sender.send(shared.quote_path_available(1)).unwrap();
        })
    };

    barrier.wait();
    assert!(receiver.recv_timeout(Duration::from_millis(10)).is_err());
    assert!(shared.complete_correction(token));
    admission.disarm();
    assert!(receiver.recv_timeout(Duration::from_secs(1)).unwrap());
    waiter.join().unwrap();

    let abandoned =
        PendingCorrectionAdmission::begin(Arc::clone(&shared)).expect("ready state is leased");
    drop(abandoned);
    assert!(!shared.is_available());
    assert!(!shared.quote_path_available(1));
}

#[test]
fn panicking_private_mutation_keeps_snapshot_and_poison_fails_closed() {
    let shared = shared_state();
    let before = shared.indexer.load_full();
    assert!(before.reducer.cursor().is_none());
    let cursor = ChainCursor::block(1, 99, Some(B256::new([9; 32])), Commitment::Realtime);

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = shared.mutate_indexer(|candidate| -> () {
            candidate.reducer.bootstrap(cursor);
            panic!("expected private mutation panic");
        });
    }));
    assert!(panic.is_err());

    let after = shared.indexer.load_full();
    assert!(Arc::ptr_eq(&before, &after));
    assert!(after.reducer.cursor().is_none());
    assert!(matches!(
        shared.load_indexer(),
        Err(IndexerError::LockPoisoned)
    ));
    assert!(!shared.quote_path_available(1));
    assert!(matches!(
        shared.publish_indexer(after.as_ref().clone()),
        Err(IndexerError::LockPoisoned)
    ));
    assert_eq!(shared.publication_generation(), 0);
}

fn shared_state() -> SharedQuoteState {
    SharedQuoteState::new_not_ready(QuoteIndexer::new(
        QuoteState::default(),
        DeploymentConfig {
            network: Network::Evm,
            chain_id: 1,
            core: Address::new([1; 20]),
            fee_class: FeeClass::Whitelisted,
            verified_router: None,
            deployment_block: 0,
            expected_implementation: Address::new([2; 20]),
            expected_implementation_code_hash: B256::new([3; 32]),
            contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            explicit_lane_assets: Vec::new(),
        },
    ))
}
