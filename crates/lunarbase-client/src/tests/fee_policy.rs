use super::{ASSET, ROUTER, config, cursor, snapshot};
use crate::bootstrap::VerifiedRouterSnapshot;
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::IndexerError;
use crate::model::{Commitment, QuoteEvent};
use crate::state::reducer::{QuoteReducer, ReducerError};
use lunarbase_math::{FeeClass, QuoteState};

#[test]
fn verified_router_updates_allocation_and_rejects_fee_class_changes() {
    let verified = VerifiedRouterSnapshot {
        router: ROUTER,
        partner_fee_bps: [(ASSET, 100_000)].into_iter().collect(),
    };
    let mut reducer = QuoteReducer::new(
        QuoteState {
            cash: ASSET,
            ..Default::default()
        },
        FeeClass::Whitelisted,
        Some(verified),
    );
    reducer.bootstrap(cursor(100, Commitment::Canonical));
    let mut fee_cursor = cursor(101, Commitment::Realtime);
    fee_cursor.transaction_index = Some(0);
    fee_cursor.log_index = Some(0);
    reducer
        .apply(
            fee_cursor,
            QuoteEvent::PartnerFeeSet {
                router: ROUTER,
                asset: ASSET,
                fee: 900_000,
            },
        )
        .unwrap();
    assert_eq!(
        reducer
            .verified_router_snapshot()
            .unwrap()
            .partner_fee_bps
            .get(&ASSET),
        Some(&900_000)
    );

    let mut whitelist_cursor = cursor(101, Commitment::Realtime);
    whitelist_cursor.transaction_index = Some(0);
    whitelist_cursor.log_index = Some(1);
    assert_eq!(
        reducer.apply(
            whitelist_cursor,
            QuoteEvent::WhitelistSet {
                router: ROUTER,
                whitelisted: false,
            },
        ),
        Err(ReducerError::FeeClassMismatch)
    );
}

#[test]
fn snapshot_verified_router_must_match_deployment_policy() {
    let mut exact_deployment = config().deployment;
    exact_deployment.verified_router = Some(ROUTER);
    let mut exact_indexer = QuoteIndexer::new(QuoteState::default(), exact_deployment);
    assert!(matches!(
        exact_indexer.bootstrap(snapshot(100)),
        Err(IndexerError::Source(_))
    ));

    let mut unexpected = snapshot(100);
    unexpected.verified_router = Some(VerifiedRouterSnapshot {
        router: ROUTER,
        partner_fee_bps: [(ASSET, 100_000)].into_iter().collect(),
    });
    let mut class_indexer = QuoteIndexer::new(QuoteState::default(), config().deployment);
    assert!(matches!(
        class_indexer.bootstrap(unexpected),
        Err(IndexerError::Source(_))
    ));
}

#[test]
fn verified_router_requires_refresh_before_quoting_a_new_lane() {
    let verified = VerifiedRouterSnapshot {
        router: ROUTER,
        partner_fee_bps: [(ASSET, 100_000)].into_iter().collect(),
    };
    let mut reducer =
        QuoteReducer::new(QuoteState::default(), FeeClass::Whitelisted, Some(verified));
    reducer.bootstrap(cursor(100, Commitment::Canonical));
    let mut event_cursor = cursor(101, Commitment::Realtime);
    event_cursor.transaction_index = Some(0);
    event_cursor.log_index = Some(0);

    assert_eq!(
        reducer.apply(event_cursor, QuoteEvent::LaneAdded { asset: super::CORE },),
        Err(ReducerError::VerifiedRouterRefreshRequired)
    );
}
