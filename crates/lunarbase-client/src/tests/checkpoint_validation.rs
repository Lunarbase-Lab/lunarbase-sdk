use super::*;

#[test]
fn checkpoint_binds_deployment_policy_and_validates_state() {
    let deployment = config().deployment;
    let mut indexer = QuoteIndexer::new(QuoteState::default(), deployment.clone());
    indexer.bootstrap(snapshot(100)).unwrap();
    let checkpoint = indexer.checkpoint().unwrap();

    assert!(checkpoint.is_compatible(&deployment));

    let mut changed = checkpoint.clone();
    changed.network = Network::Monad;
    assert!(!changed.is_compatible(&deployment));

    let mut changed = checkpoint.clone();
    changed.deployment_block += 1;
    assert!(!changed.is_compatible(&deployment));

    let mut changed = checkpoint.clone();
    changed.expect_whitelisted = !changed.expect_whitelisted;
    assert!(!changed.is_compatible(&deployment));

    let mut changed = checkpoint.clone();
    changed.explicit_lane_assets = vec![CASH];
    assert!(!changed.is_compatible(&deployment));

    let mut invalid_state = checkpoint.clone();
    invalid_state.state.cash = Address::ZERO;
    assert!(!invalid_state.is_compatible(&deployment));

    let mut invalid_whitelist = checkpoint.clone();
    invalid_whitelist.state.fee_profile.whitelisted = !invalid_whitelist.expect_whitelisted;
    assert!(!invalid_whitelist.is_compatible(&deployment));

    let mut invalid_hash = checkpoint;
    invalid_hash.cursor.block_hash = Some(B256::ZERO);
    assert!(!invalid_hash.is_compatible(&deployment));
}

#[test]
fn snapshot_chain_id_mismatch_is_rejected_before_state_installation() {
    let deployment = config().deployment;
    let mut indexer = QuoteIndexer::new(QuoteState::default(), deployment);
    let mut mismatched = snapshot(100);
    mismatched.cursor.chain_id = 1;

    assert!(matches!(
        indexer.bootstrap(mismatched),
        Err(IndexerError::Reducer(
            crate::state::reducer::ReducerError::ChainIdMismatch
        ))
    ));
    assert!(!indexer.reducer.is_ready());
}

#[test]
fn deployment_rejects_duplicate_or_zero_explicit_lanes() {
    let mut deployment = config().deployment;
    deployment.explicit_lane_assets = vec![ASSET, ASSET];
    assert!(deployment.validate().is_err());
    deployment.explicit_lane_assets = vec![Address::ZERO];
    assert!(deployment.validate().is_err());
}
