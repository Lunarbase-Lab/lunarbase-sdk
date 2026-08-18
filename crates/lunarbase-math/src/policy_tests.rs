use crate::arithmetic::WAD;
use crate::quote::quote;
use crate::slot0::{LaneSlot0, encode_lane_slot0};
use crate::state::{
    FeeClass, LaneState, QuoteMode, QuoteOutcome, QuotePolicy, QuoteRequest, QuoteState,
};
use crate::types::{Address, U256};

#[test]
fn verified_partner_profiles_change_only_accounting_allocation() {
    let cash = Address::with_last_byte(1);
    let asset = Address::with_last_byte(2);
    let mut state = QuoteState {
        cash,
        cash_reserve: u128::MAX,
        ..Default::default()
    };
    state.lanes.insert(
        asset,
        LaneState::new(
            encode_lane_slot0(&LaneSlot0 {
                price: WAD.try_into().unwrap(),
                ask_fee_bps: 10_000,
                latest_update_block: 1,
                exists: true,
                ..Default::default()
            })
            .unwrap(),
            u128::MAX,
            1_000_000,
        ),
    );
    let request = QuoteRequest {
        asset_in: cash,
        asset_out: asset,
        amount: U256::from(100_000),
        mode: QuoteMode::ExactIn,
    };
    let result = |partner_fee_bps| {
        let QuoteOutcome::Available(result) = quote(
            &request,
            1,
            &state,
            QuotePolicy::with_verified_partner_fee(FeeClass::Whitelisted, partner_fee_bps),
        )
        .unwrap() else {
            panic!("quote unavailable")
        };
        result
    };
    let low_share = result(100_000);
    let high_share = result(900_000);

    assert_eq!(low_share.amount_in, high_share.amount_in);
    assert_eq!(low_share.amount_out, high_share.amount_out);
    assert_eq!(low_share.fee_asset, high_share.fee_asset);
    assert_eq!(low_share.fee_amount, high_share.fee_amount);
    assert_ne!(low_share.fee_allocation, high_share.fee_allocation);
}
