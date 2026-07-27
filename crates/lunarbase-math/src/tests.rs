use crate::arithmetic::WAD;
use crate::quote::{
    quote, solidity_exact_in_amount, solidity_exact_out_amount,
    solidity_exact_out_amount_for_request,
};
use crate::slot0::{
    LaneSlot0, apply_lane_update_slot0, decode_lane_slot0, encode_lane_slot0, encode_update_fees,
    lane_slot0_price,
};
use crate::state::{
    LaneState, QuoteError, QuoteMode, QuoteOutcome, QuoteRequest, QuoteState, UnavailableReason,
};
use crate::types::{Address, B256, Bytes, MathError, U256};
use serde::Deserialize;
use std::str::FromStr;

fn n(value: u64) -> U256 {
    U256::from(value)
}
fn address(value: u8) -> Address {
    let mut bytes = [0u8; 20];
    bytes[19] = value;
    Address::new(bytes)
}

#[test]
fn slot0_round_trips_boundaries_and_reserved_bits() {
    let fields = LaneSlot0 {
        price: (1u128 << 112) - 1,
        ask_fee_bps: (1u32 << 20) - 1,
        bid_fee_bps: (1u32 << 20) - 1,
        price_push_threshold: (1u8 << 7) - 1,
        threshold_enabled: true,
        latest_update_block: (1u64 << 40) - 1,
        exists: true,
        paused: true,
        block_delay: u8::MAX,
        slippage_k_bps: u32::MAX,
        reserved_high_bits: (1u16 << 14) - 1,
    };
    assert_eq!(
        decode_lane_slot0(encode_lane_slot0(&fields).unwrap()),
        fields
    );
}

#[test]
fn hot_lane_state_is_compact_and_raw() {
    assert!(std::mem::size_of::<LaneState>() <= 64);
    assert_eq!(lane_slot0_price(U256::MAX), (U256::ONE << 112) - U256::ONE);
}

#[test]
fn alloy_evm_primitives_use_canonical_hex_json() {
    let address = Address::repeat_byte(0x11);
    let hash = B256::repeat_byte(0x22);
    let bytes = Bytes::from(vec![0xab, 0xcd]);

    assert_eq!(
        serde_json::to_value(address).unwrap(),
        serde_json::json!("0x1111111111111111111111111111111111111111")
    );
    assert_eq!(
        format!("{address:#x}"),
        "0x1111111111111111111111111111111111111111"
    );
    assert_eq!(
        serde_json::to_value(hash).unwrap(),
        serde_json::json!("0x2222222222222222222222222222222222222222222222222222222222222222")
    );
    assert_eq!(
        serde_json::to_value(bytes).unwrap(),
        serde_json::json!("0xabcd")
    );
}

#[test]
fn direct_quote_matches_fee_split() {
    let cash = address(1);
    let asset = address(2);
    let mut state = QuoteState {
        cash,
        cash_reserve: u128::MAX,
        ..Default::default()
    };
    state.fee_profile.whitelisted = false;
    state.lanes.insert(
        asset,
        LaneState::new(
            encode_lane_slot0(&LaneSlot0 {
                price: (WAD * n(2)).try_into().unwrap(),
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
    state.fee_profile.partner_fee_bps.insert(asset, 500_000);
    let request = QuoteRequest {
        asset_in: cash,
        asset_out: asset,
        amount: n(100),
        mode: QuoteMode::ExactIn,
    };
    let outcome = quote(&request, 1, &state).unwrap();
    let QuoteOutcome::Available(result) = outcome else {
        panic!("quote unavailable")
    };
    assert_eq!(result.amount_in, n(100));
    assert_eq!(result.fee_asset, asset);
    assert!(result.amount_out > U256::ZERO);
}

#[test]
fn exact_out_asset_to_cash_uses_requested_cash_value() {
    let cash = address(1);
    let asset = address(2);
    let mut state = QuoteState {
        cash,
        cash_reserve: u128::MAX,
        ..Default::default()
    };
    state.fee_profile.whitelisted = false;
    state.lanes.insert(
        asset,
        LaneState::new(
            encode_lane_slot0(&LaneSlot0 {
                price: (WAD * n(2)).try_into().unwrap(),
                bid_fee_bps: 10_000,
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
        asset_in: asset,
        asset_out: cash,
        amount: n(100),
        mode: QuoteMode::ExactOut,
    };
    let QuoteOutcome::Available(result) = quote(&request, 1, &state).unwrap() else {
        panic!("quote unavailable")
    };
    assert_eq!(result.amount_in, n(51));
}

#[test]
fn lane_quote_ttl_includes_boundary_and_expires_next_block() {
    let cash = address(1);
    let asset = address(2);
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
                latest_update_block: 100,
                exists: true,
                block_delay: 3,
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
        amount: n(100),
        mode: QuoteMode::ExactIn,
    };

    assert!(matches!(
        quote(&request, 100, &state).unwrap(),
        QuoteOutcome::Available(_)
    ));
    assert!(matches!(
        quote(&request, 103, &state).unwrap(),
        QuoteOutcome::Available(_)
    ));
    assert_eq!(
        quote(&request, 104, &state).unwrap(),
        QuoteOutcome::Unavailable(UnavailableReason::StaleLane(asset))
    );
}

#[test]
fn sentinels_match_solidity_edge_behavior() {
    let outcome = QuoteOutcome::Unavailable(UnavailableReason::MissingLane(Address::ZERO));
    assert_eq!(solidity_exact_in_amount(&outcome), U256::ZERO);
    assert_eq!(solidity_exact_out_amount(&outcome), U256::MAX);
}

#[test]
fn packed_update_preserves_unwritten_bits() {
    let previous = encode_lane_slot0(&LaneSlot0 {
        price: 7,
        ask_fee_bps: 8,
        bid_fee_bps: 9,
        price_push_threshold: 63,
        threshold_enabled: true,
        latest_update_block: 10,
        exists: true,
        paused: false,
        block_delay: 15,
        slippage_k_bps: 16,
        reserved_high_bits: (1u16 << 14) - 1,
    })
    .unwrap();
    let updated =
        apply_lane_update_slot0(previous, 11, encode_update_fees(12, 13).unwrap(), 14).unwrap();
    let fields = decode_lane_slot0(updated);
    assert_eq!(fields.price, 11);
    assert_eq!(fields.ask_fee_bps, 12);
    assert_eq!(fields.bid_fee_bps, 13);
    assert_eq!(fields.latest_update_block, 14);
    assert_eq!(fields.price_push_threshold, 63);
    assert!(fields.threshold_enabled);
    assert_eq!(fields.reserved_high_bits, (1u16 << 14) - 1);
    assert!(fields.exists);
    assert_eq!(fields.block_delay, 15);
    assert_eq!(fields.slippage_k_bps, 16);
}

#[test]
fn packed_update_matches_threshold_pause_and_accepts_later_updates() {
    let base = encode_lane_slot0(&LaneSlot0 {
        price: 100,
        price_push_threshold: 10,
        threshold_enabled: true,
        exists: true,
        ..Default::default()
    })
    .unwrap();
    let boundary = apply_lane_update_slot0(base, 110, 0, 7).unwrap();
    let boundary_fields = decode_lane_slot0(boundary);
    assert_eq!(boundary_fields.price, 110);
    assert!(!boundary_fields.paused);

    for price in [89, 111] {
        let paused = apply_lane_update_slot0(base, price, 0, 8).unwrap();
        let fields = decode_lane_slot0(paused);
        assert_eq!(fields.price, price);
        assert!(fields.paused);
    }

    let paused = apply_lane_update_slot0(base, 89, 0, 8).unwrap();
    let refreshed =
        apply_lane_update_slot0(paused, 77, encode_update_fees(12, 13).unwrap(), 9).unwrap();
    let refreshed_fields = decode_lane_slot0(refreshed);
    assert_eq!(refreshed_fields.price, 77);
    assert_eq!(refreshed_fields.ask_fee_bps, 12);
    assert_eq!(refreshed_fields.bid_fee_bps, 13);
    assert_eq!(refreshed_fields.latest_update_block, 9);
    assert!(refreshed_fields.paused);
}

#[test]
fn reserve_boundary_matches_exact_in_and_exact_out_settlement() {
    let cash = address(1);
    let asset = address(2);
    let slot0 = encode_lane_slot0(&LaneSlot0 {
        price: WAD.try_into().unwrap(),
        ask_fee_bps: 10_000,
        latest_update_block: 1,
        exists: true,
        ..Default::default()
    })
    .unwrap();
    let mut state = QuoteState {
        cash,
        cash_reserve: u128::MAX,
        ..Default::default()
    };
    state
        .lanes
        .insert(asset, LaneState::new(slot0, u128::MAX, 1_000_000));

    let exact_in = QuoteRequest {
        asset_in: cash,
        asset_out: asset,
        amount: n(100),
        mode: QuoteMode::ExactIn,
    };
    let QuoteOutcome::Available(result) = quote(&exact_in, 1, &state).unwrap() else {
        panic!("reference exact-in quote unavailable")
    };
    let required = u128::try_from(result.amount_out + result.fee_amount).unwrap();
    state.lanes.get_mut(&asset).unwrap().asset_reserve = required;
    assert!(matches!(
        quote(&exact_in, 1, &state).unwrap(),
        QuoteOutcome::Available(_)
    ));
    state.lanes.get_mut(&asset).unwrap().asset_reserve = required - 1;
    assert_eq!(
        quote(&exact_in, 1, &state).unwrap(),
        QuoteOutcome::Unavailable(UnavailableReason::InsufficientOutputReserve(asset))
    );

    let exact_out = QuoteRequest {
        mode: QuoteMode::ExactOut,
        amount: n(100),
        ..exact_in
    };
    state.lanes.get_mut(&asset).unwrap().asset_reserve = 100;
    assert!(matches!(
        quote(&exact_out, 1, &state).unwrap(),
        QuoteOutcome::Available(_)
    ));
    state.lanes.get_mut(&asset).unwrap().asset_reserve = 99;
    assert_eq!(
        quote(&exact_out, 1, &state).unwrap(),
        QuoteOutcome::Unavailable(UnavailableReason::InsufficientOutputReserve(asset))
    );
}

#[test]
fn route_preserves_contract_evaluation_order_before_zero_price_sentinel() {
    let cash = address(1);
    let asset_in = address(2);
    let asset_out = address(3);
    let mut state = QuoteState {
        cash,
        cash_reserve: u128::MAX,
        ..Default::default()
    };
    for (asset, price) in [(&asset_in, 0), (&asset_out, (1u128 << 112) - 1)] {
        let slot0 = encode_lane_slot0(&LaneSlot0 {
            price,
            latest_update_block: 1,
            exists: true,
            ..Default::default()
        })
        .unwrap();
        state
            .lanes
            .insert(*asset, LaneState::new(slot0, u128::MAX, 1));
    }
    let request = QuoteRequest {
        asset_in,
        asset_out,
        amount: U256::MAX,
        mode: QuoteMode::ExactOut,
    };

    assert_eq!(
        quote(&request, 1, &state).unwrap_err(),
        QuoteError::Arithmetic(MathError::Overflow)
    );
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoldenFile {
    vectors: Vec<GoldenVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoldenVector {
    name: String,
    cash: String,
    asset_in: String,
    asset_out: String,
    mode: QuoteMode,
    amount: String,
    execution_block_number: String,
    blacklist_fee_multiplier: String,
    whitelisted: bool,
    partner_fee_bps: String,
    lane_in: Option<GoldenLane>,
    lane_out: Option<GoldenLane>,
    expected: Option<GoldenResult>,
    expected_public_amount: Option<String>,
    expected_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoldenLane {
    price: String,
    ask_fee_bps: String,
    bid_fee_bps: String,
    latest_update_block: String,
    exists: bool,
    paused: bool,
    block_delay: String,
    slippage_k_bps: String,
    principal: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoldenResult {
    amount_in: String,
    amount_out: String,
    fee_asset: String,
    fee_amount: String,
    partner_fee: String,
    treasury_fee: String,
}

fn golden_u256(value: &str) -> U256 {
    U256::from_str(value).expect("valid decimal U256 fixture value")
}

#[test]
fn shared_quote_vectors_match_rust_math() {
    let fixture: GoldenFile =
        serde_json::from_str(include_str!("../../../fixtures/quote-vectors.json"))
            .expect("valid quote vector fixture");
    for vector in fixture.vectors {
        let cash = Address::from_str(&vector.cash).unwrap();
        let asset_in = Address::from_str(&vector.asset_in).unwrap();
        let asset_out = Address::from_str(&vector.asset_out).unwrap();
        let mut state = QuoteState {
            cash,
            cash_reserve: u128::MAX,
            ..Default::default()
        };
        state.fee_profile.whitelisted = vector.whitelisted;
        state.fee_profile.blacklist_fee_multiplier = golden_u256(&vector.blacklist_fee_multiplier);
        let fee_asset = if vector.mode == QuoteMode::ExactIn {
            asset_out
        } else {
            asset_in
        };
        state
            .fee_profile
            .partner_fee_bps
            .insert(fee_asset, vector.partner_fee_bps.parse().unwrap());
        for (asset, lane) in [(asset_in, vector.lane_in), (asset_out, vector.lane_out)] {
            if let Some(lane) = lane {
                let slot0 = encode_lane_slot0(&LaneSlot0 {
                    price: lane.price.parse().unwrap(),
                    ask_fee_bps: lane.ask_fee_bps.parse().unwrap(),
                    bid_fee_bps: lane.bid_fee_bps.parse().unwrap(),
                    latest_update_block: lane.latest_update_block.parse().unwrap(),
                    exists: lane.exists,
                    paused: lane.paused,
                    block_delay: lane.block_delay.parse().unwrap(),
                    slippage_k_bps: lane.slippage_k_bps.parse().unwrap(),
                    ..Default::default()
                })
                .unwrap();
                state.lanes.insert(
                    asset,
                    LaneState::new(slot0, u128::MAX, lane.principal.parse().unwrap()),
                );
            }
        }
        let request = QuoteRequest {
            asset_in,
            asset_out,
            amount: golden_u256(&vector.amount),
            mode: vector.mode,
        };
        let outcome = quote(
            &request,
            vector.execution_block_number.parse().unwrap(),
            &state,
        );
        if let Some(expected_error) = vector.expected_error {
            assert_eq!(expected_error, "Overflow", "{}", vector.name);
            assert_eq!(
                outcome.unwrap_err(),
                QuoteError::Arithmetic(MathError::Overflow),
                "{}",
                vector.name
            );
            continue;
        }
        let outcome = outcome.unwrap();
        if let Some(public_amount) = vector.expected_public_amount {
            let actual = if vector.mode == QuoteMode::ExactIn {
                solidity_exact_in_amount(&outcome)
            } else {
                solidity_exact_out_amount_for_request(&request, &outcome)
            };
            assert_eq!(actual, golden_u256(&public_amount), "{}", vector.name);
        } else {
            let expected = vector.expected.expect("full expected result");
            let QuoteOutcome::Available(actual) = outcome else {
                panic!("golden vector unexpectedly unavailable: {}", vector.name)
            };
            assert_eq!(actual.amount_in, golden_u256(&expected.amount_in));
            assert_eq!(actual.amount_out, golden_u256(&expected.amount_out));
            assert_eq!(
                actual.fee_asset,
                Address::from_str(&expected.fee_asset).unwrap()
            );
            assert_eq!(actual.fee_amount, golden_u256(&expected.fee_amount));
            assert_eq!(actual.partner_fee, golden_u256(&expected.partner_fee));
            assert_eq!(actual.treasury_fee, golden_u256(&expected.treasury_fee));
        }
    }
}
