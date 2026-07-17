use super::*;
use serde::Deserialize;
use std::str::FromStr;

fn n(value: u64) -> U256 {
    U256::from(value)
}
fn address(value: u8) -> Address {
    let mut bytes = [0u8; 20];
    bytes[19] = value;
    Address(bytes)
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
        reserved_high_bits: (1u64 << 56) - 1,
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
fn direct_quote_matches_fee_split() {
    let cash = address(1);
    let asset = address(2);
    let mut state = QuoteState {
        cash,
        ..Default::default()
    };
    state.fee_profile.whitelisted = false;
    state.lanes.insert(
        asset,
        LaneState::new(
            encode_lane_slot0(&LaneSlot0 {
                price: (WAD * n(2)).try_into().unwrap(),
                ask_fee_bps: 10_000,
                ..Default::default()
            })
            .unwrap(),
            1_000_000,
            0,
            0,
            true,
            false,
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
        ..Default::default()
    };
    state.fee_profile.whitelisted = false;
    state.lanes.insert(
        asset,
        LaneState::new(
            encode_lane_slot0(&LaneSlot0 {
                price: (WAD * n(2)).try_into().unwrap(),
                bid_fee_bps: 10_000,
                ..Default::default()
            })
            .unwrap(),
            1_000_000,
            0,
            0,
            true,
            false,
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
        reserved_high_bits: (1u64 << 56) - 1,
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
    assert_eq!(fields.reserved_high_bits, (1u64 << 56) - 1);
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
        let cash = Address::from_hex(&vector.cash).unwrap();
        let asset_in = Address::from_hex(&vector.asset_in).unwrap();
        let asset_out = Address::from_hex(&vector.asset_out).unwrap();
        let mut state = QuoteState {
            cash,
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
                    ..Default::default()
                })
                .unwrap();
                state.lanes.insert(
                    asset,
                    LaneState::new(
                        slot0,
                        lane.principal.parse().unwrap(),
                        lane.slippage_k_bps.parse().unwrap(),
                        lane.block_delay.parse().unwrap(),
                        lane.exists,
                        lane.paused,
                    ),
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
                Address::from_hex(&expected.fee_asset).unwrap()
            );
            assert_eq!(actual.fee_amount, golden_u256(&expected.fee_amount));
            assert_eq!(actual.partner_fee, golden_u256(&expected.partner_fee));
            assert_eq!(actual.treasury_fee, golden_u256(&expected.treasury_fee));
        }
    }
}
