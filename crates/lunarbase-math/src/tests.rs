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
        price: (U256::ONE << 112) - U256::ONE,
        ask_fee_bps: (U256::ONE << 20) - U256::ONE,
        bid_fee_bps: (U256::ONE << 20) - U256::ONE,
        price_push_threshold: (U256::ONE << 7) - U256::ONE,
        threshold_enabled: true,
        latest_update_block: (U256::ONE << 40) - U256::ONE,
        reserved_high_bits: (U256::ONE << 56) - U256::ONE,
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
    let router = address(3);
    let mut state = QuoteState {
        cash,
        state_version: 1,
        blacklist_fee_multiplier: n(1),
        ..Default::default()
    };
    state.lanes.insert(
        asset,
        LaneState {
            exists: true,
            slot0: encode_lane_slot0(&LaneSlot0 {
                price: WAD * n(2),
                ask_fee_bps: n(10_000),
                ..Default::default()
            })
            .unwrap(),
            ..Default::default()
        },
    );
    state.total_principal_amount.insert(asset, n(1_000_000));
    state.partner_fee_bps.insert((router, asset), n(500_000));
    let context = QuoteContext {
        cash,
        execution_block_number: n(1),
        state_version: 1,
    };
    let request = QuoteRequest {
        router,
        asset_in: cash,
        asset_out: asset,
        amount: n(100),
        mode: QuoteMode::ExactIn,
    };
    let outcome = quote(&request, &context, &state).unwrap();
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
    let router = address(3);
    let mut state = QuoteState {
        cash,
        state_version: 1,
        blacklist_fee_multiplier: n(1),
        ..Default::default()
    };
    state.lanes.insert(
        asset,
        LaneState {
            exists: true,
            slot0: encode_lane_slot0(&LaneSlot0 {
                price: WAD * n(2),
                bid_fee_bps: n(10_000),
                ..Default::default()
            })
            .unwrap(),
            ..Default::default()
        },
    );
    state.total_principal_amount.insert(asset, n(1_000_000));
    let context = QuoteContext {
        cash,
        execution_block_number: n(1),
        state_version: 1,
    };
    let request = QuoteRequest {
        router,
        asset_in: asset,
        asset_out: cash,
        amount: n(100),
        mode: QuoteMode::ExactOut,
    };
    let QuoteOutcome::Available(result) = quote(&request, &context, &state).unwrap() else {
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
        price: n(7),
        ask_fee_bps: n(8),
        bid_fee_bps: n(9),
        price_push_threshold: n(63),
        threshold_enabled: true,
        latest_update_block: n(10),
        reserved_high_bits: (U256::ONE << 56) - U256::ONE,
    })
    .unwrap();
    let updated = apply_lane_update_slot0(
        previous,
        n(11),
        encode_update_fees(n(12), n(13)).unwrap(),
        n(14),
    )
    .unwrap();
    let fields = decode_lane_slot0(updated);
    assert_eq!(fields.price, n(11));
    assert_eq!(fields.ask_fee_bps, n(12));
    assert_eq!(fields.bid_fee_bps, n(13));
    assert_eq!(fields.latest_update_block, n(14));
    assert_eq!(fields.price_push_threshold, n(63));
    assert!(fields.threshold_enabled);
    assert_eq!(fields.reserved_high_bits, (U256::ONE << 56) - U256::ONE);
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoldenFile {
    vectors: Vec<GoldenVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoldenVector {
    cash: String,
    router: String,
    asset_in: String,
    asset_out: String,
    mode: QuoteMode,
    amount: String,
    execution_block_number: String,
    state_version: String,
    blacklist_fee_multiplier: String,
    whitelisted: bool,
    partner_fee_bps: String,
    lane_in: Option<GoldenLane>,
    lane_out: Option<GoldenLane>,
    expected: Option<GoldenResult>,
    expected_public_amount: Option<String>,
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
        let router = Address::from_hex(&vector.router).unwrap();
        let asset_in = Address::from_hex(&vector.asset_in).unwrap();
        let asset_out = Address::from_hex(&vector.asset_out).unwrap();
        let mut state = QuoteState {
            cash,
            blacklist_fee_multiplier: golden_u256(&vector.blacklist_fee_multiplier),
            state_version: golden_u256(&vector.state_version).try_into().unwrap(),
            ..Default::default()
        };
        state.whitelist.insert(router, vector.whitelisted);
        let fee_asset = if vector.mode == QuoteMode::ExactIn {
            asset_out
        } else {
            asset_in
        };
        state
            .partner_fee_bps
            .insert((router, fee_asset), golden_u256(&vector.partner_fee_bps));
        for (asset, lane) in [(asset_in, vector.lane_in), (asset_out, vector.lane_out)] {
            if let Some(lane) = lane {
                let slot0 = encode_lane_slot0(&LaneSlot0 {
                    price: golden_u256(&lane.price),
                    ask_fee_bps: golden_u256(&lane.ask_fee_bps),
                    bid_fee_bps: golden_u256(&lane.bid_fee_bps),
                    latest_update_block: golden_u256(&lane.latest_update_block),
                    ..Default::default()
                })
                .unwrap();
                state.lanes.insert(
                    asset,
                    LaneState {
                        slot0,
                        exists: lane.exists,
                        paused: lane.paused,
                        block_delay: lane.block_delay.parse().unwrap(),
                        slippage_k_bps: lane.slippage_k_bps.parse().unwrap(),
                    },
                );
                state
                    .total_principal_amount
                    .insert(asset, golden_u256(&lane.principal));
            }
        }
        let request = QuoteRequest {
            router,
            asset_in,
            asset_out,
            amount: golden_u256(&vector.amount),
            mode: vector.mode,
        };
        let context = QuoteContext {
            cash,
            execution_block_number: golden_u256(&vector.execution_block_number),
            state_version: state.state_version,
        };
        let outcome = quote(&request, &context, &state).unwrap();
        if let Some(public_amount) = vector.expected_public_amount {
            let actual = if vector.mode == QuoteMode::ExactIn {
                solidity_exact_in_amount(&outcome)
            } else {
                solidity_exact_out_amount_for_request(&request, &outcome)
            };
            assert_eq!(actual, golden_u256(&public_amount), "{}", vector.cash);
        } else {
            let expected = vector.expected.expect("full expected result");
            let QuoteOutcome::Available(actual) = outcome else {
                panic!("golden vector unexpectedly unavailable")
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
