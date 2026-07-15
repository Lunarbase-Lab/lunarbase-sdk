use lunarbase_math::{
    encode_lane_slot0, quote, solidity_exact_in_amount, solidity_exact_out_amount_for_request,
    Address, LaneSlot0, LaneState, QuoteContext, QuoteMode, QuoteOutcome, QuoteRequest, QuoteState,
    U256,
};
use serde::Deserialize;
use std::fs;
use std::io::{self, Write};
use std::str::FromStr;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureFile {
    vectors: Vec<Vector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vector {
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
    lane_in: Option<LaneVector>,
    lane_out: Option<LaneVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LaneVector {
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

fn u256(value: &str) -> U256 {
    U256::from_str(value).unwrap_or_else(|_| panic!("invalid decimal U256 `{value}`"))
}

fn address(value: &str) -> Address {
    Address::from_hex(value).unwrap_or_else(|_| panic!("invalid address `{value}`"))
}

fn address_word(value: Address) -> U256 {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(&value.0);
    U256::from_be_bytes::<32>(bytes)
}

fn build_state(vector: &Vector) -> (QuoteState, QuoteRequest, QuoteContext) {
    let cash = address(&vector.cash);
    let router = address(&vector.router);
    let asset_in = address(&vector.asset_in);
    let asset_out = address(&vector.asset_out);
    let mut state = QuoteState {
        cash,
        blacklist_fee_multiplier: u256(&vector.blacklist_fee_multiplier),
        state_version: u256(&vector.state_version).try_into().unwrap(),
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
        .insert((router, fee_asset), u256(&vector.partner_fee_bps));
    for (asset, lane) in [(asset_in, &vector.lane_in), (asset_out, &vector.lane_out)] {
        let Some(lane) = lane else { continue };
        let slot0 = encode_lane_slot0(&LaneSlot0 {
            price: u256(&lane.price),
            ask_fee_bps: u256(&lane.ask_fee_bps),
            bid_fee_bps: u256(&lane.bid_fee_bps),
            latest_update_block: u256(&lane.latest_update_block),
            ..Default::default()
        })
        .expect("fixture lane fits slot0");
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
            .insert(asset, u256(&lane.principal));
    }
    let request = QuoteRequest {
        router,
        asset_in,
        asset_out,
        amount: u256(&vector.amount),
        mode: vector.mode,
    };
    let context = QuoteContext {
        cash,
        execution_block_number: u256(&vector.execution_block_number),
        state_version: state.state_version,
    };
    (state, request, context)
}

fn output_words(vector: &Vector) -> [U256; 7] {
    let (state, request, context) = build_state(vector);
    let outcome = quote(&request, &context, &state).expect("oracle arithmetic must not fail");
    match outcome {
        QuoteOutcome::Available(result) => [
            U256::ONE,
            result.amount_in,
            result.amount_out,
            address_word(result.fee_asset),
            result.fee_amount,
            result.partner_fee,
            result.treasury_fee,
        ],
        unavailable => [
            U256::ZERO,
            solidity_exact_in_amount(&unavailable),
            solidity_exact_out_amount_for_request(&request, &unavailable),
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
        ],
    }
}

fn write_words(words: [U256; 7]) -> io::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout.write_all(b"hex:")?;
    for word in words {
        for byte in word.to_be_bytes::<32>() {
            write!(stdout, "{byte:02x}")?;
        }
    }
    stdout.flush()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let file = args
        .windows(2)
        .find(|pair| pair[0] == "--file")
        .map(|pair| pair[1].clone())
        .ok_or("missing --file")?;
    let index: usize = args
        .windows(2)
        .find(|pair| pair[0] == "--index")
        .ok_or("missing --index")?[1]
        .parse()?;
    let fixture: FixtureFile = serde_json::from_str(&fs::read_to_string(file)?)?;
    let vector = fixture
        .vectors
        .get(index)
        .ok_or("vector index out of range")?;
    write_words(output_words(vector))?;
    Ok(())
}
