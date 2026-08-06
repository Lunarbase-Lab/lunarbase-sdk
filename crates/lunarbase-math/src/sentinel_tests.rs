use crate::quote::solidity_quote_amount;
use crate::state::{QuoteMode, QuoteOutcome, QuoteRequest, UnavailableReason};
use crate::types::{Address, U256};

fn request(mode: QuoteMode, amount: U256) -> QuoteRequest {
    QuoteRequest {
        asset_in: Address::ZERO,
        asset_out: Address::repeat_byte(1),
        amount,
        mode,
    }
}

#[test]
fn sentinels_match_solidity_edge_behavior() {
    let outcome = QuoteOutcome::Unavailable(UnavailableReason::MissingLane(Address::ZERO));
    assert_eq!(
        solidity_quote_amount(&request(QuoteMode::ExactIn, U256::ONE), &outcome),
        U256::ZERO
    );
    assert_eq!(
        solidity_quote_amount(&request(QuoteMode::ExactOut, U256::ONE), &outcome),
        U256::MAX
    );
    assert_eq!(
        solidity_quote_amount(&request(QuoteMode::ExactOut, U256::ZERO), &outcome),
        U256::ZERO
    );
}
