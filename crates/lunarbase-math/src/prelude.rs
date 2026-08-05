//! Convenient imports for applications embedding LunarBase quote math.
//!
//! The prelude keeps common quote-math types and functions in one import.

pub use crate::arithmetic::{BPS, WAD};
pub use crate::quote::{
    quote, quote_exact_in, quote_exact_out, solidity_exact_in_amount, solidity_exact_out_amount,
    solidity_exact_out_amount_for_request,
};
pub use crate::slot0::{LaneSlot0, decode_lane_slot0, encode_lane_slot0};
pub use crate::state::{
    FeeProfile, LaneState, QuoteError, QuoteMode, QuoteOutcome, QuoteRequest, QuoteResult,
    QuoteState, UnavailableReason,
};
pub use crate::types::{Address, B256, Bytes, MathError, U256, U512};
