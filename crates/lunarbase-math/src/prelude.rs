//! Convenient imports for applications embedding LunarBase quote math.
//!
//! The prelude keeps common quote-math types and functions in one import.

pub use crate::{
    Address, BPS, FeeProfile, LaneSlot0, LaneState, MathError, QuoteError, QuoteMode, QuoteOutcome,
    QuoteRequest, QuoteResult, QuoteState, U256, UnavailableReason, WAD, decode_lane_slot0,
    encode_lane_slot0, quote, solidity_quote_amount,
};
