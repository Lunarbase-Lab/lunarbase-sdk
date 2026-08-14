//! Pure LunarBase quote math.
//!
//! This crate has no RPC, Redis, filesystem, clock, async, or network
//! dependency. Callers provide an immutable quote snapshot and execution
//! context.

/// Checked and full-width arithmetic primitives matching Solidity semantics.
pub mod arithmetic;
mod fees;
pub mod prelude;
mod quote;
/// Packing and decoding helpers for the protocol's `Lane.slot0` word.
pub mod slot0;
mod state;
mod types;

pub use arithmetic::{BPS, WAD};
pub use quote::{quote, solidity_quote_amount};
pub use slot0::{LaneSlot0, decode_lane_slot0, encode_lane_slot0};
pub use state::{
    FeeAllocation, FeeClass, LaneState, QuoteError, QuoteMode, QuoteOutcome, QuotePolicy,
    QuoteRequest, QuoteResult, QuoteState, UnavailableReason,
};
pub use types::{Address, B256, Bytes, MathError, U256};

#[cfg(test)]
mod fee_tests;
#[cfg(test)]
mod policy_tests;
#[cfg(test)]
mod sentinel_tests;
#[cfg(test)]
mod tests;
