//! Pure LunarBase quote math.
//!
//! This crate has no RPC, Redis, filesystem, clock, async, or network
//! dependency. Callers provide an immutable quote snapshot and execution
//! context.

/// Checked and full-width arithmetic primitives matching Solidity semantics.
pub mod arithmetic;
/// Fee, spread, and slippage calculations for individual lanes and routes.
pub mod fees;
pub mod prelude;
/// High-level exact-in and exact-out quote evaluation.
pub mod quote;
/// Packing and decoding helpers for the protocol's `Lane.slot0` word.
pub mod slot0;
/// Quote requests, outcomes, and compact in-memory protocol state.
pub mod state;
/// Canonical Alloy EVM primitives and math errors used by this crate.
pub mod types;

#[cfg(test)]
mod tests;
