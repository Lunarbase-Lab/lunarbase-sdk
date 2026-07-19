//! Pure LunarBase quote math.
//!
//! This crate has no RPC, Redis, filesystem, clock, async, or network
//! dependency. Callers provide an immutable quote snapshot and execution
//! context.

pub mod arithmetic;
pub mod fees;
pub mod prelude;
pub mod quote;
pub mod slot0;
pub mod state;
pub mod types;

#[cfg(test)]
mod tests;
