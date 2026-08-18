//! Generic EVM HTTP bootstrap, canonical recovery, and WebSocket ingestion.
//!
//! Includes standard Ethereum subscriptions and Base Flashblocks profiles.

pub mod fork;
pub mod prelude;
pub mod rpc;
pub mod ws;

#[cfg(test)]
mod fork_tests;
