//! Generic EVM HTTP bootstrap, canonical recovery, and WebSocket ingestion.
//!
//! Concrete network I/O lives here while [`lunarbase_client`] remains
//! transport-independent. Standard Ethereum subscriptions and Base
//! Flashblocks are profiles of the same source instead of separate clients.

pub mod prelude;
pub mod rpc;
pub mod ws;
