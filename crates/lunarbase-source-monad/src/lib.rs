//! Monad execution-events data sources.
//!
//! Parser WebSocket and native shared-memory readers live in this network
//! package; the common reducer has no Monad-specific dependency.

pub mod execution;
pub mod parser;
pub mod prelude;
pub mod protocol;

#[cfg(all(feature = "native-event-ring", target_os = "linux"))]
pub mod native;
