//! Embeddable client lifecycle and lock-free-read quote facade.

mod bootstrap_handoff;
pub(crate) mod checkpoint_recovery;
pub mod client;
pub mod client_types;
pub mod engine;
pub mod errors;
pub(crate) mod event_delivery;
#[cfg(feature = "perf-trace")]
pub mod perf_trace;
pub mod quote_types;
pub(crate) mod runtime_helpers;
mod shutdown;
pub(crate) mod tasks;
