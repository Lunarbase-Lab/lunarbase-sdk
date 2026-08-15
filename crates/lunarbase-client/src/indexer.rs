//! Embeddable client lifecycle and lock-free-read quote facade.

pub(crate) mod checkpoint_recovery;
pub mod client;
pub mod client_types;
pub mod engine;
pub mod errors;
pub(crate) mod event_delivery;
pub mod quote_types;
pub(crate) mod runtime_helpers;
mod shutdown;
pub(crate) mod tasks;
