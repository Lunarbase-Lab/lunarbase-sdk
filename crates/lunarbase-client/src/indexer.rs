//! Embeddable client lifecycle and lock-free-read quote facade.

pub mod client;
pub mod client_types;
pub mod engine;
pub mod errors;
pub mod quote_types;
pub(crate) mod tasks;
