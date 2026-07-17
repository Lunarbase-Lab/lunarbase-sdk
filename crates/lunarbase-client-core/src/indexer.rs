//! Embeddable client lifecycle and lock-free-read quote facade.

mod client;
mod client_types;
mod engine;
mod errors;
mod quote_types;
mod tasks;

pub use client_types::{ClientConnectConfig, ClientRuntimeStatsSnapshot};
pub use engine::QuoteIndexer;
pub use errors::{ClientRuntimeEvent, IndexerError};
pub use quote_types::{ClientBatchQuote, ClientQuote, IndexerHealth};

pub use client::ConnectedQuoteClient;
