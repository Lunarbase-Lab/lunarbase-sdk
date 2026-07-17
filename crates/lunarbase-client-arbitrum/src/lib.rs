//! Arbitrum Nitro client.
//!
//! This crate consumes executed Nitro state and preserves the EVM-visible
//! parent-chain block context needed by delayed quote predicates.

mod normalizer;
mod transport;

pub use normalizer::*;
pub use transport::*;

use lunarbase_client_core::{Network, NetworkSource, NormalizedBackend};
use std::sync::Arc;

/// Runtime-facing Arbitrum source backed by an executed Nitro transport.
pub type ArbitrumNitroSource<B> = NetworkSource<B>;

/// Wraps a Nitro backend with the common runtime source interface.
pub fn make_arbitrum_source<B: NormalizedBackend + 'static>(
    backend: Arc<B>,
) -> ArbitrumNitroSource<B> {
    NetworkSource::new(Network::Arbitrum, backend)
}
