//! Base Flashblocks client.
//!
//! This crate contains only Base-specific normalization and transport code.
//! Stateful runtime, recovery, persistence, and quoting live in
//! `lunarbase-client-core`.

mod normalizer;
mod transport;

pub use normalizer::*;
pub use transport::*;

use lunarbase_client_core::{Network, NetworkSource, NormalizedBackend};
use std::sync::Arc;

/// Runtime-facing Base source backed by a Flashblocks transport.
pub type BaseFlashblocksSource<B> = NetworkSource<B>;

/// Wraps a Base backend with the common runtime source interface.
pub fn make_base_source<B: NormalizedBackend + 'static>(
    backend: Arc<B>,
) -> BaseFlashblocksSource<B> {
    NetworkSource::new(Network::Base, backend)
}
