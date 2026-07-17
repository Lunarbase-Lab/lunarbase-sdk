//! HTTP JSON-RPC bootstrap and canonical recovery implementation.

mod backend;
mod client;
mod codec;
mod snapshot;

pub use backend::RpcHttpBackend;
pub use client::{RpcError, RpcHttpClient};
pub use codec::parse_rpc_log;
pub use snapshot::RpcSnapshotProvider;

#[cfg(test)]
mod tests;
