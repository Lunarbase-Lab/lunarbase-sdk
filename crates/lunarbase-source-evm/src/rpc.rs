//! HTTP JSON-RPC bootstrap and canonical recovery implementation.

pub mod backend;
pub mod client;
pub mod codec;
pub mod snapshot;

#[cfg(test)]
mod tests;
