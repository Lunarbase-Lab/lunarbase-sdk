//! HTTP JSON-RPC bootstrap and canonical recovery implementation.

pub mod backend;
pub mod client;
pub mod codec;
mod http;
pub mod snapshot;

#[cfg(test)]
mod http_tests;
#[cfg(test)]
mod tests;
