//! Network-independent reducer and embeddable realtime client runtime.

pub mod bootstrap;
pub mod indexer;
pub mod model;
pub mod prelude;
pub mod protocol;
pub mod source;
pub mod state;
pub mod transport;

#[cfg(test)]
mod tests;
