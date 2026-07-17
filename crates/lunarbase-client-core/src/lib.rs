//! Network-independent reducer and embeddable realtime client runtime.

mod bootstrap;
mod indexer;
mod model;
mod protocol;
mod source;
mod state;
mod transport;

pub use bootstrap::*;
pub use indexer::*;
pub use model::*;
pub use protocol::abi::*;
pub use source::*;
pub use state::*;
pub use transport::*;

#[cfg(test)]
mod tests;
