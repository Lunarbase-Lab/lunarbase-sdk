//! Stateful client facade. Implementation is split by responsibility:
//! normalized models, ABI decoding, source adapters, bootstrap/recovery,
//! ordered reducer, binary codec, persistence, and high-level indexer.

mod bootstrap;
mod indexer;
mod model;
mod persistence;
mod protocol;
mod sources;
mod state;

pub use bootstrap::*;
pub use indexer::*;
pub use model::*;
pub use persistence::*;
pub use protocol::abi::*;
pub use protocol::codec::{decode_checkpoint, decode_update, encode_checkpoint, encode_update};
pub use sources::*;
pub use state::*;

#[cfg(test)]
mod tests;
