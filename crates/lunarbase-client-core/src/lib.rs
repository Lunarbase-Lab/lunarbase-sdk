//! Network-independent client runtime. Implementation is split by responsibility:
//! normalized models, ABI decoding, source adapters, bootstrap/recovery,
//! ordered reducer, binary codec, persistence, and high-level indexer.

mod bootstrap;
mod execution;
mod indexer;
mod model;
mod persistence;
mod protocol;
mod source;
mod state;
mod transport;

pub use bootstrap::*;
pub use execution::*;
pub use indexer::*;
pub use model::*;
pub use persistence::*;
pub use protocol::abi::*;
pub use protocol::codec::{decode_checkpoint, decode_update, encode_checkpoint, encode_update};
pub use source::*;
pub use state::*;
pub use transport::*;

#[cfg(test)]
mod tests;
