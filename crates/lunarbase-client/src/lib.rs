//! Stateful client facade. Implementation is split by responsibility:
//! normalized models, ABI decoding, source adapters, bootstrap/recovery,
//! ordered reducer, binary codec, persistence, and high-level indexer.

mod abi;
mod bootstrap;
mod codec;
mod flashblocks;
mod indexer;
mod model;
mod nitro;
mod ordering;
mod persistence;
mod reducer;
mod rpc;
mod sources;
mod ws;

pub use abi::*;
pub use bootstrap::*;
pub use codec::{decode_checkpoint, decode_update, encode_checkpoint, encode_update};
pub use flashblocks::*;
pub use indexer::*;
pub use model::*;
pub use nitro::*;
pub use ordering::*;
pub use persistence::*;
pub use reducer::*;
pub use rpc::*;
pub use sources::*;
pub use ws::*;

#[cfg(test)]
mod tests;
