//! Wire-facing protocol helpers.
//!
//! This context contains event ABI decoding and the stable checkpoint/update
//! binary codec. Both are deliberately kept separate from the reducer and
//! source transports so a storage or transport change cannot silently change
//! the representation consumed by the state machine.

pub mod abi;
pub(crate) mod codec;
