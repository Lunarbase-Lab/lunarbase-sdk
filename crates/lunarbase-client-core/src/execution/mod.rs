//! Execution-event runtime abstractions.
//!
//! Readers own deployment-specific I/O. Engines own ordering and conversion
//! into normalized [`crate::ChainUpdate`] values consumed by the runtime.

mod engine;

#[cfg(feature = "monad")]
mod monad;

pub use engine::*;

#[cfg(feature = "monad")]
pub use monad::*;
