//! Core ordered state-transition machinery.
//!
//! `ordering` handles transport reordering and watermark release;
//! `reducer` applies canonical quote events to the in-memory state.

pub(crate) mod ordering;
pub(crate) mod reducer;

pub use ordering::*;
pub use reducer::*;
