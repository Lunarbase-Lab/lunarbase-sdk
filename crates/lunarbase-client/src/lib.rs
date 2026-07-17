//! Compatibility facade for all Rust client packages.
//!
//! New applications may depend on the narrow network crate they deploy.
//! Existing users can keep importing `lunarbase-client`.

pub use lunarbase_client_arbitrum::*;
pub use lunarbase_client_base::*;
pub use lunarbase_client_core::*;
pub use lunarbase_client_monad::*;
