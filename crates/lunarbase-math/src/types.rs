/// Canonical EVM value types shared with Alloy, Foundry, Reth, and Revm.
///
/// Re-exporting these primitives pins one concrete family of EVM types across
/// LunarBase's public Rust API.
pub use alloy_primitives::{Address, B256, Bytes, U256, U512};

#[derive(Clone, Debug, Eq, thiserror::Error, PartialEq)]
/// Typed failure at a Solidity-compatible arithmetic or input-width boundary.
pub enum MathError {
    #[error("division by zero")]
    DivisionByZero,
    #[error("uint256 overflow")]
    Overflow,
    #[error("packed field {field} does not fit in {bits} bits")]
    FieldOverflow { field: &'static str, bits: u16 },
}
