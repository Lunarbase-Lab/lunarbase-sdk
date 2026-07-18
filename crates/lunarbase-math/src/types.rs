/// Canonical EVM value types shared with Alloy, Foundry, Reth, and Revm.
///
/// Re-exporting the primitives from the math crate keeps downstream LunarBase
/// APIs on one concrete type while avoiding a second address/hash
/// implementation.
pub use alloy_primitives::{Address, Bytes, B256, U256, U512};

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
