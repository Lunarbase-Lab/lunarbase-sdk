//! Canonical EVM primitives and typed math failures.

/// Canonical EVM value types shared with Alloy, Foundry, Reth, and Revm.
///
/// Re-exporting these primitives pins one concrete family of EVM types across
/// LunarBase's public Rust API.
pub use alloy_primitives::{Address, B256, Bytes, U256, U512};

#[derive(Clone, Debug, Eq, thiserror::Error, PartialEq)]
/// Typed failure at a Solidity-compatible arithmetic or input-width boundary.
pub enum MathError {
    /// A denominator was zero at a Solidity-reverting division boundary.
    #[error("division by zero")]
    DivisionByZero,
    /// A checked 256-bit operation or narrowing conversion overflowed.
    #[error("uint256 overflow")]
    Overflow,
    /// A native value did not fit the declared Solidity packed-field width.
    #[error("packed field {field} does not fit in {bits} bits")]
    FieldOverflow {
        /// Stable field name used to identify the invalid input.
        field: &'static str,
        /// Maximum width of the packed destination field.
        bits: u16,
    },
}
