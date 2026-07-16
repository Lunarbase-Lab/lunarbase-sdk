use core::fmt;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

pub use ruint::aliases::U256;

#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
/// A canonical 20-byte EVM address.
pub struct Address(pub [u8; 20]);
impl Address {
    pub const ZERO: Self = Self([0; 20]);
    /// Parses an optional-`0x` 40-hex-character address.
    ///
    /// # Errors
    ///
    /// Returns [`MathError::InvalidAddress`] for the wrong length or any
    /// non-hexadecimal byte.
    pub fn from_hex(value: &str) -> Result<Self, MathError> {
        let value = value.strip_prefix("0x").unwrap_or(value);
        if value.len() != 40 {
            return Err(MathError::InvalidAddress);
        }
        let mut bytes = [0u8; 20];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                .map_err(|_| MathError::InvalidAddress)?;
        }
        Ok(Self(bytes))
    }
    /// Formats the address as a lowercase `0x`-prefixed hexadecimal string.
    pub fn to_hex(self) -> String {
        let mut result = String::with_capacity(42);
        result.push_str("0x");
        for byte in self.0 {
            result.push_str(&format!("{byte:02x}"));
        }
        result
    }
}
impl fmt::Display for Address {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}
impl FromStr for Address {
    type Err = MathError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

#[derive(Clone, Debug, Eq, thiserror::Error, PartialEq)]
/// Typed failure at a Solidity-compatible arithmetic or input-width boundary.
pub enum MathError {
    #[error("division by zero")]
    DivisionByZero,
    #[error("uint256 overflow")]
    Overflow,
    #[error("packed field {field} does not fit in {bits} bits")]
    FieldOverflow { field: &'static str, bits: u16 },
    #[error("invalid EVM address")]
    InvalidAddress,
}
