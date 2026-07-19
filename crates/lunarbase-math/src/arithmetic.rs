use crate::types::{MathError, U256, U512};

pub const WAD: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
pub const BPS: U256 = U256::from_limbs([1_000_000, 0, 0, 0]);
pub const SLIPPAGE_SCALE: U256 = U256::from_limbs([10, 0, 0, 0]);
pub const MAX_SLIPPAGE_BPS: U256 = U256::from_limbs([100_000, 0, 0, 0]);
pub const U128_MAX: U256 = U256::from_limbs([u64::MAX, u64::MAX, 0, 0]);

#[inline(always)]
pub(crate) fn checked_add(x: U256, y: U256) -> Result<U256, MathError> {
    x.checked_add(y).ok_or(MathError::Overflow)
}
#[inline(always)]
pub(crate) fn checked_sub(x: U256, y: U256) -> Result<U256, MathError> {
    x.checked_sub(y).ok_or(MathError::Overflow)
}
#[inline(always)]
pub(crate) fn checked_mul(x: U256, y: U256) -> Result<U256, MathError> {
    x.checked_mul(y).ok_or(MathError::Overflow)
}
#[inline(always)]
pub(crate) fn ceil_div(x: U256, denominator: U256) -> Result<U256, MathError> {
    if denominator == U256::ZERO {
        return Err(MathError::DivisionByZero);
    }
    let (quotient, remainder) = x.div_rem(denominator);
    if remainder == U256::ZERO {
        Ok(quotient)
    } else {
        checked_add(quotient, U256::ONE)
    }
}

/// Computes `floor(x * y / denominator)` using a full 512-bit intermediate.
///
/// This is the Rust equivalent of Solady's `fullMulDiv` floor path. The
/// product is allowed to exceed 256 bits, but the final quotient must fit in
/// `U256`; this distinction is required by the Solidity quote formulas.
///
/// # Errors
///
/// Returns [`MathError::DivisionByZero`] when `denominator` is zero or
/// [`MathError::Overflow`] when the final quotient does not fit in 256 bits.
pub fn full_mul_div_down(x: U256, y: U256, denominator: U256) -> Result<U256, MathError> {
    if denominator == U256::ZERO {
        return Err(MathError::DivisionByZero);
    }
    let product = x.widening_mul::<256, 4, 512, 8>(y);
    let quotient = product / U512::from(denominator);
    U256::checked_from_limbs_slice(quotient.as_limbs()).ok_or(MathError::Overflow)
}
/// Computes `ceil(x * y / denominator)` using a full 512-bit intermediate.
///
/// Rounding happens only after the full product is divided. This preserves the
/// independent ceil operations used by weighted slippage and exact-out
/// quoting instead of accidentally rounding a narrower intermediate.
///
/// # Errors
///
/// Returns [`MathError::DivisionByZero`] for a zero denominator and
/// [`MathError::Overflow`] if either the rounded quotient or its final `U256`
/// representation overflows.
pub fn full_mul_div_up(x: U256, y: U256, denominator: U256) -> Result<U256, MathError> {
    if denominator == U256::ZERO {
        return Err(MathError::DivisionByZero);
    }
    let product = x.widening_mul::<256, 4, 512, 8>(y);
    let denominator_512 = U512::from(denominator);
    let (quotient, remainder) = product.div_rem(denominator_512);
    let quotient = if remainder != U512::ZERO {
        quotient.checked_add(U512::ONE).ok_or(MathError::Overflow)?
    } else {
        quotient
    };
    U256::checked_from_limbs_slice(quotient.as_limbs()).ok_or(MathError::Overflow)
}
/// Computes `floor(x * y / denominator)` with Solidity's checked-256 product.
///
/// Unlike [`full_mul_div_down`], multiplication is performed in the 256-bit
/// domain first. This is the primitive used by `splitFee` in the pinned
/// contract and must not be replaced with the full-width variant.
///
/// # Errors
///
/// Returns [`MathError::DivisionByZero`] for a zero denominator or
/// [`MathError::Overflow`] when `x * y` exceeds `U256`.
pub fn mul_div_down_256(x: U256, y: U256, denominator: U256) -> Result<U256, MathError> {
    if denominator == U256::ZERO {
        return Err(MathError::DivisionByZero);
    }
    Ok(checked_mul(x, y)? / denominator)
}
