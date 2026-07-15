use crate::{MathError, U256};

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

pub fn full_mul_div_down(x: U256, y: U256, denominator: U256) -> Result<U256, MathError> {
    if denominator == U256::ZERO {
        return Err(MathError::DivisionByZero);
    }
    let product = x.widening_mul::<256, 4, 512, 8>(y);
    let quotient = product / ruint::aliases::U512::from(denominator);
    U256::checked_from_limbs_slice(quotient.as_limbs()).ok_or(MathError::Overflow)
}
pub fn full_mul_div_up(x: U256, y: U256, denominator: U256) -> Result<U256, MathError> {
    if denominator == U256::ZERO {
        return Err(MathError::DivisionByZero);
    }
    let product = x.widening_mul::<256, 4, 512, 8>(y);
    let denominator_512 = ruint::aliases::U512::from(denominator);
    let (quotient, remainder) = product.div_rem(denominator_512);
    let quotient = if remainder != ruint::aliases::U512::ZERO {
        quotient
            .checked_add(ruint::aliases::U512::ONE)
            .ok_or(MathError::Overflow)?
    } else {
        quotient
    };
    U256::checked_from_limbs_slice(quotient.as_limbs()).ok_or(MathError::Overflow)
}
pub fn mul_div_down_256(x: U256, y: U256, denominator: U256) -> Result<U256, MathError> {
    if denominator == U256::ZERO {
        return Err(MathError::DivisionByZero);
    }
    Ok(checked_mul(x, y)? / denominator)
}
