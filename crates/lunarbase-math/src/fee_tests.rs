use crate::arithmetic::BPS;
use crate::fees::split_fee;
use crate::types::{MathError, U256};

fn n(value: u64) -> U256 {
    U256::from(value)
}

#[test]
fn fee_split_applies_partner_share_to_explicit_fee() {
    assert_eq!(
        split_fee(n(1_000_000), n(250_000)).unwrap(),
        (n(250_000), n(750_000))
    );
    assert_eq!(split_fee(n(1), n(500_000)).unwrap(), (n(0), n(1)));
    assert_eq!(split_fee(n(0), n(500_000)).unwrap(), (n(0), n(0)));
    assert_eq!(split_fee(n(1_000_000), BPS).unwrap(), (n(1_000_000), n(0)));
    assert_eq!(split_fee(U256::MAX, n(2)).unwrap_err(), MathError::Overflow);
}
