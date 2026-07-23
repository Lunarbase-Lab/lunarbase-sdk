//! ERC-1967 proxy constants and strict storage decoding.

use lunarbase_math::types::{Address, B256};

/// `bytes32(uint256(keccak256("eip1967.proxy.implementation")) - 1)`.
pub const ERC1967_IMPLEMENTATION_SLOT: B256 = B256::new([
    0x36, 0x08, 0x94, 0xa1, 0x3b, 0xa1, 0xa3, 0x21, 0x06, 0x67, 0xc8, 0x28, 0x49, 0x2d, 0xb9, 0x8d,
    0xca, 0x3e, 0x20, 0x76, 0xcc, 0x37, 0x35, 0xa9, 0x20, 0xa3, 0xca, 0x50, 0x5d, 0x38, 0x2b, 0xbc,
]);

/// Decodes the canonical right-aligned address stored in the implementation slot.
///
/// Non-zero high padding and the zero address are rejected so a malformed
/// provider response cannot silently select a different byte range.
pub fn decode_implementation(word: B256) -> Option<Address> {
    let bytes = word.as_slice();
    if bytes[..12].iter().any(|byte| *byte != 0) {
        return None;
    }
    let implementation = Address::from_slice(&bytes[12..]);
    (implementation != Address::ZERO).then_some(implementation)
}

#[cfg(test)]
mod tests {
    use super::decode_implementation;
    use lunarbase_math::types::{Address, B256};

    #[test]
    fn decodes_only_canonical_non_zero_addresses() {
        let address = Address::new([0x11; 20]);
        let mut word = [0_u8; 32];
        word[12..].copy_from_slice(address.as_slice());
        assert_eq!(decode_implementation(B256::new(word)), Some(address));

        word[0] = 1;
        assert_eq!(decode_implementation(B256::new(word)), None);
        assert_eq!(decode_implementation(B256::ZERO), None);
    }
}
