use super::client::SELECTOR_LANE;
use super::codec::{decode_words, keccak256, selector_address};
use lunarbase_math::Address;

#[test]
fn encodes_abi_address_arguments_as_padded_words() {
    let address = Address::from_hex("0x0000000000000000000000000000000000000001").unwrap();
    assert_eq!(
        selector_address(SELECTOR_LANE, address),
        "0xd1bacd10".to_owned() + &"0".repeat(63) + "1"
    );
}

#[test]
fn decodes_five_reserve_words_and_rejects_wrong_width() {
    let data = format!("0x{}", "00".repeat(32 * 5));
    assert_eq!(decode_words(&data, 5).unwrap().len(), 5);
    assert!(decode_words(&data, 4).is_err());
}

#[test]
fn hashes_runtime_code_with_keccak256() {
    assert_eq!(
        keccak256(b""),
        [
            0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7,
            0x03, 0xc0, 0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04,
            0x5d, 0x85, 0xa4, 0x70,
        ]
    );
}
