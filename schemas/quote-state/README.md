# Quote state schema

Version 2 is the compatibility format used by Rust `encode_checkpoint` and
TypeScript `encodeCheckpoint`. The runtime representation is a compact binary
`LBQ1` payload; this directory describes its logical fields for migrations and
cross-language fixtures.

All integers that can carry chain or monetary values are decimal strings in
the logical JSON form. The binary form stores U256 values as exactly 32-byte
big-endian values, addresses as 20 bytes, and optional cursor fields with an
explicit presence byte. Map entries are sorted by lowercase address/key before
encoding, so identical state produces identical checkpoint bytes.
