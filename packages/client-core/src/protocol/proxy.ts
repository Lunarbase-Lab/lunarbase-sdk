/** ERC-1967 proxy constants and strict storage decoding. */
import type { Address } from "@lunarbase/math";
import * as Hex from "ox/Hex";

/** `bytes32(uint256(keccak256("eip1967.proxy.implementation")) - 1)`. */
export const ERC1967_IMPLEMENTATION_SLOT =
  "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc" as const;

/** Decodes a canonical right-aligned, non-zero ERC-1967 implementation. */
export function decodeImplementation(word: Hex.Hex): Address {
  if (Hex.size(word) !== 32) throw new RangeError("ERC-1967 implementation slot must be bytes32");
  if (Hex.toBigInt(Hex.slice(word, 0, 12)) !== 0n)
    throw new RangeError("ERC-1967 implementation slot has non-zero high padding");
  const implementation = Hex.slice(word, 12) as Address;
  if (Hex.toBigInt(implementation) === 0n) throw new RangeError("ERC-1967 implementation is zero");
  return implementation;
}
