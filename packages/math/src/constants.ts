/** Unit scale used for wad-denominated price conversions. */
export const WAD = 1_000_000_000_000_000_000n;
/** Fee basis-point scale used by the Solidity math. */
export const BPS = 1_000_000n;
/** Scale converting slippage K-bps into ordinary fee bps. */
export const SLIPPAGE_SCALE = 10n;
/** Maximum representable slippage after K-bps conversion. */
export const MAX_SLIPPAGE_BPS = BPS / SLIPPAGE_SCALE;
/** Largest value representable by an unsigned 256-bit integer. */
export const U256_MAX = (1n << 256n) - 1n;
/** Largest value representable by an unsigned 128-bit integer. */
export const U128_MAX = (1n << 128n) - 1n;

export type { Address };
export type Word = bigint;

export class MathError extends Error {
  readonly code: "DIVISION_BY_ZERO" | "OVERFLOW" | "FIELD_OVERFLOW" | "INVALID_ADDRESS";
  readonly field?: string;
  readonly bits?: bigint;
  constructor(code: MathError["code"], message: string, field?: string, bits?: bigint) {
    super(message);
    this.name = "MathError";
    this.code = code;
    this.field = field;
    this.bits = bits;
  }
}

/** Validates and returns a bigint in the uint256 range. */
export function assertU256(value: bigint, label = "value"): bigint {
  if (typeof value !== "bigint" || value < 0n || value > U256_MAX)
    throw new MathError("OVERFLOW", `${label} is outside uint256`);
  return value;
}

/** Canonicalizes and validates a 20-byte EVM address. */
export function parseAddress(value: string): Address {
  try {
    return EvmAddress.from(value, { checksum: false });
  } catch {
    throw new MathError("INVALID_ADDRESS", "invalid EVM address");
  }
}
import * as EvmAddress from "ox/Address";
import type { Address } from "ox/Address";
