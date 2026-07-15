export const WAD = 1_000_000_000_000_000_000n;
export const BPS = 1_000_000n;
export const SLIPPAGE_SCALE = 10n;
export const MAX_SLIPPAGE_BPS = BPS / SLIPPAGE_SCALE;
export const U256_MAX = (1n << 256n) - 1n;
export const U128_MAX = (1n << 128n) - 1n;

export type Address = string;
export type Word = bigint;

export class MathError extends Error {
  readonly code: "DIVISION_BY_ZERO" | "OVERFLOW" | "FIELD_OVERFLOW" | "INVALID_ADDRESS";
  readonly field?: string;
  readonly bits?: bigint;
  constructor(code: MathError["code"], message: string, field?: string, bits?: bigint) {
    super(message); this.name = "MathError"; this.code = code; this.field = field; this.bits = bits;
  }
}

export class QuoteError extends Error {
  readonly code: "CASH_MISMATCH" | "STATE_VERSION_MISMATCH" | "ARITHMETIC";
  constructor(code: QuoteError["code"], message: string, options?: { cause?: unknown }) { super(message, options); this.name = "QuoteError"; this.code = code; }
}

export function assertU256(value: bigint, label = "value"): bigint {
  if (typeof value !== "bigint" || value < 0n || value > U256_MAX) throw new MathError("OVERFLOW", `${label} is outside uint256`);
  return value;
}

export function parseAddress(value: string): Address {
  if (!/^0x[0-9a-fA-F]{40}$/.test(value)) throw new MathError("INVALID_ADDRESS", "invalid EVM address");
  return value.toLowerCase();
}
