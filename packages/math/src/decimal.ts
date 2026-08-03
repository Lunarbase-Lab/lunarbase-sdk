/** Rounding applied when a decimal does not fit the requested integer scale. */
export type DecimalRounding = "exact" | "down" | "up" | "nearest";

const DECIMAL_NUMBER = /^(\d+)(?:\.(\d+))?(?:e([+-]?\d+))?$/i;
const MAX_DECIMALS = 255;

interface DecimalParts {
  /** Decimal digits with the point removed. */
  coefficient: bigint;
  /** Base-ten exponent applied to `coefficient`. */
  exponent: number;
}

/**
 * Converts a non-negative JavaScript number into a scaled integer without
 * performing floating-point multiplication.
 *
 * The input is interpreted through JavaScript's shortest round-trippable
 * decimal representation (`value.toString()`). For example,
 * `decimalNumberToBigInt(2.824467842, 20)` returns
 * `282446784200000000000n`, while `BigInt(2.824467842 * 1e20)` first incurs a
 * binary64 multiplication error.
 *
 * This function preserves every decimal digit present in the `number`; it
 * cannot recover digits already lost before the value entered JavaScript.
 * Prefer an upstream decimal string when the producer can provide one.
 *
 * @param value Non-negative finite number produced by the external model.
 * @param decimals Number of base-ten fractional digits in the target integer.
 * @param rounding Behavior when the decimal has more precision than the
 * target scale. `exact` rejects discarded digits, `down` truncates, `up`
 * rounds away from zero, and `nearest` uses half-up rounding.
 */
export function decimalNumberToBigInt(value: number, decimals: number, rounding: DecimalRounding = "exact"): bigint {
  if (!Number.isFinite(value) || value < 0) throw new RangeError("value must be a non-negative finite number");
  if (!Number.isSafeInteger(decimals) || decimals < 0 || decimals > MAX_DECIMALS)
    throw new RangeError(`decimals must be an integer between 0 and ${MAX_DECIMALS}`);

  const parts = parseDecimalNumber(value);
  const scaledExponent = parts.exponent + decimals;
  if (scaledExponent >= 0) return parts.coefficient * 10n ** BigInt(scaledExponent);

  const divisor = 10n ** BigInt(-scaledExponent);
  const quotient = parts.coefficient / divisor;
  const remainder = parts.coefficient % divisor;
  if (remainder === 0n) return quotient;

  switch (rounding) {
    case "exact":
      throw new RangeError(`value is not exactly representable with ${decimals} decimals`);
    case "down":
      return quotient;
    case "up":
      return quotient + 1n;
    case "nearest":
      return remainder * 2n >= divisor ? quotient + 1n : quotient;
  }
  throw new RangeError(`unsupported decimal rounding mode: ${String(rounding)}`);
}

function parseDecimalNumber(value: number): DecimalParts {
  const decimal = value.toString();
  const match = DECIMAL_NUMBER.exec(decimal);
  if (!match) throw new RangeError(`number has an unsupported decimal representation: ${decimal}`);

  const integer = match[1] ?? "0";
  const fraction = match[2] ?? "";
  const scientificExponent = Number(match[3] ?? "0");
  const digits = `${integer}${fraction}`.replace(/^0+/, "") || "0";
  return {
    coefficient: BigInt(digits),
    exponent: scientificExponent - fraction.length,
  };
}
