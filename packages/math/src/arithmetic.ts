import { assertU256, MathError, U256_MAX } from "./constants.js";

/** Checked uint256 addition. Throws when the mathematical result exceeds 256 bits. */
export function checkedAdd(x: bigint, y: bigint): bigint {
  assertU256(x, "x");
  assertU256(y, "y");
  return assertU256(x + y, "addition");
}
/** Checked uint256 subtraction. Throws on underflow instead of wrapping. */
export function checkedSub(x: bigint, y: bigint): bigint {
  assertU256(x, "x");
  assertU256(y, "y");
  if (x < y) throw new MathError("OVERFLOW", "uint256 subtraction underflow");
  return x - y;
}
/** Checked uint256 multiplication. Throws when the product exceeds 256 bits. */
export function checkedMul(x: bigint, y: bigint): bigint {
  assertU256(x, "x");
  assertU256(y, "y");
  return assertU256(x * y, "multiplication");
}
/** Rejects a zero denominator before a division operation is evaluated. */
export function ensureDenominator(denominator: bigint): void {
  assertU256(denominator, "denominator");
  if (denominator === 0n) throw new MathError("DIVISION_BY_ZERO", "division by zero");
}

/**
 * Computes floor(`x * y / denominator`) using an unbounded intermediate
 * product, matching Solidity's full-width mulDiv semantics.
 */
export function fullMulDivDown(x: bigint, y: bigint, denominator: bigint): bigint {
  assertU256(x, "x");
  assertU256(y, "y");
  assertU256(denominator, "denominator");
  ensureDenominator(denominator);
  return assertU256((x * y) / denominator, "fullMulDivDown result");
}
/** Computes ceil(`x * y / denominator`) with a full-width intermediate product. */
export function fullMulDivUp(x: bigint, y: bigint, denominator: bigint): bigint {
  assertU256(x, "x");
  assertU256(y, "y");
  assertU256(denominator, "denominator");
  ensureDenominator(denominator);
  const product = x * y;
  const quotient = product / denominator;
  const remainder = product % denominator;
  return assertU256(quotient + (remainder === 0n ? 0n : 1n), "fullMulDivUp result");
}
/** Computes floor(`x * y / denominator`) but rejects products outside uint256. */
export function mulDivDown256(x: bigint, y: bigint, denominator: bigint): bigint {
  assertU256(x, "x");
  assertU256(y, "y");
  assertU256(denominator, "denominator");
  ensureDenominator(denominator);
  return checkedMul(x, y) / denominator;
}
/** Computes integer ceiling division for non-negative bigint operands. */
export function ceilDiv(x: bigint, denominator: bigint): bigint {
  assertU256(x, "x");
  ensureDenominator(denominator);
  const quotient = x / denominator;
  return quotient + (x % denominator === 0n ? 0n : 1n);
}
export { U256_MAX };
