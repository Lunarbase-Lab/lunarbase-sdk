import { assertU256, MathError, U256_MAX } from "./constants.js";

export function checkedAdd(x: bigint, y: bigint): bigint { return assertU256(x + y, "addition"); }
export function checkedSub(x: bigint, y: bigint): bigint { if (x < y) throw new MathError("OVERFLOW", "uint256 subtraction underflow"); return x - y; }
export function checkedMul(x: bigint, y: bigint): bigint { return assertU256(x * y, "multiplication"); }
export function ensureDenominator(denominator: bigint): void { if (denominator === 0n) throw new MathError("DIVISION_BY_ZERO", "division by zero"); }

export function fullMulDivDown(x: bigint, y: bigint, denominator: bigint): bigint { assertU256(x, "x"); assertU256(y, "y"); assertU256(denominator, "denominator"); ensureDenominator(denominator); return assertU256((x * y) / denominator, "fullMulDivDown result"); }
export function fullMulDivUp(x: bigint, y: bigint, denominator: bigint): bigint { assertU256(x, "x"); assertU256(y, "y"); assertU256(denominator, "denominator"); ensureDenominator(denominator); const product = x * y; const quotient = product / denominator; const remainder = product % denominator; return assertU256(quotient + (remainder === 0n ? 0n : 1n), "fullMulDivUp result"); }
export function mulDivDown256(x: bigint, y: bigint, denominator: bigint): bigint { assertU256(x, "x"); assertU256(y, "y"); assertU256(denominator, "denominator"); ensureDenominator(denominator); return checkedMul(x, y) / denominator; }
export function ceilDiv(x: bigint, denominator: bigint): bigint { ensureDenominator(denominator); const quotient = x / denominator; return quotient + (x % denominator === 0n ? 0n : 1n); }
export { U256_MAX };
