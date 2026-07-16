import { assertU256, MathError, type Word } from "./constants.js";
import type { LaneSlot0 } from "./types.js";

function fieldMask(bits: bigint): bigint {
  return (1n << bits) - 1n;
}
function readField(word: bigint, shift: bigint, bits: bigint): bigint {
  return (word >> shift) & fieldMask(bits);
}
const SLOT0_PRICE_MASK = (1n << 112n) - 1n;
const SLOT0_FEE_MASK = (1n << 20n) - 1n;
const SLOT0_BLOCK_MASK = (1n << 40n) - 1n;
function validateField(value: bigint, bits: bigint, field: string): void {
  assertU256(value, field);
  if (value > fieldMask(bits))
    throw new MathError("FIELD_OVERFLOW", `${field} does not fit in ${bits} bits`, field, bits);
}

/** Decodes every packed field from the 256-bit lane storage word. */
export function decodeLaneSlot0(word: Word): LaneSlot0 {
  assertU256(word, "slot0");
  return {
    price: readField(word, 0n, 112n),
    askFeeBps: readField(word, 112n, 20n),
    bidFeeBps: readField(word, 132n, 20n),
    pricePushThreshold: readField(word, 152n, 7n),
    thresholdEnabled: readField(word, 159n, 1n) === 1n,
    latestUpdateBlock: readField(word, 160n, 40n),
    reservedHighBits: word >> 200n,
  };
}
/** Returns the packed 112-bit lane price. */
export const laneSlot0Price = (word: Word): bigint => assertU256(word, "slot0") & SLOT0_PRICE_MASK;
/** Returns the packed 20-bit ask fee in basis points. */
export const laneSlot0AskFeeBps = (word: Word): bigint => (assertU256(word, "slot0") >> 112n) & SLOT0_FEE_MASK;
/** Returns the packed 20-bit bid fee in basis points. */
export const laneSlot0BidFeeBps = (word: Word): bigint => (assertU256(word, "slot0") >> 132n) & SLOT0_FEE_MASK;
/** Returns the packed 40-bit latest-update block number. */
export const laneSlot0LatestUpdateBlock = (word: Word): bigint =>
  (assertU256(word, "slot0") >> 160n) & SLOT0_BLOCK_MASK;
/** Encodes a validated lane slot into the canonical bit layout. */
export function encodeLaneSlot0(fields: LaneSlot0): Word {
  validateField(fields.price, 112n, "price");
  validateField(fields.askFeeBps, 20n, "askFeeBps");
  validateField(fields.bidFeeBps, 20n, "bidFeeBps");
  validateField(fields.pricePushThreshold, 7n, "pricePushThreshold");
  validateField(fields.latestUpdateBlock, 40n, "latestUpdateBlock");
  validateField(fields.reservedHighBits, 56n, "reservedHighBits");
  let word =
    fields.price | (fields.askFeeBps << 112n) | (fields.bidFeeBps << 132n) | (fields.pricePushThreshold << 152n);
  if (fields.thresholdEnabled) word |= 1n << 159n;
  word |= fields.latestUpdateBlock << 160n;
  word |= fields.reservedHighBits << 200n;
  return assertU256(word, "slot0");
}
/** Packs ask and bid fees into the 40-bit update-fee payload. */
export function encodeUpdateFees(askFeeBps: bigint, bidFeeBps: bigint): bigint {
  validateField(askFeeBps, 20n, "askFeeBps");
  validateField(bidFeeBps, 20n, "bidFeeBps");
  return askFeeBps | (bidFeeBps << 20n);
}
/** Decodes the packed update-fee payload as `[askFeeBps, bidFeeBps]`. */
export function decodeUpdateFees(fees: bigint): readonly [bigint, bigint] {
  validateField(fees, 40n, "fees");
  return [fees & fieldMask(20n), fees >> 20n];
}
/** Applies a price/fee/block update while preserving unrelated slot fields. */
export function applyLaneUpdateSlot0(previous: Word, price: bigint, fees: bigint, blockNumber: bigint): Word {
  validateField(price, 112n, "price");
  validateField(fees, 40n, "fees");
  validateField(blockNumber, 40n, "blockNumber");
  const [askFeeBps, bidFeeBps] = decodeUpdateFees(fees);
  return encodeLaneSlot0({ ...decodeLaneSlot0(previous), price, askFeeBps, bidFeeBps, latestUpdateBlock: blockNumber });
}
