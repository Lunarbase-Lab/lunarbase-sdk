import { assertU256, MathError, type Word } from "./constants.js";
import { decimalNumberToBigInt, type DecimalRounding } from "./decimal.js";
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
const SLOT0_SLIPPAGE_K_MASK = (1n << 32n) - 1n;

/** Decimal model fields required to construct one lane price update. */
export interface LaneModelQuoteNumbers {
  /** CASH per one lane asset; use the model's `S` anchor price. */
  anchorPrice: number;
  /** Ask spread expressed in conventional basis points. */
  askSpreadBps: number;
  /** Bid spread expressed in conventional basis points. */
  bidSpreadBps: number;
  /** Decimal precision of the Core settlement asset. */
  cashDecimals: number;
  /** Decimal precision of the lane asset. */
  assetDecimals: number;
}

/** Integer fields from a model quote that are stored in `Lane.slot0`. */
export interface LaneSlot0QuoteFields {
  /** Decimal-adjusted WAD anchor price ready for the low `uint112`. */
  price: bigint;
  /** Ask spread in LunarBase protocol BPS, ready for `uint20`. */
  askFeeBps: bigint;
  /** Bid spread in LunarBase protocol BPS, ready for `uint20`. */
  bidFeeBps: bigint;
}

function validateField(value: bigint, bits: bigint, field: string): void {
  assertU256(value, field);
  if (value > fieldMask(bits))
    throw new MathError("FIELD_OVERFLOW", `${field} does not fit in ${bits} bits`, field, bits);
}

/**
 * Converts a model anchor price into the exact fixed-point value stored in
 * slot0.
 *
 * Floating-point multiplication is deliberately avoided. The canonical
 * decimal representation of `price` is scaled by
 * `10 ** (cashDecimals - assetDecimals) * WAD`, matching `Lanes.sol`. The
 * scaled result must be exact and fit the contract's `uint112` price field.
 */
export function lanePriceFromNumber(price: number, cashDecimals: number, assetDecimals: number): bigint {
  validateTokenDecimals(cashDecimals, "cashDecimals");
  validateTokenDecimals(assetDecimals, "assetDecimals");
  const scaleDecimals = 18 + cashDecimals - assetDecimals;
  if (scaleDecimals < 0)
    throw new RangeError("cashDecimals - assetDecimals produces a sub-integer LaneSlot0 price scale");
  const encoded = decimalNumberToBigInt(price, scaleDecimals, "exact");
  validateField(encoded, 112n, "price");
  return encoded;
}

/**
 * Converts conventional basis points into LunarBase protocol BPS.
 *
 * The protocol denominator is `1_000_000`, so one conventional basis point is
 * exactly 100 protocol units. A model spread can contain more than two
 * fractional bps digits; `rounding` determines how those excess digits map to
 * the integer `uint20` field.
 */
export function laneFeeBpsFromConventionalBps(spreadBps: number, rounding: DecimalRounding = "nearest"): bigint {
  const encoded = decimalNumberToBigInt(spreadBps, 2, rounding);
  validateField(encoded, 20n, "feeBps");
  return encoded;
}

/**
 * Decodes the quote-critical decimal fields produced by the pricing model.
 *
 * Pass model `S` as `anchorPrice` and the directional spreads as
 * `spreadAskBps` and `spreadBidBps`. Inputs must use conventional basis
 * points; Q24-encoded fee values are not accepted.
 */
export function modelQuoteToLaneSlot0Fields(
  quote: LaneModelQuoteNumbers,
  feeRounding: DecimalRounding = "nearest",
): LaneSlot0QuoteFields {
  return {
    price: lanePriceFromNumber(quote.anchorPrice, quote.cashDecimals, quote.assetDecimals),
    askFeeBps: laneFeeBpsFromConventionalBps(quote.askSpreadBps, feeRounding),
    bidFeeBps: laneFeeBpsFromConventionalBps(quote.bidSpreadBps, feeRounding),
  };
}

function validateTokenDecimals(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > 0xff)
    throw new RangeError(`${field} must fit an ERC-20 uint8`);
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
    exists: readField(word, 200n, 1n) === 1n,
    paused: readField(word, 201n, 1n) === 1n,
    blockDelay: Number(readField(word, 202n, 8n)),
    slippageKBps: Number(readField(word, 210n, 32n)),
    reservedHighBits: word >> 242n,
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
/** Returns the packed lane existence bit. */
export const laneSlot0Exists = (word: Word): boolean => ((assertU256(word, "slot0") >> 200n) & 1n) === 1n;
/** Returns the packed lane pause bit. */
export const laneSlot0Paused = (word: Word): boolean => ((assertU256(word, "slot0") >> 201n) & 1n) === 1n;
/** Returns the packed inclusive quote TTL in execution blocks. */
export const laneSlot0BlockDelay = (word: Word): number => Number((assertU256(word, "slot0") >> 202n) & 0xffn);
/** Returns the packed lane slippage coefficient. */
export const laneSlot0SlippageKBps = (word: Word): number =>
  Number((assertU256(word, "slot0") >> 210n) & SLOT0_SLIPPAGE_K_MASK);
function replaceField(word: Word, value: bigint, shift: bigint, bits: bigint): Word {
  const mask = fieldMask(bits) << shift;
  return assertU256((word & ~mask) | ((value & fieldMask(bits)) << shift), "slot0");
}

/** Replaces the packed existence bit. */
export const setLaneSlot0Exists = (word: Word, exists: boolean): Word => replaceField(word, exists ? 1n : 0n, 200n, 1n);
/** Replaces the packed lane pause bit. */
export const setLaneSlot0Paused = (word: Word, paused: boolean): Word => replaceField(word, paused ? 1n : 0n, 201n, 1n);
/** Replaces the packed price-push threshold and its enable bit. */
export function setLaneSlot0PricePushThreshold(word: Word, pricePushThreshold: number, enabled: boolean): Word {
  if (!Number.isSafeInteger(pricePushThreshold) || pricePushThreshold < 0 || pricePushThreshold > 0x7f)
    throw new RangeError("pricePushThreshold does not fit uint7");
  const updated = replaceField(word, BigInt(pricePushThreshold), 152n, 7n);
  return replaceField(updated, enabled ? 1n : 0n, 159n, 1n);
}
/** Replaces the packed block delay. */
export const setLaneSlot0BlockDelay = (word: Word, blockDelay: number): Word => {
  if (!Number.isSafeInteger(blockDelay) || blockDelay < 0 || blockDelay > 0xff)
    throw new RangeError("blockDelay does not fit uint8");
  return replaceField(word, BigInt(blockDelay), 202n, 8n);
};
/** Replaces the packed slippage coefficient. */
export const setLaneSlot0SlippageKBps = (word: Word, slippageKBps: number): Word => {
  if (!Number.isSafeInteger(slippageKBps) || slippageKBps < 0 || slippageKBps > 0xffff_ffff)
    throw new RangeError("slippageKBps does not fit uint32");
  return replaceField(word, BigInt(slippageKBps), 210n, 32n);
};
/** Encodes a validated lane slot into the canonical bit layout. */
export function encodeLaneSlot0(fields: LaneSlot0): Word {
  validateField(fields.price, 112n, "price");
  validateField(fields.askFeeBps, 20n, "askFeeBps");
  validateField(fields.bidFeeBps, 20n, "bidFeeBps");
  validateField(fields.pricePushThreshold, 7n, "pricePushThreshold");
  validateField(fields.latestUpdateBlock, 40n, "latestUpdateBlock");
  if (!Number.isSafeInteger(fields.blockDelay) || fields.blockDelay < 0 || fields.blockDelay > 0xff)
    throw new RangeError("blockDelay does not fit uint8");
  if (!Number.isSafeInteger(fields.slippageKBps) || fields.slippageKBps < 0 || fields.slippageKBps > 0xffff_ffff)
    throw new RangeError("slippageKBps does not fit uint32");
  validateField(fields.reservedHighBits, 14n, "reservedHighBits");
  let word =
    fields.price | (fields.askFeeBps << 112n) | (fields.bidFeeBps << 132n) | (fields.pricePushThreshold << 152n);
  if (fields.thresholdEnabled) word |= 1n << 159n;
  word |= fields.latestUpdateBlock << 160n;
  if (fields.exists) word |= 1n << 200n;
  if (fields.paused) word |= 1n << 201n;
  word |= BigInt(fields.blockDelay) << 202n;
  word |= BigInt(fields.slippageKBps) << 210n;
  word |= fields.reservedHighBits << 242n;
  return assertU256(word, "slot0");
}
/** Packs ask and bid fees into the 40-bit update-fee payload. */
export function encodeUpdateFees(askFeeBps: bigint, bidFeeBps: bigint): bigint {
  validateField(askFeeBps, 20n, "askFeeBps");
  validateField(bidFeeBps, 20n, "bidFeeBps");
  return askFeeBps | (bidFeeBps << 20n);
}
/** Decodes the packed update-fee payload as `[askFeeBps, bidFeeBps]`. */
function decodeUpdateFees(fees: bigint): readonly [bigint, bigint] {
  validateField(fees, 40n, "fees");
  return [fees & fieldMask(20n), fees >> 20n];
}
/** Applies a price/fee/block update while preserving unrelated slot fields. */
export function applyLaneUpdateSlot0(previous: Word, price: bigint, fees: bigint, blockNumber: bigint): Word {
  validateField(price, 112n, "price");
  validateField(fees, 40n, "fees");
  validateField(blockNumber, 40n, "blockNumber");
  const [askFeeBps, bidFeeBps] = decodeUpdateFees(fees);
  const fields = decodeLaneSlot0(previous);
  const delta = price >= fields.price ? price - fields.price : fields.price - price;
  const exceedsThreshold =
    fields.thresholdEnabled && fields.price !== 0n && delta * 100n > fields.price * fields.pricePushThreshold;
  return encodeLaneSlot0({
    ...fields,
    price,
    askFeeBps,
    bidFeeBps,
    latestUpdateBlock: blockNumber,
    paused: exceedsThreshold || fields.paused,
  });
}
