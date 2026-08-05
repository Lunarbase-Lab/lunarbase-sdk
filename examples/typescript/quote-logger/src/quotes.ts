import type { Address, QuoteRequest } from "@lunarbase-lab/pmm-v2-math";

/**
 * Builds an exact-input request in each direction for every active lane.
 *
 * The returned array is stable with respect to lane order, allowing quote
 * results from one `quoteMany` state snapshot to be paired by index.
 */
export function buildQuoteRequests(cash: Address, lanes: readonly Address[], amount: bigint): QuoteRequest[] {
  return lanes.flatMap((lane) => [
    { assetIn: lane, assetOut: cash, amount, mode: "ExactIn" },
    { assetIn: cash, assetOut: lane, amount, mode: "ExactIn" },
  ]);
}
