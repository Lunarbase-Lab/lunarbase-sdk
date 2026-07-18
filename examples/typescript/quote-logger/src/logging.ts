import type { ClientBatchQuote } from "@lunarbase/client-core";
import type { QuoteRequest } from "@lunarbase/math";

/**
 * Emits every outcome from one atomic `quoteMany` result as structured JSON.
 *
 * Cursor metadata is retained alongside available quotes so operators can
 * identify the exact execution state from which a displayed quote was
 * calculated.
 */
export function logQuoteBatch(requests: readonly QuoteRequest[], batch: ClientBatchQuote): void {
  for (const [index, outcome] of batch.results.entries()) {
    const request = requests[index];
    if (request === undefined) throw new Error("quote result count exceeds request count");
    if (outcome.kind === "Available") {
      writeLog("info", "quote", {
        block: batch.executionBlockNumber,
        commitment: batch.cursor.commitment,
        assetIn: request.assetIn,
        assetOut: request.assetOut,
        ...outcome.result,
      });
    } else {
      writeLog("warn", "quote unavailable", {
        block: batch.executionBlockNumber,
        assetIn: request.assetIn,
        assetOut: request.assetOut,
        reason: outcome.reason,
      });
    }
  }
}

/**
 * Writes one newline-delimited JSON record and encodes bigint fields exactly.
 *
 * Decimal strings avoid lossy conversion through JavaScript `number` and keep
 * the terminal output safe for downstream log collectors.
 */
export function writeLog(
  level: "info" | "warn" | "error",
  message: string,
  fields: Readonly<Record<string, unknown>> = {},
): void {
  const payload = { timestamp: new Date().toISOString(), level, message, ...fields };
  console.log(JSON.stringify(payload, (_, value: unknown) => (typeof value === "bigint" ? value.toString() : value)));
}
