/** Cancellation-safe deadlines for untrusted source operations. */
import { IndexerError } from "../model.js";

/** Bounds one promise and removes every timer/listener on all exit paths. */
export function withDeadline<T>(
  operation: string,
  milliseconds: number,
  signal: AbortSignal | undefined,
  start: () => Promise<T>,
  cancel?: () => void,
): Promise<T> {
  if (signal?.aborted) return Promise.reject(cancelled(operation));
  return new Promise((resolve, reject) => {
    let settled = false;
    const cancelOperation = () => {
      try {
        cancel?.();
      } catch {
        // A cleanup hook cannot override the stable timeout/cancellation error.
      }
    };
    const finish = (result: { value: T } | { error: unknown }) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      if ("error" in result) reject(result.error);
      else resolve(result.value);
    };
    const onAbort = () => {
      cancelOperation();
      finish({ error: cancelled(operation) });
    };
    const timer = setTimeout(() => {
      cancelOperation();
      finish({ error: new IndexerError("SOURCE", `${operation} exceeded its ${milliseconds} ms deadline`) });
    }, milliseconds);
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) {
      onAbort();
      return;
    }
    try {
      start().then(
        (value) => finish({ value }),
        (error: unknown) => finish({ error }),
      );
    } catch (error) {
      finish({ error });
    }
  });
}

/** Monotonic milliseconds suitable for process-local lifecycle deadlines. */
export function monotonicMilliseconds(): number {
  return performance.now();
}

function cancelled(operation: string): IndexerError {
  return new IndexerError("SOURCE", `${operation} cancelled`);
}
