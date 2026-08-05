export type LogLevel = "debug" | "info" | "warn" | "error";

function jsonValue(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? value.toString() : value;
}

/** Emits one secret-free structured log record. */
export function log(level: LogLevel, event: string, details: Readonly<Record<string, unknown>> = {}): void {
  process.stdout.write(
    `${JSON.stringify({ timestamp: new Date().toISOString(), level, event, ...details }, jsonValue)}\n`,
  );
}

/** Returns a bounded diagnostic message without serializing provider internals. */
export function errorMessage(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length <= 1_000 ? message : `${message.slice(0, 997)}...`;
}
