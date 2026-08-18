import { RpcError } from "./error.js";

const TEXT_ENCODER = new TextEncoder();

/** Strict JSON-RPC and canonical backfill memory limits. */
export interface HttpRpcLimits {
  readonly maxRequestBytes: number;
  readonly maxResponseBytes: number;
  readonly maxBackfillPageBlocks: bigint;
  readonly maxBackfillLogs: number;
  readonly maxBackfillBytes: number;
}

/** Production-safe defaults shared by the source and callers overriding one limit. */
export const DEFAULT_HTTP_RPC_LIMITS: HttpRpcLimits = Object.freeze({
  maxRequestBytes: 1024 * 1024,
  maxResponseBytes: 8 * 1024 * 1024,
  maxBackfillPageBlocks: 1_000n,
  maxBackfillLogs: 16_384,
  maxBackfillBytes: 32 * 1024 * 1024,
});

export function validateHttpLimits(limits: HttpRpcLimits): HttpRpcLimits {
  for (const [name, value] of Object.entries({
    maxRequestBytes: limits.maxRequestBytes,
    maxResponseBytes: limits.maxResponseBytes,
    maxBackfillLogs: limits.maxBackfillLogs,
    maxBackfillBytes: limits.maxBackfillBytes,
  }))
    if (!Number.isSafeInteger(value) || value <= 0) throw new RpcError("INVALID", `${name} must be positive`);
  if (limits.maxBackfillPageBlocks <= 0n) throw new RpcError("INVALID", "maxBackfillPageBlocks must be positive");
  return Object.freeze(limits);
}

/** Wraps fetch with count-independent request and streaming response byte caps. */
export function boundedFetcher(fetcher: typeof fetch, limits: HttpRpcLimits): typeof fetch {
  return (async (input: RequestInfo | URL, init?: RequestInit) => {
    const requestBytes = await requestBodyBytes(input, init);
    if (requestBytes > limits.maxRequestBytes)
      throw new RpcError("LIMIT", `JSON-RPC request body exceeds ${limits.maxRequestBytes} bytes`);
    const response = await fetcher(input, init);
    const contentLength = response.headers.get("content-length");
    if (contentLength !== null && parseContentLength(contentLength) > limits.maxResponseBytes)
      throw new RpcError("LIMIT", `HTTP response content-length exceeds ${limits.maxResponseBytes} bytes`);
    if (response.body === null) return response;
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength > limits.maxResponseBytes - total) {
        await reader.cancel();
        throw new RpcError("LIMIT", `HTTP response body exceeds ${limits.maxResponseBytes} bytes`);
      }
      chunks.push(value);
      total += value.byteLength;
    }
    const body = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return new Response(body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  }) as typeof fetch;
}

async function requestBodyBytes(input: RequestInfo | URL, init?: RequestInit): Promise<number> {
  const body = init?.body;
  if (typeof body === "string") return TEXT_ENCODER.encode(body).byteLength;
  if (body instanceof URLSearchParams) return TEXT_ENCODER.encode(body.toString()).byteLength;
  if (body instanceof ArrayBuffer) return body.byteLength;
  if (ArrayBuffer.isView(body)) return body.byteLength;
  if (body instanceof Blob) return body.size;
  if (body !== undefined && body !== null)
    throw new RpcError("INVALID", "streaming JSON-RPC request bodies are unsupported");
  if (typeof Request !== "undefined" && input instanceof Request) return (await input.clone().arrayBuffer()).byteLength;
  return 0;
}

function parseContentLength(value: string): number {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) throw new RpcError("INVALID", "HTTP content-length is invalid");
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new RpcError("LIMIT", "HTTP content-length exceeds safe integer range");
  return parsed;
}
