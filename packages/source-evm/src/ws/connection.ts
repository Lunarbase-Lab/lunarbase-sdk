/** Browser-compatible socket lifecycle and bounded frame queue. */
import { BoundedRingBuffer, type ContractFilter } from "@lunarbase-lab/pmm-v2-client";
import { parseHexU64, RpcError } from "../rpc.js";
import { subscriptionRequest } from "./protocol.js";

const HANDSHAKE_TIMEOUT_MILLISECONDS = 10_000;

export interface WebSocketLike {
  readonly readyState?: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open" | "message" | "error" | "close", listener: (event: SocketEvent) => void): void;
  removeEventListener?(type: "open" | "message" | "error" | "close", listener: (event: SocketEvent) => void): void;
}

export type SocketEvent = { data?: unknown; error?: unknown; reason?: string };
export type WebSocketFactory = (url: string) => WebSocketLike;

export interface EstablishedSocket {
  readonly socket: WebSocketLike;
  readonly queue: BoundedFrameQueue;
  readonly logsSubscription: string;
  readonly headsSubscription: string;
  readonly prefetched: BoundedRingBuffer<string>;
  readonly close: () => void;
}

export async function establishSocket(
  url: string,
  factory: WebSocketFactory,
  filter: ContractFilter,
  logsKind: "logs" | "pendingLogs",
  expectedChainId: bigint,
  queueCapacity: number,
  queueByteCapacity: number,
  prefetchByteCapacity: number,
  maxFrameBytes: number,
  signal?: AbortSignal,
): Promise<EstablishedSocket> {
  const socket = factory(url);
  const queue = new BoundedFrameQueue(queueCapacity, queueByteCapacity);
  const onOpen = () => queue.open();
  const onMessage = (event: SocketEvent) => {
    const frame = decodeFrame(event.data);
    if (frame === undefined) queue.fail(new Error("RPC WebSocket delivered a non-text frame"));
    else if (utf8Bytes(frame) > maxFrameBytes) queue.fail(new Error("RPC WebSocket frame exceeded configured bound"));
    else queue.push(frame);
  };
  const onError = (event: SocketEvent) =>
    queue.fail(event.error instanceof Error ? event.error : new Error("RPC WebSocket error"));
  const onClose = (event: SocketEvent) => (event.reason ? queue.fail(new Error(event.reason)) : queue.close());
  socket.addEventListener("open", onOpen);
  socket.addEventListener("message", onMessage);
  socket.addEventListener("error", onError);
  socket.addEventListener("close", onClose);
  const abort = () => queue.close();
  signal?.addEventListener("abort", abort, { once: true });
  const close = () => {
    queue.close();
    signal?.removeEventListener("abort", abort);
    socket.removeEventListener?.("open", onOpen);
    socket.removeEventListener?.("message", onMessage);
    socket.removeEventListener?.("error", onError);
    socket.removeEventListener?.("close", onClose);
    if (socket.readyState === undefined || socket.readyState < 2) {
      socket.close(1000, "source consumer stopped");
    }
  };

  try {
    const deadline = performance.now() + HANDSHAKE_TIMEOUT_MILLISECONDS;
    if (socket.readyState === 1) queue.open();
    await beforeHandshakeDeadline(queue.waitUntilOpen(signal), deadline, signal);
    socket.send(subscriptionRequest(1, filter, logsKind));
    socket.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "eth_subscribe", params: ["newHeads"] }));
    socket.send(JSON.stringify({ jsonrpc: "2.0", id: 3, method: "eth_chainId", params: [] }));
    const result = await readAcknowledgements(
      queue,
      expectedChainId,
      queueCapacity,
      prefetchByteCapacity,
      deadline,
      signal,
    );
    return { socket, queue, close, ...result };
  } catch (error) {
    queue.close();
    close();
    throw error;
  }
}

async function readAcknowledgements(
  queue: BoundedFrameQueue,
  expectedChainId: bigint,
  prefetchCapacity: number,
  prefetchByteCapacity: number,
  deadline: number,
  signal?: AbortSignal,
): Promise<Pick<EstablishedSocket, "logsSubscription" | "headsSubscription" | "prefetched">> {
  let logsSubscription: string | undefined;
  let headsSubscription: string | undefined;
  let chainVerified = false;
  const prefetched = new BoundedRingBuffer<string>(prefetchCapacity);
  let prefetchedBytes = 0;
  while (!logsSubscription || !headsSubscription || !chainVerified) {
    const frame = await beforeHandshakeDeadline(queue.next(signal), deadline, signal);
    if (frame === undefined) throw new RpcError("TRANSPORT", "RPC WebSocket closed during subscription handshake");
    const value = JSON.parse(frame) as Record<string, unknown>;
    if (value.error) throw new RpcError("TRANSPORT", `RPC subscription error: ${JSON.stringify(value.error)}`);
    const id = handshakeResponseId(value);
    if (id === 1) logsSubscription = subscriptionAcknowledgement(logsSubscription, value.result, "logs");
    else if (id === 2) headsSubscription = subscriptionAcknowledgement(headsSubscription, value.result, "heads");
    else if (id === 3) {
      if (typeof value.result !== "string") throw new RpcError("TRANSPORT", "RPC chain-id acknowledgement is invalid");
      const actual = parseHexU64(value.result, "eth_chainId");
      if (actual !== expectedChainId)
        throw new RpcError("INVALID", `WebSocket RPC chain id mismatch: expected ${expectedChainId}, got ${actual}`);
      chainVerified = true;
    } else {
      const bytes = utf8Bytes(frame);
      if (prefetched.length >= prefetchCapacity || bytes > prefetchByteCapacity - prefetchedBytes)
        throw new RpcError("TRANSPORT", "RPC subscription handshake prefetch count or byte budget exceeded");
      prefetched.push(frame);
      prefetchedBytes += bytes;
    }
  }
  if (performance.now() >= deadline) throw new RpcError("TRANSPORT", "RPC subscription handshake timed out");
  return { logsSubscription, headsSubscription, prefetched };
}

function handshakeResponseId(value: Record<string, unknown>): number | undefined {
  if (value.id === undefined) return undefined;
  if (typeof value.id !== "number" || !Number.isSafeInteger(value.id))
    throw new RpcError("TRANSPORT", "RPC handshake response id must be an integer");
  return value.id;
}

function subscriptionAcknowledgement(current: string | undefined, result: unknown, kind: "logs" | "heads"): string {
  if (typeof result !== "string" || result.length === 0)
    throw new RpcError("TRANSPORT", `RPC ${kind} subscription acknowledgement is invalid`);
  if (current !== undefined && current !== result)
    throw new RpcError("TRANSPORT", `RPC ${kind} subscription acknowledgement changed`);
  return result;
}

function beforeHandshakeDeadline<T>(operation: Promise<T>, deadline: number, signal?: AbortSignal): Promise<T> {
  if (signal?.aborted) return Promise.reject(new RpcError("TRANSPORT", "RPC WebSocket operation aborted"));
  const remaining = deadline - performance.now();
  if (remaining <= 0) return Promise.reject(new RpcError("TRANSPORT", "RPC subscription handshake timed out"));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new RpcError("TRANSPORT", "RPC subscription handshake timed out")),
      remaining,
    );
    operation.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function decodeFrame(data: unknown): string | undefined {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data))
    return new TextDecoder().decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
  return undefined;
}

export function defaultWebSocketFactory(url: string): WebSocketLike {
  const constructor = (globalThis as typeof globalThis & { WebSocket?: new (url: string) => WebSocketLike }).WebSocket;
  if (!constructor) throw new RpcError("INVALID", "global WebSocket is unavailable; inject a WebSocketFactory");
  return new constructor(url);
}

export class BoundedFrameQueue {
  private readonly values: BoundedRingBuffer<{ value: string; bytes: number }>;
  private readonly openWaiters = new Set<() => void>();
  private readonly valueWaiters = new Set<(value: string | undefined) => void>();
  private opened = false;
  private ended = false;
  private failure?: Error;
  private retainedBytes = 0;

  constructor(
    private readonly capacity: number,
    private readonly byteCapacity: number,
  ) {
    if (!Number.isSafeInteger(byteCapacity) || byteCapacity <= 0)
      throw new Error("frame queue byte capacity must be a positive safe integer");
    this.values = new BoundedRingBuffer(capacity);
  }

  open(): void {
    if (this.ended) return;
    this.opened = true;
    this.resolveOpenWaiters();
  }

  push(value: string): void {
    if (this.ended) return;
    const bytes = utf8Bytes(value);
    if (this.values.length >= this.capacity || bytes > this.byteCapacity - this.retainedBytes) {
      this.fail(new Error("RPC WebSocket frame queue count or byte budget exceeded; canonical recovery required"));
      return;
    }
    const waiter = this.valueWaiters.values().next().value;
    if (waiter) {
      waiter(value);
      return;
    }
    this.values.push({ value, bytes });
    this.retainedBytes += bytes;
  }

  fail(error: Error): void {
    if (this.ended) return;
    this.failure = error;
    this.ended = true;
    this.values.clear();
    this.retainedBytes = 0;
    this.resolveOpenWaiters();
    this.resolveValueWaiters();
  }

  close(): void {
    if (this.ended) return;
    this.ended = true;
    this.resolveOpenWaiters();
    this.resolveValueWaiters();
  }

  async waitUntilOpen(signal?: AbortSignal): Promise<void> {
    if (this.opened) return;
    if (this.failure) throw this.failure;
    if (this.ended) throw new Error("RPC WebSocket closed before open");
    await this.waitForOpen(signal);
    if (!this.opened) throw this.failure ?? new Error("RPC WebSocket closed before open");
  }

  async next(signal?: AbortSignal): Promise<string | undefined> {
    if (this.failure) throw this.failure;
    const entry = this.values.shift();
    if (entry !== undefined) {
      this.retainedBytes -= entry.bytes;
      return entry.value;
    }
    if (this.ended) return undefined;
    return this.waitForValue(signal);
  }

  private waitForOpen(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new Error("RPC WebSocket operation aborted"));
    return new Promise((resolve, reject) => {
      let settled = false;
      const waiter = () => {
        if (settled) return;
        settled = true;
        signal?.removeEventListener("abort", onAbort);
        this.openWaiters.delete(waiter);
        if (this.failure) reject(this.failure);
        else if (this.opened) resolve();
        else reject(new Error("RPC WebSocket closed before open"));
      };
      const onAbort = () => {
        if (settled) return;
        settled = true;
        signal?.removeEventListener("abort", onAbort);
        this.openWaiters.delete(waiter);
        reject(new Error("RPC WebSocket operation aborted"));
      };
      this.openWaiters.add(waiter);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
    });
  }

  private waitForValue(signal?: AbortSignal): Promise<string | undefined> {
    if (signal?.aborted) return Promise.resolve(undefined);
    return new Promise((resolve, reject) => {
      let settled = false;
      const waiter = (value: string | undefined) => {
        if (settled) return;
        settled = true;
        signal?.removeEventListener("abort", onAbort);
        this.valueWaiters.delete(waiter);
        if (this.failure) reject(this.failure);
        else resolve(value);
      };
      const onAbort = () => {
        waiter(undefined);
      };
      this.valueWaiters.add(waiter);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
    });
  }

  private resolveOpenWaiters(): void {
    for (const waiter of [...this.openWaiters]) waiter();
  }

  private resolveValueWaiters(): void {
    while (this.valueWaiters.size > 0 && (this.values.length > 0 || this.ended)) {
      const entry = this.values.shift();
      if (entry) this.retainedBytes -= entry.bytes;
      const waiter = this.valueWaiters.values().next().value;
      if (waiter) waiter(entry?.value);
    }
  }
}

const UTF8_ENCODER = new TextEncoder();

function utf8Bytes(value: string): number {
  return UTF8_ENCODER.encode(value).byteLength;
}
