/** Browser-compatible socket lifecycle and bounded frame queue. */
import type { ContractFilter } from "@lunarbase/client";
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
  readonly prefetched: string[];
  readonly close: () => void;
}

export async function establishSocket(
  url: string,
  factory: WebSocketFactory,
  filter: ContractFilter,
  logsKind: "logs" | "pendingLogs",
  expectedChainId: bigint,
  queueCapacity: number,
  maxFrameBytes: number,
  signal?: AbortSignal,
): Promise<EstablishedSocket> {
  const socket = factory(url);
  const queue = new BoundedFrameQueue(queueCapacity);
  const onOpen = () => queue.open();
  const onMessage = (event: SocketEvent) => {
    const frame = decodeFrame(event.data);
    if (frame === undefined) queue.fail(new Error("RPC WebSocket delivered a non-text frame"));
    else if (new TextEncoder().encode(frame).byteLength > maxFrameBytes)
      queue.fail(new Error("RPC WebSocket frame exceeded configured bound"));
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
    if (socket.readyState === 1) queue.open();
    await withTimeout(queue.waitUntilOpen(signal), signal);
    socket.send(subscriptionRequest(1, filter, logsKind));
    socket.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "eth_subscribe", params: ["newHeads"] }));
    socket.send(JSON.stringify({ jsonrpc: "2.0", id: 3, method: "eth_chainId", params: [] }));
    const result = await readAcknowledgements(queue, expectedChainId, signal);
    return { socket, queue, close, ...result };
  } catch (error) {
    close();
    throw error;
  }
}

async function readAcknowledgements(
  queue: BoundedFrameQueue,
  expectedChainId: bigint,
  signal?: AbortSignal,
): Promise<Pick<EstablishedSocket, "logsSubscription" | "headsSubscription" | "prefetched">> {
  let logsSubscription: string | undefined;
  let headsSubscription: string | undefined;
  let chainVerified = false;
  const prefetched: string[] = [];
  while (!logsSubscription || !headsSubscription || !chainVerified) {
    const frame = await withTimeout(queue.next(signal), signal);
    if (frame === undefined) throw new RpcError("TRANSPORT", "RPC WebSocket closed during subscription handshake");
    const value = JSON.parse(frame) as Record<string, unknown>;
    if (value.error) throw new RpcError("TRANSPORT", `RPC subscription error: ${JSON.stringify(value.error)}`);
    if (Number(value.id) === 1 && typeof value.result === "string") logsSubscription = value.result;
    else if (Number(value.id) === 2 && typeof value.result === "string") headsSubscription = value.result;
    else if (Number(value.id) === 3 && typeof value.result === "string") {
      const actual = parseHexU64(value.result, "eth_chainId");
      if (actual !== expectedChainId)
        throw new RpcError("INVALID", `WebSocket RPC chain id mismatch: expected ${expectedChainId}, got ${actual}`);
      chainVerified = true;
    } else prefetched.push(frame);
  }
  return { logsSubscription, headsSubscription, prefetched };
}

function withTimeout<T>(operation: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal?.aborted) return Promise.reject(new RpcError("TRANSPORT", "RPC WebSocket operation aborted"));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new RpcError("TRANSPORT", "RPC subscription handshake timed out")),
      HANDSHAKE_TIMEOUT_MILLISECONDS,
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
  private readonly values: string[] = [];
  private readonly waiters: Array<(value: string | undefined) => void> = [];
  private opened = false;
  private ended = false;
  private failure?: Error;

  constructor(private readonly capacity: number) {}

  open(): void {
    this.opened = true;
    this.resolveWaiters();
  }

  push(value: string): void {
    if (this.ended) return;
    if (this.values.length >= this.capacity) {
      this.fail(new Error("RPC WebSocket frame queue overflow; canonical recovery required"));
      return;
    }
    this.values.push(value);
    this.resolveWaiters();
  }

  fail(error: Error): void {
    if (this.ended) return;
    this.failure = error;
    this.ended = true;
    this.resolveWaiters();
  }

  close(): void {
    if (this.ended) return;
    this.ended = true;
    this.resolveWaiters();
  }

  async waitUntilOpen(signal?: AbortSignal): Promise<void> {
    if (this.opened) return;
    await this.wait(signal);
    if (!this.opened) throw this.failure ?? new Error("RPC WebSocket closed before open");
  }

  async next(signal?: AbortSignal): Promise<string | undefined> {
    if (this.failure) throw this.failure;
    const value = this.values.shift();
    if (value !== undefined) return value;
    if (this.ended) return undefined;
    return this.wait(signal);
  }

  private wait(signal?: AbortSignal): Promise<string | undefined> {
    if (signal?.aborted) return Promise.resolve(undefined);
    return new Promise((resolve, reject) => {
      const waiter = (value: string | undefined) => {
        signal?.removeEventListener("abort", onAbort);
        if (this.failure) reject(this.failure);
        else resolve(value);
      };
      const removeWaiter = () => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
      };
      const onAbort = () => {
        signal?.removeEventListener("abort", onAbort);
        removeWaiter();
        resolve(undefined);
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      this.waiters.push(waiter);
    });
  }

  private resolveWaiters(): void {
    while (this.waiters.length > 0 && (this.values.length > 0 || this.ended || this.opened)) {
      this.waiters.shift()?.(this.values.shift());
      if (!this.values.length && !this.ended && !this.opened) break;
    }
  }
}
