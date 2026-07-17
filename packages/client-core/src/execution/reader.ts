import type { Address, Word } from "@lunarbase/math";
import type { ChainCursor, Commitment, ContractFilter } from "../model.js";

/** Block lifecycle notification emitted by an execution engine. */
export interface ExecutionHead {
  readonly sequence: bigint;
  readonly blockNumber: bigint;
  readonly blockHash?: string;
  readonly commitment: Commitment;
}

/** EVM log emitted before network-specific source normalization. */
export interface ExecutionLog {
  readonly sequence: bigint;
  readonly sourceSubIndex: bigint;
  readonly blockNumber: bigint;
  readonly blockHash?: string;
  readonly transactionIndex: bigint;
  readonly logIndex: bigint;
  readonly address: Address;
  readonly topics: readonly Word[];
  readonly data: string;
  readonly commitment: Commitment;
}

/** Raw lifecycle event produced by a colocated or remote execution reader. */
export type ExecutionEvent =
  | { readonly kind: "Head"; readonly head: ExecutionHead }
  | { readonly kind: "Log"; readonly log: ExecutionLog }
  | { readonly kind: "Gap"; readonly cursor?: ChainCursor; readonly reason: string };

/** Deployment-specific execution event input such as parser WS or native FFI. */
export interface ExecutionEventReader {
  subscribeExecution(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ExecutionEvent>;
}
