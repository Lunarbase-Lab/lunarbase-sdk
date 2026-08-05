/** Official Base Flashblocks profile for the generic EVM source. */
import { Network } from "@lunarbase-lab/pmm-v2-client";
import { JsonRpcHttpClient } from "./rpc.js";
import { EvmRpcSource, type WebSocketFactory, type WsRpcConfig } from "./ws.js";

/** Optional dependency injection and resource bounds for Base Flashblocks. */
export interface BaseFlashblocksOptions {
  /** Canonical block tag used for coherent bootstrap snapshots. */
  readonly snapshotTag?: string;
  /** Overrides for bounded Flashblocks WebSocket resources. */
  readonly wsConfig?: Partial<WsRpcConfig>;
  /** Optional HTTP implementation for Node, browser, or tests. */
  readonly fetcher?: typeof fetch;
  /** Optional WebSocket implementation for Node, browser, or tests. */
  readonly webSocketFactory?: WebSocketFactory;
}

/** Endpoints and chain identity required by the Base source. */
export interface BaseFlashblocksSourceConfig {
  /** Canonical HTTP JSON-RPC endpoint used for bootstrap and recovery. */
  readonly httpRpcUrl: string;
  /** Flashblocks WebSocket endpoint used for realtime subscriptions. */
  readonly realtimeUrl: string;
  /** EIP-155 chain identifier attached to normalized cursors. */
  readonly chainId: bigint;
}

/** Creates the official Base `pendingLogs + newHeads` data source. */
export function createBaseFlashblocksSource(
  config: BaseFlashblocksSourceConfig,
  options: BaseFlashblocksOptions = {},
): EvmRpcSource {
  return new EvmRpcSource(
    new JsonRpcHttpClient(config.httpRpcUrl, options.fetcher ?? fetch),
    config.realtimeUrl,
    Network.Base,
    config.chainId,
    options.snapshotTag ?? "latest",
    {
      ...options.wsConfig,
      logsSubscription: "pendingLogs",
      progressiveHeads: true,
    },
    options.webSocketFactory,
  );
}
