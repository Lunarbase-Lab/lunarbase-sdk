/** Experimental Arbitrum Nitro client pending node-level execution validation. */
import {
  connect,
  JsonRpcHttpClient,
  Network,
  WsRpcBackend,
  type Checkpoint,
  type ClientConnectConfig,
  type ConnectedQuoteClient,
  type WebSocketFactory,
  type WsRpcConfig,
} from "@lunarbase/client-core";

/** Optional dependency injection and transport bounds for Nitro RPC. */
export interface ArbitrumClientOptions {
  /** Canonical block tag used for coherent bootstrap snapshots. */
  readonly snapshotTag?: string;
  /** Overrides for bounded Nitro WebSocket resources. */
  readonly wsConfig?: Partial<WsRpcConfig>;
  /** Optional HTTP implementation for Node, browser, or tests. */
  readonly fetcher?: typeof fetch;
  /** Optional WebSocket implementation for Node, browser, or tests. */
  readonly webSocketFactory?: WebSocketFactory;
}

/**
 * Generic Nitro source.
 *
 * `l1BlockNumber`, when exposed by the node, becomes the EVM execution block;
 * exact live semantics remain gated by Nitro-node validation.
 */
export class ArbitrumDataSource extends WsRpcBackend {
  constructor(config: ClientConnectConfig, options: ArbitrumClientOptions = {}) {
    if (config.deployment.network !== Network.Arbitrum) throw new Error("Arbitrum client requires Network.Arbitrum");
    super(
      new JsonRpcHttpClient(config.deployment.httpRpcUrl, options.fetcher ?? fetch),
      config.deployment.realtimeSource,
      Network.Arbitrum,
      config.deployment.chainId,
      options.snapshotTag ?? "finalized",
      { ...options.wsConfig, logsSubscription: "logs" },
      options.webSocketFactory,
    );
  }
}

/** Connects the experimental Arbitrum client. */
export function connectArbitrum(
  config: ClientConnectConfig,
  optionalCheckpoint?: Checkpoint,
  options?: ArbitrumClientOptions,
): Promise<ConnectedQuoteClient> {
  return connect(config, new ArbitrumDataSource(config, options), optionalCheckpoint);
}
