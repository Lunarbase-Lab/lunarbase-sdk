/** Stable Base Flashblocks client using official `pendingLogs` and `newHeads`. */
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

/** Optional dependency injection and transport bounds for Base. */
export interface BaseClientOptions {
  readonly snapshotTag?: string;
  readonly wsConfig?: Partial<WsRpcConfig>;
  readonly fetcher?: typeof fetch;
  readonly webSocketFactory?: WebSocketFactory;
}

/** Complete Base source with HTTP bootstrap/recovery and Flashblocks realtime. */
export class BaseDataSource extends WsRpcBackend {
  /** Builds the official `pendingLogs + newHeads` source. */
  constructor(config: ClientConnectConfig, options: BaseClientOptions = {}) {
    if (config.deployment.network !== Network.Base) throw new Error("Base client requires Network.Base");
    super(
      new JsonRpcHttpClient(config.deployment.httpRpcUrl, options.fetcher ?? fetch),
      config.deployment.realtimeSource,
      Network.Base,
      config.deployment.chainId,
      options.snapshotTag ?? "finalized",
      {
        ...options.wsConfig,
        logsSubscription: "pendingLogs",
        progressiveHeads: true,
      },
      options.webSocketFactory,
    );
  }
}

/** Connects a ready-to-embed Base client. */
export function connectBase(
  config: ClientConnectConfig,
  optionalCheckpoint?: Checkpoint,
  options?: BaseClientOptions,
): Promise<ConnectedQuoteClient> {
  return connect(config, new BaseDataSource(config, options), optionalCheckpoint);
}
