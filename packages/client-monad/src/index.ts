/** Experimental portable Monad execution-events client. */
import {
  connect,
  JsonRpcHttpClient,
  Network,
  type Checkpoint,
  type ClientConnectConfig,
  type ConnectedQuoteClient,
  type WebSocketFactory,
} from "@lunarbase/client-core";
import { MonadParserSource, type MonadParserConfig } from "./transport.js";

export * from "./execution.js";
export * from "./transport.js";

/** Optional parser transport dependency injection. */
export interface MonadClientOptions {
  readonly snapshotTag?: string;
  readonly parserConfig?: Partial<MonadParserConfig>;
  readonly fetcher?: typeof fetch;
  readonly webSocketFactory?: WebSocketFactory;
}

/** Connects the portable parser/RPC Monad client. */
export function connectMonad(
  config: ClientConnectConfig,
  optionalCheckpoint?: Checkpoint,
  options: MonadClientOptions = {},
): Promise<ConnectedQuoteClient> {
  if (config.deployment.network !== Network.Monad) throw new Error("Monad client requires Network.Monad");
  const source = new MonadParserSource(
    new JsonRpcHttpClient(config.deployment.httpRpcUrl, options.fetcher ?? fetch),
    config.deployment.realtimeSource,
    config.deployment.chainId,
    options.snapshotTag ?? "finalized",
    options.parserConfig,
    options.webSocketFactory,
  );
  return connect(config, source, optionalCheckpoint);
}
