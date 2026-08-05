/** Portable Monad execution-events source. */
import { JsonRpcHttpClient, type WebSocketFactory } from "@lunarbase-lab/pmm-v2-source-evm";
import { MonadParserSource, type MonadParserConfig } from "./transport.js";

export * from "./execution.js";
export * from "./transport.js";

/** Optional parser transport dependency injection. */
export interface MonadSourceOptions {
  /** Canonical block tag used for coherent bootstrap snapshots. */
  readonly snapshotTag?: string;
  /** Overrides for bounded parser WebSocket resources. */
  readonly parserConfig?: Partial<MonadParserConfig>;
  /** Optional HTTP implementation for Node, browser, or tests. */
  readonly fetcher?: typeof fetch;
  /** Optional WebSocket implementation for Node, browser, or tests. */
  readonly webSocketFactory?: WebSocketFactory;
}

/** Endpoints and chain identity required by the portable Monad source. */
export interface MonadParserSourceConfig {
  /** Canonical HTTP JSON-RPC endpoint used for bootstrap and recovery. */
  readonly httpRpcUrl: string;
  /** Portable parser WebSocket subscription endpoint. */
  readonly realtimeUrl: string;
  /** EIP-155 chain identifier attached to normalized cursors. */
  readonly chainId: bigint;
}

/** Creates the portable parser/RPC Monad source. */
export function createMonadParserSource(
  config: MonadParserSourceConfig,
  options: MonadSourceOptions = {},
): MonadParserSource {
  return new MonadParserSource(
    new JsonRpcHttpClient(config.httpRpcUrl, options.fetcher ?? fetch),
    config.realtimeUrl,
    config.chainId,
    options.snapshotTag ?? "latest",
    options.parserConfig,
    options.webSocketFactory,
  );
}
