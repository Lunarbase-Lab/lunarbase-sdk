/** Experimental Arbitrum Nitro client pending node-level execution validation. */
import {
  connect,
  Commitment,
  JsonRpcHttpClient,
  Network,
  WsRpcBackend,
  type Checkpoint,
  type BackfillRequest,
  type ClientConnectConfig,
  type ConnectedQuoteClient,
  type ContractLog,
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
      options.snapshotTag ?? "latest",
      { ...options.wsConfig, logsSubscription: "logs" },
      options.webSocketFactory,
    );
  }

  /** Resolves Nitro execution context for every distinct backfilled block. */
  override async backfill(request: BackfillRequest): Promise<readonly ContractLog[]> {
    const logs = [...(await super.backfill(request))];
    const blockNumbers = [...new Set(logs.map((log) => log.cursor.blockNumber))];
    const contexts = new Map<bigint, bigint>();
    for (let offset = 0; offset < blockNumbers.length; offset += 16) {
      const entries = await Promise.all(
        blockNumbers.slice(offset, offset + 16).map(async (blockNumber) => {
          const cursor = await this.rpc.blockCursor(
            `0x${blockNumber.toString(16)}`,
            this.chainId,
            Commitment.Canonical,
          );
          return [blockNumber, cursor.executionBlockNumber] as const;
        }),
      );
      for (const [blockNumber, executionBlockNumber] of entries) contexts.set(blockNumber, executionBlockNumber);
    }
    return logs.map((log) => {
      const executionBlockNumber = contexts.get(log.cursor.blockNumber);
      if (executionBlockNumber === undefined)
        throw new Error(`missing execution context for Arbitrum block ${log.cursor.blockNumber}`);
      return {
        ...log,
        cursor: { ...log.cursor, executionBlockNumber },
      };
    });
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
