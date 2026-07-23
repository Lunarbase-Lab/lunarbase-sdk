/** Experimental Arbitrum Nitro source pending node-level validation. */
import { Commitment, Network, type BackfillRequest, type ContractLog } from "@lunarbase/client";
import { EvmRpcSource, JsonRpcHttpClient, type WebSocketFactory, type WsRpcConfig } from "@lunarbase/source-evm";

/** Optional dependency injection and transport bounds for Nitro RPC. */
export interface ArbitrumSourceOptions {
  /** Canonical block tag used for coherent bootstrap snapshots. */
  readonly snapshotTag?: string;
  /** Overrides for bounded Nitro WebSocket resources. */
  readonly wsConfig?: Partial<WsRpcConfig>;
  /** Optional HTTP implementation for Node, browser, or tests. */
  readonly fetcher?: typeof fetch;
  /** Optional WebSocket implementation for Node, browser, or tests. */
  readonly webSocketFactory?: WebSocketFactory;
}

/** Endpoints and chain identity required by the Nitro source. */
export interface ArbitrumNitroSourceConfig {
  /** Canonical Nitro HTTP JSON-RPC endpoint. */
  readonly httpRpcUrl: string;
  /** Executed-state WebSocket endpoint for logs and heads. */
  readonly realtimeUrl: string;
  /** EIP-155 chain identifier attached to normalized cursors. */
  readonly chainId: bigint;
}

/**
 * Generic Nitro source.
 *
 * `l1BlockNumber`, when exposed by the node, becomes the EVM execution block;
 * exact live semantics remain gated by Nitro-node validation.
 */
export class ArbitrumNitroSource extends EvmRpcSource {
  constructor(config: ArbitrumNitroSourceConfig, options: ArbitrumSourceOptions = {}) {
    super(
      new JsonRpcHttpClient(config.httpRpcUrl, options.fetcher ?? fetch),
      config.realtimeUrl,
      Network.Arbitrum,
      config.chainId,
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
