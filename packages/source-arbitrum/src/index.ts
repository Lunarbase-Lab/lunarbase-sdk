/** Arbitrum Nitro data source. */
import {
  Commitment,
  Network,
  type BackfillRequest,
  type BootstrapSnapshot,
  type ChainCursor,
  type ContractLog,
  type DeploymentConfig,
} from "@lunarbase-lab/pmm-v2-client";
import {
  EvmRpcSource,
  JsonRpcHttpClient,
  RpcError,
  type WebSocketFactory,
  type WsRpcConfig,
} from "@lunarbase-lab/pmm-v2-source-evm";

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
 * Nitro source with execution-block context.
 *
 * Nitro `l1BlockNumber` becomes the EVM execution block.
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

  /** Reads a coherent snapshot with explicit Nitro execution context. */
  override async snapshot(deployment: DeploymentConfig): Promise<BootstrapSnapshot> {
    const snapshot = await super.snapshot(deployment);
    return { ...snapshot, cursor: await withNitroExecutionContext(this.rpc, snapshot.cursor) };
  }

  /** Resolves Nitro execution context for every distinct backfilled block. */
  override async backfill(request: BackfillRequest): Promise<readonly ContractLog[]> {
    const logs = [...(await super.backfill(request))];
    const blockHashes = new Map<bigint, string>();
    for (const log of logs) {
      const blockHash = log.cursor.blockHash;
      if (blockHash === undefined)
        throw new RpcError("INVALID", `Arbitrum backfill log at block ${log.cursor.blockNumber} has no blockHash`);
      const previous = blockHashes.get(log.cursor.blockNumber);
      if (previous !== undefined && previous.toLowerCase() !== blockHash.toLowerCase())
        throw new RpcError("INVALID", `Arbitrum backfill block ${log.cursor.blockNumber} has conflicting hashes`);
      blockHashes.set(log.cursor.blockNumber, blockHash);
    }
    const blocks = [...blockHashes.entries()];
    const contexts = new Map<bigint, bigint>();
    for (let offset = 0; offset < blocks.length; offset += 16) {
      const entries = await Promise.all(
        blocks.slice(offset, offset + 16).map(async ([blockNumber, blockHash]) => {
          return [
            blockNumber,
            await nitroExecutionBlockNumber(this.rpc, this.chainId, blockNumber, blockHash, Commitment.Canonical),
          ] as const;
        }),
      );
      for (const [blockNumber, executionBlockNumber] of entries) contexts.set(blockNumber, executionBlockNumber);
    }
    return logs.map((log) => {
      const executionBlockNumber = contexts.get(log.cursor.blockNumber);
      if (executionBlockNumber === undefined)
        throw new RpcError("INVALID", `missing execution context for Arbitrum block ${log.cursor.blockNumber}`);
      return {
        ...log,
        cursor: { ...log.cursor, executionBlockNumber },
      };
    });
  }

  /** Reads the canonical head with explicit Nitro execution context. */
  override async canonicalHead(): Promise<ChainCursor> {
    return withNitroExecutionContext(this.rpc, await super.canonicalHead());
  }
}

async function withNitroExecutionContext(rpc: JsonRpcHttpClient, cursor: ChainCursor): Promise<ChainCursor> {
  if (cursor.blockHash === undefined)
    throw new RpcError("INVALID", `Nitro block ${cursor.blockNumber} has no block hash`);
  return {
    ...cursor,
    executionBlockNumber: await nitroExecutionBlockNumber(
      rpc,
      cursor.chainId,
      cursor.blockNumber,
      cursor.blockHash,
      cursor.commitment,
    ),
  };
}

async function nitroExecutionBlockNumber(
  rpc: JsonRpcHttpClient,
  chainId: bigint,
  blockNumber: bigint,
  expectedBlockHash: string,
  commitment: ChainCursor["commitment"],
): Promise<bigint> {
  const cursor = await rpc.blockCursor(`0x${blockNumber.toString(16)}`, chainId, commitment, true);
  if (cursor.blockNumber !== blockNumber)
    throw new RpcError("INVALID", `Nitro context block mismatch: expected ${blockNumber}, got ${cursor.blockNumber}`);
  if (cursor.blockHash === undefined || cursor.blockHash.toLowerCase() !== expectedBlockHash.toLowerCase())
    throw new RpcError("INVALID", `Nitro context hash mismatch for block ${blockNumber}`);
  return cursor.executionBlockNumber;
}
