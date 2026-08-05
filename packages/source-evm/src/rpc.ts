import {
  BPS,
  createLaneState,
  laneExists,
  parseAddress,
  type Address,
  type QuoteState,
} from "@lunarbase-lab/pmm-v2-math";
import type { AbiEvent as AbiEventType } from "ox/AbiEvent";
import * as Hash from "ox/Hash";
import * as Hex from "ox/Hex";
import { createPublicClient, http, type BlockTag, type PublicClient } from "viem";
import {
  Commitment as CommitmentValue,
  compareCursor,
  CORE_ABI,
  CORE_EVENTS,
  CORE_EVENT_TOPICS,
  decodeCoreEvent,
  decodeImplementation,
  ERC1967_IMPLEMENTATION_SLOT,
  laneDiscoveryTopics,
  type BackfillRequest,
  type BootstrapSnapshot,
  type ChainCursor,
  type Checkpoint,
  type ContractLog,
  type DeploymentConfig,
  type Network,
} from "@lunarbase-lab/pmm-v2-client";
import { RpcError } from "./rpc/error.js";
import { normalizeViemLog, parseHash, parseHexU64 } from "./rpc/log.js";

export { RpcError };
export { parseHexU64 };
export { parseHash, parseRpcLog } from "./rpc/log.js";

const BLOCK_TAGS = new Set<BlockTag>(["earliest", "finalized", "latest", "pending", "safe"]);
const LOG_RANGE_CHUNK_BLOCKS = 10_000n;
const SNAPSHOT_CONCURRENCY = 16;
const addressKey = parseAddress;

/**
 * Read-only viem HTTP client with every implicit network behavior disabled.
 *
 * One method call issues one JSON-RPC request: batching, retries, multicall,
 * and CCIP-read are disabled. Chain ID is read only through `chainId()`.
 */
export class JsonRpcHttpClient {
  /** viem public client configured without batching, retries, or CCIP reads. */
  readonly client: PublicClient;
  /** Injected fetch implementation retained for strict response-envelope validation. */
  private readonly strictFetcher: typeof fetch;

  /** Creates a strict read-only JSON-RPC client with an injectable fetcher. */
  constructor(
    /** Validated HTTP JSON-RPC endpoint retained for diagnostics. */
    readonly endpoint: string,
    fetcher: typeof fetch = fetch,
  ) {
    const url = new URL(endpoint);
    if (url.protocol !== "http:" && url.protocol !== "https:")
      throw new RpcError("INVALID", "HTTP RPC URL must use http: or https:");
    this.strictFetcher = fetcher;
    this.client = createPublicClient({
      batch: { multicall: false },
      ccipRead: false,
      transport: http(url.toString(), {
        batch: false,
        fetchFn: fetcher,
        retryCount: 0,
        timeout: 15_000,
      }),
    });
  }

  /** Reads the chain ID with exactly one explicit `eth_chainId` request. */
  async chainId(): Promise<bigint> {
    return BigInt(await this.remote(() => this.client.getChainId()));
  }

  /** Reads runtime bytecode at an explicit block tag or block number. */
  async getCode(address: Address, blockTag: string): Promise<Hex.Hex> {
    return (
      (await this.remote(() =>
        this.client.getCode({
          address,
          ...blockParameters(blockTag),
        }),
      )) ?? "0x"
    );
  }

  /** Reads runtime bytecode against an exact EIP-1898 block hash. */
  async getCodeAtHash(address: Address, blockHash: Hex.Hex): Promise<Hex.Hex> {
    return (
      (await this.remote(() =>
        this.client.getCode({
          address,
          blockHash,
        }),
      )) ?? "0x"
    );
  }

  /** Reads one storage word against an exact EIP-1898 block hash. */
  async getStorageAtHash(address: Address, slot: Hex.Hex, blockHash: Hex.Hex): Promise<Hex.Hex> {
    return (
      (await this.remote(() =>
        this.client.getStorageAt({
          address,
          slot,
          blockHash,
        }),
      )) ?? Hex.padLeft("0x", 32)
    );
  }

  /** Computes the exact runtime bytecode hash at an explicit block tag. */
  async runtimeCodeHash(address: Address, blockTag: string): Promise<Hex.Hex> {
    return Hash.keccak256(await this.getCode(address, blockTag));
  }

  /** Computes runtime bytecode hash at one exact EIP-1898 block hash. */
  async runtimeCodeHashAtHash(address: Address, blockHash: Hex.Hex): Promise<Hex.Hex> {
    return Hash.keccak256(await this.getCodeAtHash(address, blockHash));
  }

  /** Converts one explicit `eth_getBlockByNumber` into a source cursor. */
  async blockCursor(
    blockTag: string,
    chainId: bigint,
    commitment: ChainCursor["commitment"],
    requireExecutionBlockNumber = false,
  ): Promise<ChainCursor> {
    if (requireExecutionBlockNumber) return this.strictExecutionBlockCursor(blockTag, chainId, commitment);
    const block = await this.remote(() =>
      this.client.getBlock({
        includeTransactions: false,
        ...blockParameters(blockTag),
      }),
    );
    if (block.number === null) throw new RpcError("INVALID", "eth_getBlockByNumber returned a pending block");
    const l1BlockNumber = (block as typeof block & { l1BlockNumber?: unknown }).l1BlockNumber;
    return {
      chainId,
      blockNumber: block.number,
      executionBlockNumber:
        l1BlockNumber === undefined || l1BlockNumber === null
          ? block.number
          : parseHexU64(l1BlockNumber, "block.l1BlockNumber"),
      blockHash: block.hash ?? undefined,
      commitment,
    };
  }

  private async strictExecutionBlockCursor(
    blockTag: string,
    chainId: bigint,
    commitment: ChainCursor["commitment"],
  ): Promise<ChainCursor> {
    return this.remote(async () => {
      blockParameters(blockTag);
      const requestId = `execution-context:${blockTag}`;
      const response = await this.strictFetcher(this.endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: requestId,
          method: "eth_getBlockByNumber",
          params: [blockTag, false],
        }),
        signal: AbortSignal.timeout(15_000),
      });
      if (!response.ok) throw new RpcError("TRANSPORT", `HTTP RPC returned ${response.status}`);
      const envelope = (await response.json()) as unknown;
      if (envelope === null || typeof envelope !== "object" || Array.isArray(envelope))
        throw new RpcError("INVALID", "JSON-RPC response is not an object");
      const object = envelope as Record<string, unknown>;
      if (object.jsonrpc !== "2.0") throw new RpcError("INVALID", "JSON-RPC response version is not 2.0");
      if (object.id !== requestId) throw new RpcError("INVALID", "JSON-RPC response id mismatch");
      if (object.error !== undefined && object.error !== null)
        throw new RpcError("TRANSPORT", `JSON-RPC returned an error: ${JSON.stringify(object.error)}`);
      if (object.result === null || typeof object.result !== "object" || Array.isArray(object.result))
        throw new RpcError("INVALID", "JSON-RPC response has no block result");
      const block = object.result as Record<string, unknown>;
      const blockNumber = parseHexU64(block.number, "block.number");
      const executionBlockNumber = parseHexU64(block.l1BlockNumber, "block.l1BlockNumber");
      return {
        chainId,
        blockNumber,
        executionBlockNumber,
        blockHash: block.hash === null || block.hash === undefined ? undefined : parseHash(block.hash, "block.hash"),
        commitment,
      };
    });
  }

  /**
   * Fetches canonical logs in bounded ranges and bisects dense chunks rejected
   * by a provider's documented result/range limit.
   */
  async getLogs(
    request: BackfillRequest,
    chainId: bigint,
    commitment: ChainCursor["commitment"],
  ): Promise<ContractLog[]> {
    if (request.fromBlock > request.toBlock) throw new RpcError("INVALID", "log range starts after its end");
    const events = eventsForTopics(request.filter.topics);
    const pending = initialLogRanges(request.fromBlock, request.toBlock);
    const logs: ContractLog[] = [];
    while (pending.length > 0) {
      const [fromBlock, toBlock] = pending.shift()!;
      try {
        const chunk = await this.remote(() =>
          this.client.getLogs({
            address: request.filter.address,
            events: events.length === 0 ? undefined : events,
            fromBlock,
            toBlock,
            strict: false,
          }),
        );
        logs.push(...chunk.map((value) => normalizeViemLog(value, chainId, commitment)));
      } catch (error) {
        if (fromBlock >= toBlock || !isLogRangeLimit(error)) throw error;
        const middle = fromBlock + (toBlock - fromBlock) / 2n;
        pending.unshift([middle + 1n, toBlock]);
        pending.unshift([fromBlock, middle]);
      }
    }
    logs.sort((left, right) => compareCursor(left.cursor, right.cursor));
    return logs;
  }

  private async remote<T>(operation: () => Promise<T>): Promise<T> {
    try {
      return await operation();
    } catch (error) {
      if (error instanceof RpcError) throw error;
      throw new RpcError("TRANSPORT", error instanceof Error ? error.message : String(error));
    }
  }
}

function initialLogRanges(fromBlock: bigint, toBlock: bigint): Array<[bigint, bigint]> {
  const ranges: Array<[bigint, bigint]> = [];
  for (let start = fromBlock; start <= toBlock; start += LOG_RANGE_CHUNK_BLOCKS) {
    const end = start + LOG_RANGE_CHUNK_BLOCKS - 1n;
    ranges.push([start, end < toBlock ? end : toBlock]);
  }
  return ranges;
}

function isLogRangeLimit(error: unknown): boolean {
  const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
  return ["too many results", "response size", "block range", "query exceeds", "limit exceeded", "-32005"].some(
    (needle) => message.includes(needle),
  );
}

/** Canonical HTTP fallback for backfill, heads, and checkpoint validation. */
export class RpcHttpBackend {
  constructor(
    /** Strict read-only JSON-RPC client. */
    readonly rpc: JsonRpcHttpClient,
    /** Network family exposed through the common source interface. */
    readonly network: Network,
    /** EIP-155 chain identifier attached to normalized cursors. */
    readonly chainId: bigint,
    /** Explicit block tag used for canonical snapshots and heads. */
    readonly snapshotTag = "latest",
  ) {}

  /** Reads the block-tagged source head. */
  canonicalHead(): Promise<ChainCursor> {
    return this.rpc.blockCursor(
      this.snapshotTag,
      this.chainId,
      this.snapshotTag === "finalized" ? CommitmentValue.Finalized : CommitmentValue.Canonical,
    );
  }

  /** Backfills canonical logs through one `eth_getLogs` request. */
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]> {
    return this.rpc.getLogs(request, this.chainId, CommitmentValue.Canonical);
  }

  /** Confirms checkpoint canonicality and the ERC-1967 implementation identity. */
  async validateCheckpoint(checkpoint: Checkpoint): Promise<boolean> {
    const canonical = await this.rpc.blockCursor(
      Hex.fromNumber(checkpoint.cursor.blockNumber),
      this.chainId,
      CommitmentValue.Canonical,
    );
    if (
      canonical.blockHash === undefined ||
      checkpoint.cursor.blockHash === undefined ||
      canonical.blockHash.toLowerCase() !== checkpoint.cursor.blockHash.toLowerCase()
    )
      return false;
    let implementation: Address;
    try {
      implementation = decodeImplementation(
        await this.rpc.getStorageAtHash(checkpoint.core, ERC1967_IMPLEMENTATION_SLOT, canonical.blockHash),
      );
    } catch {
      return false;
    }
    if (implementation.toLowerCase() !== checkpoint.expectedImplementation.toLowerCase()) return false;
    return (
      (await this.rpc.runtimeCodeHashAtHash(implementation, canonical.blockHash)).toLowerCase() ===
      checkpoint.expectedImplementationCodeHash.toLowerCase()
    );
  }
}

/** Materializes all quote-critical Core state at one explicit block. */
export class RpcSnapshotProvider {
  constructor(
    /** Strict read-only JSON-RPC client used for Core view calls. */
    readonly rpc: JsonRpcHttpClient,
    /** Block tag applied to every read in one coherent snapshot. */
    readonly snapshotTag = "latest",
  ) {}

  /** Reads code, lanes, reserves, router policy, and the snapshot cursor. */
  async snapshot(config: DeploymentConfig): Promise<BootstrapSnapshot> {
    const rpcChainId = await this.rpc.chainId();
    if (rpcChainId !== config.chainId)
      throw new RpcError("INVALID", `HTTP RPC chain id mismatch: expected ${config.chainId}, got ${rpcChainId}`);
    const commitment = this.snapshotTag === "finalized" ? CommitmentValue.Finalized : CommitmentValue.Canonical;
    const cursor = await this.rpc.blockCursor(this.snapshotTag, config.chainId, commitment);
    if (cursor.blockHash === undefined) throw new RpcError("INVALID", "snapshot block has no canonical hash");
    if (cursor.blockNumber < config.deploymentBlock)
      throw new RpcError("INVALID", "snapshot block precedes deployment block");

    const pinnedBlock = Hex.fromNumber(cursor.blockNumber);
    const implementation = decodeImplementation(
      await this.rpc.getStorageAtHash(config.core, ERC1967_IMPLEMENTATION_SLOT, cursor.blockHash),
    );
    if (implementation.toLowerCase() !== config.expectedImplementation.toLowerCase())
      throw new RpcError("INVALID", "Core implementation address mismatch");
    const implementationCodeHash = Hash.keccak256(await this.rpc.getCodeAtHash(implementation, cursor.blockHash));
    if (implementationCodeHash.toLowerCase() !== config.expectedImplementationCodeHash.toLowerCase())
      throw new RpcError("INVALID", "implementation code hash mismatch");

    const assets = await this.resolveLaneAssets(config, cursor.blockNumber);
    const at = { blockHash: cursor.blockHash } as const;
    const [cash, whitelisted, blacklistFeeMultiplier] = await Promise.all([
      this.rpc.client.readContract({
        abi: CORE_ABI,
        address: config.core,
        functionName: "cash",
        ...at,
      }),
      this.rpc.client.readContract({
        abi: CORE_ABI,
        address: config.core,
        functionName: "whitelist",
        args: [config.router],
        ...at,
      }),
      this.rpc.client.readContract({
        abi: CORE_ABI,
        address: config.core,
        functionName: "blacklistFeeMultiplier",
        ...at,
      }),
    ]);
    if (whitelisted !== config.expectWhitelisted)
      throw new RpcError(
        "INVALID",
        `configured router whitelist mismatch: expected ${config.expectWhitelisted}, got ${whitelisted}`,
      );
    const lanes = new Map<Address, ReturnType<typeof createLaneState>>();
    const partnerFeeBps = new Map<Address, number>();
    const state: QuoteState = {
      cash,
      cashReserve: 0n,
      lanes,
      feeProfile: {
        whitelisted,
        blacklistFeeMultiplier,
        partnerFeeBps,
      },
    };

    for (let offset = 0; offset < assets.length; offset += SNAPSHOT_CONCURRENCY) {
      const entries = await Promise.all(
        assets.slice(offset, offset + SNAPSHOT_CONCURRENCY).map(async (asset) => {
          const [lane, reserves] = await Promise.all([
            this.rpc.client.readContract({
              abi: CORE_ABI,
              address: config.core,
              functionName: "lane",
              args: [asset],
              ...at,
            }),
            this.rpc.client.readContract({
              abi: CORE_ABI,
              address: config.core,
              functionName: "reserves",
              args: [asset],
              ...at,
            }),
          ]);
          return [asset, createLaneState(Hex.toBigInt(lane), reserves[0], reserves[4])] as const;
        }),
      );
      for (const [asset, lane] of entries) lanes.set(addressKey(asset), lane);
    }
    const cashReserves = await this.rpc.client.readContract({
      abi: CORE_ABI,
      address: config.core,
      functionName: "reserves",
      args: [cash],
      ...at,
    });
    state.cashReserve = cashReserves[0];
    if (config.explicitLaneAssets.length > 0 && [...lanes.values()].some((lane) => !laneExists(lane)))
      throw new RpcError("INVALID", "explicit lane asset is not active at the snapshot block");

    const partnerAssets = [...new Set([...assets, cash])];
    for (let offset = 0; offset < partnerAssets.length; offset += SNAPSHOT_CONCURRENCY) {
      const entries = await Promise.all(
        partnerAssets.slice(offset, offset + SNAPSHOT_CONCURRENCY).map(async (asset) => {
          const partner = await this.rpc.client.readContract({
            abi: CORE_ABI,
            address: config.core,
            functionName: "partners",
            args: [config.router, asset],
            ...at,
          });
          if (BigInt(partner[1]) > BPS) throw new RpcError("INVALID", "partner fee exceeds BPS");
          return [asset, partner[1]] as const;
        }),
      );
      for (const [asset, fee] of entries) partnerFeeBps.set(addressKey(asset), fee);
    }
    const verified = await this.rpc.blockCursor(pinnedBlock, config.chainId, commitment);
    if (verified.blockHash?.toLowerCase() !== cursor.blockHash.toLowerCase())
      throw new RpcError("TRANSPORT", "snapshot block changed while state was reconstructed");
    return { state, cursor, implementation, implementationCodeHash };
  }

  private async resolveLaneAssets(config: DeploymentConfig, snapshotBlock: bigint): Promise<Address[]> {
    if (config.explicitLaneAssets.length > 0) return config.explicitLaneAssets.map(addressKey);
    const history = await this.rpc.getLogs(
      {
        fromBlock: config.deploymentBlock,
        toBlock: snapshotBlock,
        filter: { address: config.core, topics: laneDiscoveryTopics() },
      },
      config.chainId,
      CommitmentValue.Canonical,
    );
    history.sort((left, right) => compareCursor(left.cursor, right.cursor));
    const active = new Set<Address>();
    for (const log of history) {
      const event = decodeCoreEvent(log);
      if (event?.kind === "LaneAdded") active.add(addressKey(event.asset));
      else if (event?.kind === "LaneRemoved") active.delete(addressKey(event.asset));
    }
    return [...active];
  }
}

function eventsForTopics(topics: readonly Hex.Hex[]): readonly AbiEventType[] {
  const entries = Object.entries(CORE_EVENT_TOPICS) as Array<
    [keyof typeof CORE_EVENT_TOPICS, (typeof CORE_EVENT_TOPICS)[keyof typeof CORE_EVENT_TOPICS]]
  >;
  const selected = entries
    .filter(([, selector]) => topics.includes(selector))
    .map(([name]) => CORE_EVENTS[name] as AbiEventType);
  if (selected.length !== topics.length)
    throw new RpcError("INVALID", "ContractFilter contains a topic outside the pinned Core ABI");
  return selected;
}

function blockParameters(blockTag: string): { blockNumber: bigint } | { blockTag: BlockTag } {
  if (BLOCK_TAGS.has(blockTag as BlockTag)) return { blockTag: blockTag as BlockTag };
  return { blockNumber: parseHexU64(blockTag, "block tag") };
}

/** Computes Ethereum Keccak-256 with Ox's audited noble implementation. */
export function keccak256Hex(input: Uint8Array | Hex.Hex): Hex.Hex {
  return Hash.keccak256(input, { as: "Hex" });
}
