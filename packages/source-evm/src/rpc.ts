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
  contractLogRetainedBytes,
  CORE_ABI,
  CORE_EVENTS,
  CORE_EVENT_TOPICS,
  decodeCoreEvent,
  decodeImplementation,
  ERC1967_IMPLEMENTATION_SLOT,
  laneDiscoveryTopics,
  ownBackfillRequest,
  ownContractFilter,
  ownDeploymentConfig,
  type BackfillRequest,
  type BlockRef,
  type BootstrapSnapshot,
  type ChainCursor,
  type Checkpoint,
  type ContractLog,
  type DeploymentConfig,
  type Network,
} from "@lunarbase-lab/pmm-v2-client";
import { RpcError } from "./rpc/error.js";
import { DEFAULT_HTTP_RPC_LIMITS, boundedFetcher, validateHttpLimits, type HttpRpcLimits } from "./rpc/http_limits.js";
import { ChainVerification } from "./rpc/identity.js";
import { normalizeViemBlockRef, normalizeViemLog, parseHash, parseHexU64 } from "./rpc/log.js";

export { RpcError };
export { DEFAULT_HTTP_RPC_LIMITS, type HttpRpcLimits } from "./rpc/http_limits.js";
export { parseHexU64 };
export { parseHash, parseRpcLog } from "./rpc/log.js";

const BLOCK_TAGS = new Set<BlockTag>(["earliest", "finalized", "latest", "pending", "safe"]);
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
  /** Hard request/response and normalized backfill limits. */
  readonly limits: HttpRpcLimits;

  /** Creates a strict read-only JSON-RPC client with an injectable fetcher. */
  constructor(
    /** Validated HTTP JSON-RPC endpoint retained for diagnostics. */
    readonly endpoint: string,
    fetcher: typeof fetch = fetch,
    limits: Partial<HttpRpcLimits> = {},
  ) {
    const url = new URL(endpoint);
    if (url.protocol !== "http:" && url.protocol !== "https:")
      throw new RpcError("INVALID", "HTTP RPC URL must use http: or https:");
    this.limits = validateHttpLimits({ ...DEFAULT_HTTP_RPC_LIMITS, ...limits });
    this.strictFetcher = boundedFetcher(fetcher, this.limits);
    this.client = createPublicClient({
      batch: { multicall: false },
      ccipRead: false,
      transport: http(url.toString(), {
        batch: false,
        fetchFn: this.strictFetcher,
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

  /**
   * Resolves one exact block hash with parent linkage.
   *
   * Durable fork resolution uses this only after a parent discontinuity;
   * normal head and log ingestion performs no auxiliary HTTP lookup.
   */
  async blockRefByHash(blockHash: Hex.Hex, chainId: bigint, commitment: ChainCursor["commitment"]): Promise<BlockRef> {
    const expectedHash = parseHash(blockHash, "requested block hash");
    const block = await this.remote(() =>
      this.client.getBlock({ blockHash: expectedHash, includeTransactions: false }),
    );
    return normalizeViemBlockRef(block, expectedHash, chainId, commitment);
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
    const from = request.fromBlock;
    const to = request.toBlock;
    const filter = ownContractFilter(request.filter);
    if (from > to) throw new RpcError("INVALID", "log range starts after its end");
    const expectedAddress = addressKey(filter.address);
    const events = eventsForTopics(filter.topics);
    const pending = initialLogRanges(from, to, this.limits.maxBackfillPageBlocks).reverse();
    const logs: ContractLog[] = [];
    let retainedBytes = 0;
    while (pending.length > 0) {
      const [fromBlock, toBlock] = pending.pop()!;
      try {
        const chunk = await this.remote(() =>
          this.client.getLogs({
            address: filter.address,
            events: events.length === 0 ? undefined : events,
            fromBlock,
            toBlock,
            strict: false,
          }),
        );
        for (const value of chunk) {
          const log = normalizeViemLog(value, chainId, commitment);
          if (log.address !== expectedAddress)
            throw new RpcError("INVALID", `RPC log address mismatch: expected ${expectedAddress}, got ${log.address}`);
          const bytes = contractLogRetainedBytes(log);
          if (logs.length >= this.limits.maxBackfillLogs || bytes > this.limits.maxBackfillBytes - retainedBytes)
            throw new RpcError("LIMIT", "normalized backfill batch count or byte budget exceeded");
          logs.push(log);
          retainedBytes += bytes;
        }
      } catch (error) {
        if (fromBlock >= toBlock || !isLogRangeLimit(error)) throw error;
        const middle = fromBlock + (toBlock - fromBlock) / 2n;
        pending.push([middle + 1n, toBlock]);
        pending.push([fromBlock, middle]);
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
      const message = error instanceof Error ? error.message : String(error);
      if (/JSON-RPC request body exceeds|HTTP response (?:body|content-length) exceeds/i.test(message))
        throw new RpcError("LIMIT", message);
      throw new RpcError("TRANSPORT", message);
    }
  }
}

function initialLogRanges(fromBlock: bigint, toBlock: bigint, maxPageBlocks: bigint): Array<[bigint, bigint]> {
  const ranges: Array<[bigint, bigint]> = [];
  for (let start = fromBlock; start <= toBlock; start += maxPageBlocks) {
    const end = start + maxPageBlocks - 1n;
    ranges.push([start, end < toBlock ? end : toBlock]);
  }
  return ranges;
}

function isLogRangeLimit(error: unknown): boolean {
  if (error instanceof RpcError && error.code === "LIMIT") return /http response|content-length/i.test(error.message);
  const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
  return ["too many results", "response size", "block range", "query exceeds", "limit exceeded", "-32005"].some(
    (needle) => message.includes(needle),
  );
}

/** Canonical HTTP fallback for backfill, heads, and checkpoint validation. */
export class RpcHttpBackend {
  private readonly chainVerification = new ChainVerification();

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

  /** Revalidates the independent HTTP endpoint for every subscription. */
  verifyChainId(): Promise<void> {
    return this.chainVerification.verify(() => this.checkChainId());
  }

  private ensureChainId(): Promise<void> {
    return this.chainVerification.ensure(() => this.checkChainId());
  }

  private async checkChainId(): Promise<void> {
    const actual = await this.rpc.chainId();
    if (actual !== this.chainId)
      throw new RpcError("INVALID", `HTTP RPC chain id mismatch: expected ${this.chainId}, got ${actual}`);
  }

  /** Reads the block-tagged source head. */
  async canonicalHead(): Promise<ChainCursor> {
    await this.ensureChainId();
    return this.rpc.blockCursor(
      this.snapshotTag,
      this.chainId,
      this.snapshotTag === "finalized" ? CommitmentValue.Finalized : CommitmentValue.Canonical,
    );
  }

  /** Backfills canonical logs through one `eth_getLogs` request. */
  async backfill(request: BackfillRequest): Promise<readonly ContractLog[]> {
    const ownedRequest = ownBackfillRequest(request);
    await this.ensureChainId();
    return this.rpc.getLogs(ownedRequest, this.chainId, CommitmentValue.Canonical);
  }

  /** Resolves one exact block hash after verifying HTTP chain identity. */
  async blockRefByHash(blockHash: Hex.Hex, commitment: ChainCursor["commitment"]): Promise<BlockRef> {
    await this.ensureChainId();
    return this.rpc.blockRefByHash(blockHash, this.chainId, commitment);
  }

  /** Confirms checkpoint canonicality and the ERC-1967 implementation identity. */
  async validateCheckpoint(checkpoint: Checkpoint): Promise<boolean> {
    if (checkpoint.chainId !== this.chainId || checkpoint.cursor.chainId !== this.chainId) return false;
    await this.ensureChainId();
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

  /** Reads chain-wide quote state and optional verified-router accounting. */
  async snapshot(input: DeploymentConfig): Promise<BootstrapSnapshot> {
    const config = ownDeploymentConfig(input);
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
    const [cash, blacklistFeeMultiplier] = await Promise.all([
      this.rpc.client.readContract({
        abi: CORE_ABI,
        address: config.core,
        functionName: "cash",
        ...at,
      }),
      this.rpc.client.readContract({
        abi: CORE_ABI,
        address: config.core,
        functionName: "blacklistFeeMultiplier",
        ...at,
      }),
    ]);
    const lanes = new Map<Address, ReturnType<typeof createLaneState>>();
    const state: QuoteState = {
      cash,
      cashReserve: 0n,
      lanes,
      blacklistFeeMultiplier,
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

    let verifiedRouter: BootstrapSnapshot["verifiedRouter"];
    if (config.verifiedRouter !== undefined) {
      const whitelisted = await this.rpc.client.readContract({
        abi: CORE_ABI,
        address: config.core,
        functionName: "whitelist",
        args: [config.verifiedRouter],
        ...at,
      });
      if (whitelisted !== (config.feeClass === "Whitelisted"))
        throw new RpcError(
          "INVALID",
          `verified router fee class mismatch: expected ${config.feeClass}, got whitelisted=${whitelisted}`,
        );
      const partnerFeeBps = new Map<Address, number>();
      const partnerAssets = [...new Set([...assets, cash].map(addressKey))];
      for (let offset = 0; offset < partnerAssets.length; offset += SNAPSHOT_CONCURRENCY) {
        const entries = await Promise.all(
          partnerAssets.slice(offset, offset + SNAPSHOT_CONCURRENCY).map(async (asset) => {
            const partner = await this.rpc.client.readContract({
              abi: CORE_ABI,
              address: config.core,
              functionName: "partners",
              args: [config.verifiedRouter!, asset],
              ...at,
            });
            if (BigInt(partner[1]) > BPS) throw new RpcError("INVALID", "partner fee exceeds BPS");
            return [asset, partner[1]] as const;
          }),
        );
        for (const [asset, fee] of entries) partnerFeeBps.set(addressKey(asset), fee);
      }
      verifiedRouter = { router: config.verifiedRouter, partnerFeeBps };
    }
    const verified = await this.rpc.blockCursor(pinnedBlock, config.chainId, commitment);
    if (verified.blockHash?.toLowerCase() !== cursor.blockHash.toLowerCase())
      throw new RpcError("TRANSPORT", "snapshot block changed while state was reconstructed");
    return {
      state,
      cursor,
      implementation,
      implementationCodeHash,
      ...(verifiedRouter === undefined ? {} : { verifiedRouter }),
    };
  }

  private async resolveLaneAssets(config: DeploymentConfig, snapshotBlock: bigint): Promise<Address[]> {
    if (config.explicitLaneAssets.length > 0) return config.explicitLaneAssets.map(addressKey);
    const active = new Set<Address>();
    let pageStart = config.deploymentBlock;
    while (pageStart <= snapshotBlock) {
      const candidateEnd = pageStart + this.rpc.limits.maxBackfillPageBlocks - 1n;
      const pageEnd = candidateEnd < snapshotBlock ? candidateEnd : snapshotBlock;
      const history = await this.rpc.getLogs(
        {
          fromBlock: pageStart,
          toBlock: pageEnd,
          filter: { address: config.core, topics: laneDiscoveryTopics() },
        },
        config.chainId,
        CommitmentValue.Canonical,
      );
      history.sort((left, right) => compareCursor(left.cursor, right.cursor));
      for (const log of history) {
        const event = decodeCoreEvent(log);
        if (event?.kind === "LaneAdded") active.add(addressKey(event.asset));
        else if (event?.kind === "LaneRemoved") active.delete(addressKey(event.asset));
      }
      pageStart = pageEnd + 1n;
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
