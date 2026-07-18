import { BPS, createLaneState, type Address, type QuoteState } from "@lunarbase/math";
import type { AbiEvent as AbiEventType } from "ox/AbiEvent";
import * as Hash from "ox/Hash";
import * as Hex from "ox/Hex";
import { createPublicClient, formatLog, http, type BlockTag, type PublicClient, type RpcLog } from "viem";
import type {
  BackfillRequest,
  BootstrapSnapshot,
  ChainCursor,
  Checkpoint,
  ContractLog,
  DeploymentConfig,
  Network,
} from "../model.js";
import { Commitment as CommitmentValue } from "../model.js";
import { compareCursor } from "../source.js";
import { decodeCoreEvent, laneDiscoveryTopics } from "../protocol/abi.js";
import { CORE_ABI, CORE_EVENTS, CORE_EVENT_TOPICS } from "../protocol/core.js";

const BLOCK_TAGS = new Set<BlockTag>(["earliest", "finalized", "latest", "pending", "safe"]);

/** Typed failure from HTTP, JSON-RPC, or ABI response validation. */
export class RpcError extends Error {
  constructor(
    readonly code: "TRANSPORT" | "INVALID",
    message: string,
  ) {
    super(message);
    this.name = "RpcError";
  }
}

/**
 * Read-only viem HTTP client with every implicit network behavior disabled.
 *
 * One method call issues one JSON-RPC request: batching, retries, multicall,
 * and CCIP-read are disabled. Chain ID is read only through `chainId()`.
 */
export class JsonRpcHttpClient {
  readonly client: PublicClient;

  /** Creates a strict read-only JSON-RPC client with an injectable fetcher. */
  constructor(
    readonly endpoint: string,
    fetcher: typeof fetch = fetch,
  ) {
    const url = new URL(endpoint);
    if (url.protocol !== "http:" && url.protocol !== "https:")
      throw new RpcError("INVALID", "HTTP RPC URL must use http: or https:");
    this.client = createPublicClient({
      batch: { multicall: false },
      ccipRead: false,
      transport: http(url.toString(), {
        batch: false,
        fetchFn: fetcher,
        retryCount: 0,
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

  /** Converts one explicit `eth_getBlockByNumber` into a source cursor. */
  async blockCursor(blockTag: string, chainId: bigint, commitment: ChainCursor["commitment"]): Promise<ChainCursor> {
    const block = await this.remote(() =>
      this.client.getBlock({
        includeTransactions: false,
        ...blockParameters(blockTag),
      }),
    );
    if (block.number === null) throw new RpcError("INVALID", "eth_getBlockByNumber returned a pending block");
    const l1BlockNumber = (block as typeof block & { l1BlockNumber?: Hex.Hex }).l1BlockNumber;
    return {
      chainId,
      blockNumber: block.number,
      executionBlockNumber:
        l1BlockNumber === undefined ? block.number : parseHexU64(l1BlockNumber, "block.l1BlockNumber"),
      blockHash: block.hash ?? undefined,
      commitment,
    };
  }

  /** Fetches canonical logs with topic0 OR semantics and normalizes them. */
  async getLogs(
    request: BackfillRequest,
    chainId: bigint,
    commitment: ChainCursor["commitment"],
  ): Promise<ContractLog[]> {
    const events = eventsForTopics(request.filter.topics);
    const logs = await this.remote(() =>
      this.client.getLogs({
        address: request.filter.address,
        events,
        fromBlock: request.fromBlock,
        toBlock: request.toBlock,
        strict: false,
      }),
    );
    return logs.map((value) => normalizeViemLog(value, chainId, commitment));
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

/** Canonical HTTP fallback for backfill, heads, and checkpoint validation. */
export class RpcHttpBackend {
  constructor(
    readonly rpc: JsonRpcHttpClient,
    readonly network: Network,
    readonly chainId: bigint,
    readonly snapshotTag = "finalized",
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

  /** Confirms that a checkpoint block hash remains canonical. */
  async validateCheckpoint(checkpoint: Checkpoint): Promise<boolean> {
    const canonical = await this.rpc.blockCursor(
      Hex.fromNumber(checkpoint.cursor.blockNumber),
      this.chainId,
      CommitmentValue.Canonical,
    );
    return (
      canonical.blockHash !== undefined &&
      checkpoint.cursor.blockHash !== undefined &&
      canonical.blockHash === checkpoint.cursor.blockHash
    );
  }
}

/** Materializes all quote-critical Core state at one explicit block. */
export class RpcSnapshotProvider {
  constructor(
    readonly rpc: JsonRpcHttpClient,
    readonly snapshotTag = "finalized",
  ) {}

  /** Reads code, lanes, reserves, router policy, and the snapshot cursor. */
  async snapshot(config: DeploymentConfig): Promise<BootstrapSnapshot> {
    const commitment = this.snapshotTag === "finalized" ? CommitmentValue.Finalized : CommitmentValue.Canonical;
    const cursor = await this.rpc.blockCursor(this.snapshotTag, config.chainId, commitment);
    if (cursor.blockNumber < config.deploymentBlock)
      throw new RpcError("INVALID", "snapshot block precedes deployment block");

    const code = await this.rpc.getCode(config.core, this.snapshotTag);
    const runtimeCodeHash = Hash.keccak256(code);
    if (!isZeroHash(config.expectedRuntimeCodeHash) && runtimeCodeHash !== config.expectedRuntimeCodeHash)
      throw new RpcError("INVALID", "runtime code hash mismatch");

    const assets = await this.resolveLaneAssets(config, cursor.blockNumber);
    const at = blockParameters(this.snapshotTag);
    const cash = await this.rpc.client.readContract({
      abi: CORE_ABI,
      address: config.core,
      functionName: "cash",
      ...at,
    });
    const whitelisted = await this.rpc.client.readContract({
      abi: CORE_ABI,
      address: config.core,
      functionName: "whitelist",
      args: [config.router],
      ...at,
    });
    if (whitelisted !== config.expectWhitelisted)
      throw new RpcError(
        "INVALID",
        `configured router whitelist mismatch: expected ${config.expectWhitelisted}, got ${whitelisted}`,
      );
    const blacklistFeeMultiplier = await this.rpc.client.readContract({
      abi: CORE_ABI,
      address: config.core,
      functionName: "blacklistFeeMultiplier",
      ...at,
    });
    const lanes = new Map<Address, ReturnType<typeof createLaneState>>();
    const partnerFeeBps = new Map<Address, number>();
    const state: QuoteState = {
      cash,
      lanes,
      feeProfile: {
        whitelisted,
        blacklistFeeMultiplier,
        partnerFeeBps,
      },
    };

    for (const asset of assets) {
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
      lanes.set(asset, createLaneState(Hex.toBigInt(lane[0]), reserves[4], lane[4], lane[3], lane[1], lane[2]));
    }

    for (const asset of new Set([...assets, cash])) {
      const partner = await this.rpc.client.readContract({
        abi: CORE_ABI,
        address: config.core,
        functionName: "partners",
        args: [config.router, asset],
        ...at,
      });
      if (BigInt(partner[1]) > BPS) throw new RpcError("INVALID", "partner fee exceeds BPS");
      partnerFeeBps.set(asset, partner[1]);
    }
    return { state, cursor, runtimeCodeHash };
  }

  private async resolveLaneAssets(config: DeploymentConfig, snapshotBlock: bigint): Promise<Address[]> {
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
      if (event?.kind === "LaneAdded") active.add(event.asset);
      else if (event?.kind === "LaneRemoved") active.delete(event.asset);
    }
    if (config.explicitLaneAssets.length === 0) return [...active];
    if (config.explicitLaneAssets.some((asset) => !active.has(asset)))
      throw new RpcError("INVALID", "explicit lane asset was not active in deployment history");
    return [...config.explicitLaneAssets];
  }
}

/** Parses one raw JSON-RPC log through viem's audited formatter. */
export function parseRpcLog(value: unknown, chainId: bigint, commitment: ChainCursor["commitment"]): ContractLog {
  try {
    return normalizeViemLog(formatLog(value as RpcLog), chainId, commitment);
  } catch (error) {
    if (error instanceof RpcError) throw error;
    throw new RpcError("INVALID", error instanceof Error ? error.message : "invalid RPC log");
  }
}

function normalizeViemLog(
  log: ReturnType<typeof formatLog>,
  chainId: bigint,
  commitment: ChainCursor["commitment"],
): ContractLog {
  if (log.blockNumber === null || log.transactionIndex === null || log.logIndex === null)
    throw new RpcError("INVALID", "pending RPC log has no canonical position");
  return {
    address: log.address,
    topics: log.topics,
    data: log.data,
    removed: log.removed,
    cursor: {
      chainId,
      blockNumber: log.blockNumber,
      executionBlockNumber: log.blockNumber,
      blockHash: log.blockHash ?? undefined,
      transactionIndex: BigInt(log.transactionIndex),
      logIndex: BigInt(log.logIndex),
      commitment,
    },
  };
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

/** Parses an unsigned hexadecimal uint64 RPC field through Ox. */
export function parseHexU64(value: unknown, field: string): bigint {
  if (typeof value !== "string" || !Hex.validate(value)) throw new RpcError("INVALID", `${field} is not valid hex`);
  const result = Hex.toBigInt(value);
  if (result > (1n << 64n) - 1n) throw new RpcError("INVALID", `${field} exceeds uint64`);
  return result;
}

/** Parses a canonical 32-byte hash through Ox. */
export function parseHash(value: unknown, field: string): Hex.Hex {
  if (typeof value !== "string" || !Hash.validate(value)) throw new RpcError("INVALID", `${field} is not bytes32`);
  return value.toLowerCase() as Hex.Hex;
}

function isZeroHash(value: Hex.Hex): boolean {
  return Hash.validate(value) && Hex.toBigInt(value) === 0n;
}

/** Computes legacy Keccak-256 with Ox's audited noble implementation. */
export function keccak256Hex(input: Uint8Array | Hex.Hex): Hex.Hex {
  return Hash.keccak256(input, { as: "Hex" });
}
