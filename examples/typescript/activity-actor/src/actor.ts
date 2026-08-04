import { decodeLaneSlot0 } from "@lunarbase/math";
import {
  createPublicClient,
  createWalletClient,
  getAddress,
  http,
  keccak256,
  parseEventLogs,
  parseEther,
  parseGwei,
  zeroAddress,
  type Address,
  type Hash,
  type TransactionReceipt,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { bscTestnet } from "viem/chains";
import { CORE_ACTOR_ABI, MOCK_TOKEN_ABI } from "./abi.js";
import type { ActorConfig } from "./config.js";

const ERC1967_IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc" as const;

export type TokenId = "CASH" | "ASSET1" | "ASSET2";

export interface TokenSnapshot {
  readonly id: TokenId;
  readonly address: Address;
  readonly name: string;
  readonly symbol: string;
  readonly decimals: number;
  readonly mintAmount?: bigint;
  readonly actorBalance: bigint;
  readonly allowance: bigint;
  readonly assetReserve: bigint;
  readonly totalPrincipalAmount: bigint;
}

export interface LaneSnapshot {
  readonly asset: Address;
  readonly exists: boolean;
  readonly paused: boolean;
  readonly latestUpdateBlock: bigint;
  readonly blockDelay: number;
  readonly validThroughBlock: bigint;
  readonly fresh: boolean;
}

export interface PoolSnapshot {
  readonly blockNumber: bigint;
  readonly blockTimestamp: bigint;
  readonly implementation: Address;
  readonly globallyPaused: boolean;
  readonly actorGasBalance: bigint;
  readonly tokens: readonly TokenSnapshot[];
  readonly lanes: ReadonlyMap<Address, LaneSnapshot>;
}

export interface SubmittedTransaction {
  readonly hash: Hash;
  readonly blockNumber: bigint;
  readonly gasUsed: bigint;
}

export interface SubmittedSwapTransaction extends SubmittedTransaction {
  readonly amountIn: bigint;
  readonly amountOut: bigint;
}

export interface ObservedSwap {
  readonly transactionHash: Hash;
  readonly blockNumber: bigint;
  readonly transactionIndex: number;
  readonly logIndex: number;
  readonly assetIn: Address;
  readonly assetOut: Address;
  readonly amountIn: bigint;
  readonly amountOut: bigint;
}

/** A submission may have reached the RPC, so retrying could queue conflicting activity. */
export class TransactionOutcomeUnknownError extends Error {
  readonly hash?: Hash;

  constructor(hash: Hash | undefined, cause: unknown) {
    super(
      hash === undefined
        ? "transaction submission may have been broadcast but no hash was returned"
        : `transaction ${hash} was broadcast but its final outcome is unknown`,
      { cause },
    );
    this.name = "TransactionOutcomeUnknownError";
    this.hash = hash;
  }
}

/** A swap is confirmed, but its emitted accounting cannot be reconciled safely. */
export class ConfirmedSwapLogError extends Error {
  readonly hash: Hash;

  constructor(hash: Hash, detail: string, cause?: unknown) {
    super("confirmed swap " + hash + " has an invalid SwapExecuted log: " + detail, { cause });
    this.name = "ConfirmedSwapLogError";
    this.hash = hash;
  }
}

/** Creates one strictly sequential BSC Testnet actor around viem clients. */
export function createActor(config: ActorConfig, liveArgument = false) {
  const account = privateKeyToAccount(config.actorPrivateKey);
  const writesEnabled = config.broadcast && liveArgument;
  const transport = http(config.rpcUrl, { batch: false, retryCount: 0, timeout: 15_000 });
  const publicClient = createPublicClient({ chain: bscTestnet, batch: { multicall: false }, transport });
  const walletClient = createWalletClient({ account, chain: bscTestnet, transport });

  async function requireCode(address: Address, label: string, blockHash: Hash): Promise<void> {
    const code = await publicClient.getCode({ address, blockHash });
    if (code === undefined || code === "0x") throw new Error(`${label} has no runtime code at ${address}`);
  }

  async function requireDeploymentIdentity(blockHash: Hash): Promise<Address> {
    const [word, proxyCode] = await Promise.all([
      publicClient.getStorageAt({
        address: config.pool,
        slot: ERC1967_IMPLEMENTATION_SLOT,
        blockHash,
      }),
      publicClient.getCode({ address: config.pool, blockHash }),
    ]);
    if (word === undefined || !/^0x[0-9a-fA-F]{64}$/.test(word))
      throw new Error("Core has an invalid ERC-1967 implementation word");
    if (word.slice(2, 26) !== "0".repeat(24))
      throw new Error("Core ERC-1967 implementation word has non-zero high padding");

    const implementation = getAddress(`0x${word.slice(-40)}`);
    if (implementation === zeroAddress || implementation !== config.expectedImplementation)
      throw new Error(`Core implementation ${implementation} does not match the pinned deployment`);
    if (proxyCode === undefined || proxyCode === "0x") throw new Error("Core proxy has no runtime code");
    if (keccak256(proxyCode) !== config.expectedProxyCodeHash)
      throw new Error("Core proxy runtime hash does not match the pinned deployment");

    const implementationCode = await publicClient.getCode({ address: implementation, blockHash });
    if (implementationCode === undefined || implementationCode === "0x")
      throw new Error("Core implementation has no runtime code");
    if (keccak256(implementationCode) !== config.expectedImplementationCodeHash)
      throw new Error("Core implementation runtime hash does not match the pinned deployment");
    return implementation;
  }

  async function inspectToken(id: TokenId, address: Address, blockNumber: bigint): Promise<TokenSnapshot> {
    const [name, symbol, decimals, actorBalance, allowance, reserves] = await Promise.all([
      publicClient.readContract({ address, abi: MOCK_TOKEN_ABI, functionName: "name", blockNumber }),
      publicClient.readContract({ address, abi: MOCK_TOKEN_ABI, functionName: "symbol", blockNumber }),
      publicClient.readContract({ address, abi: MOCK_TOKEN_ABI, functionName: "decimals", blockNumber }),
      publicClient.readContract({
        address,
        abi: MOCK_TOKEN_ABI,
        functionName: "balanceOf",
        args: [account.address],
        blockNumber,
      }),
      publicClient.readContract({
        address,
        abi: MOCK_TOKEN_ABI,
        functionName: "allowance",
        args: [account.address, config.pool],
        blockNumber,
      }),
      publicClient.readContract({
        address: config.pool,
        abi: CORE_ACTOR_ABI,
        functionName: "reserves",
        args: [address],
        blockNumber,
      }),
    ]);
    let mintAmount: bigint | undefined;
    try {
      mintAmount = await publicClient.readContract({
        address,
        abi: MOCK_TOKEN_ABI,
        functionName: "MINT_AMOUNT",
        blockNumber,
      });
    } catch {
      mintAmount = undefined;
    }
    return {
      id,
      address,
      name,
      symbol,
      decimals,
      mintAmount,
      actorBalance,
      allowance,
      assetReserve: reserves[0],
      totalPrincipalAmount: reserves[4],
    };
  }

  async function inspectLane(asset: Address, blockNumber: bigint): Promise<LaneSnapshot> {
    const word = await publicClient.readContract({
      address: config.pool,
      abi: CORE_ACTOR_ABI,
      functionName: "lane",
      args: [asset],
      blockNumber,
    });
    const fields = decodeLaneSlot0(BigInt(word));
    const validThroughBlock = fields.latestUpdateBlock + BigInt(fields.blockDelay);
    return {
      asset,
      exists: fields.exists,
      paused: fields.paused,
      latestUpdateBlock: fields.latestUpdateBlock,
      blockDelay: fields.blockDelay,
      validThroughBlock,
      fresh: blockNumber + BigInt(config.minimumLaneHeadroomBlocks) <= validThroughBlock,
    };
  }

  async function inspect(): Promise<PoolSnapshot> {
    const [chainId, block] = await Promise.all([
      publicClient.getChainId(),
      publicClient.getBlock({ blockTag: "latest", includeTransactions: false }),
    ]);
    if (chainId !== config.chainId)
      throw new Error(`RPC chain id ${chainId} does not match expected ${config.chainId}`);
    if (block.hash === null) throw new Error("latest block has no hash");

    const [implementation] = await Promise.all([
      requireDeploymentIdentity(block.hash),
      requireCode(config.cash, "CASH", block.hash),
      requireCode(config.asset1, "ASSET1", block.hash),
      requireCode(config.asset2, "ASSET2", block.hash),
    ]);

    const [globallyPaused, onchainCash, actorGasBalance, tokens, laneEntries] = await Promise.all([
      publicClient.readContract({
        address: config.pool,
        abi: CORE_ACTOR_ABI,
        functionName: "paused",
        blockNumber: block.number,
      }),
      publicClient.readContract({
        address: config.pool,
        abi: CORE_ACTOR_ABI,
        functionName: "cash",
        blockNumber: block.number,
      }),
      publicClient.getBalance({ address: account.address, blockNumber: block.number }),
      Promise.all([
        inspectToken("CASH", config.cash, block.number),
        inspectToken("ASSET1", config.asset1, block.number),
        inspectToken("ASSET2", config.asset2, block.number),
      ]),
      Promise.all(
        [config.asset1, config.asset2].map(async (asset) => [asset, await inspectLane(asset, block.number)] as const),
      ),
    ]);
    if (getAddress(onchainCash) !== config.cash)
      throw new Error(`Core CASH ${onchainCash} does not match configured ${config.cash}`);

    return {
      blockNumber: block.number,
      blockTimestamp: block.timestamp,
      implementation,
      globallyPaused,
      actorGasBalance,
      tokens,
      lanes: new Map(laneEntries),
    };
  }

  async function assertNoPendingTransactions(): Promise<void> {
    const [latestNonce, pendingNonce] = await Promise.all([
      publicClient.getTransactionCount({ address: account.address, blockTag: "latest" }),
      publicClient.getTransactionCount({ address: account.address, blockTag: "pending" }),
    ]);
    if (latestNonce !== pendingNonce)
      throw new Error(`actor has unresolved transactions: latest nonce ${latestNonce}, pending nonce ${pendingNonce}`);
  }

  async function requireWritePreconditions(): Promise<bigint> {
    if (!writesEnabled) throw new Error("write gate is closed; BROADCAST=true and --live are both required");

    await assertNoPendingTransactions();
    const [chainId, block, gasBalance, gasPrice] = await Promise.all([
      publicClient.getChainId(),
      publicClient.getBlock({ blockTag: "latest", includeTransactions: false }),
      publicClient.getBalance({ address: account.address }),
      publicClient.getGasPrice(),
    ]);
    if (chainId !== config.chainId) throw new Error("RPC chain changed before write");
    if (block.hash === null) throw new Error("latest block has no hash before write");
    if (gasBalance < parseEther(config.minimumGasBalance))
      throw new Error("actor gas balance is below the configured write floor");
    if (gasPrice > parseGwei(config.maximumGasPriceGwei))
      throw new Error(`gas price ${gasPrice} exceeds the configured cap`);

    await requireDeploymentIdentity(block.hash);
    return gasPrice;
  }

  async function waitForSuccess(hash: Hash): Promise<TransactionReceipt> {
    let receipt: TransactionReceipt;
    try {
      receipt = await publicClient.waitForTransactionReceipt({
        hash,
        confirmations: config.confirmations,
        timeout: 120_000,
      });
    } catch (cause) {
      throw new TransactionOutcomeUnknownError(hash, cause);
    }
    if (receipt.status !== "success") throw new Error(`transaction ${hash} reverted`);
    return receipt;
  }

  async function submit(send: () => Promise<Hash>): Promise<TransactionReceipt> {
    let hash: Hash;
    try {
      hash = await send();
    } catch (cause) {
      throw new TransactionOutcomeUnknownError(undefined, cause);
    }
    return waitForSuccess(hash);
  }

  function submitted(receipt: TransactionReceipt): SubmittedTransaction {
    return {
      hash: receipt.transactionHash,
      blockNumber: receipt.blockNumber,
      gasUsed: receipt.gasUsed,
    };
  }

  async function mint(token: Address): Promise<SubmittedTransaction> {
    const gasPrice = await requireWritePreconditions();
    const { request } = await publicClient.simulateContract({
      account,
      address: token,
      abi: MOCK_TOKEN_ABI,
      functionName: "mint",
      args: [account.address],
      gasPrice,
    });
    return submitted(await submit(() => walletClient.writeContract(request)));
  }

  async function approve(token: Address, amount: bigint): Promise<SubmittedTransaction> {
    const gasPrice = await requireWritePreconditions();
    const { request } = await publicClient.simulateContract({
      account,
      address: token,
      abi: MOCK_TOKEN_ABI,
      functionName: "approve",
      args: [config.pool, amount],
      gasPrice,
    });
    return submitted(await submit(() => walletClient.writeContract(request)));
  }

  async function balance(token: Address): Promise<bigint> {
    return publicClient.readContract({
      address: token,
      abi: MOCK_TOKEN_ABI,
      functionName: "balanceOf",
      args: [account.address],
    });
  }

  async function allowance(token: Address): Promise<bigint> {
    return publicClient.readContract({
      address: token,
      abi: MOCK_TOKEN_ABI,
      functionName: "allowance",
      args: [account.address, config.pool],
    });
  }

  async function quoteExactIn(amountIn: bigint, assetIn: Address, assetOut: Address): Promise<bigint> {
    return publicClient.readContract({
      account: account.address,
      address: config.pool,
      abi: CORE_ACTOR_ABI,
      functionName: "quoteExactIn",
      args: [amountIn, assetIn, assetOut],
    });
  }

  async function swapExactIn(
    assetIn: Address,
    assetOut: Address,
    amountIn: bigint,
    amountOutMinimum: bigint,
    deadline: bigint,
  ): Promise<SubmittedSwapTransaction> {
    const gasPrice = await requireWritePreconditions();
    const { request } = await publicClient.simulateContract({
      account,
      address: config.pool,
      abi: CORE_ACTOR_ABI,
      functionName: "swapExactIn",
      args: [
        {
          assetIn,
          assetOut,
          recipient: account.address,
          amountIn,
          amountOutMinimum,
          deadline,
        },
      ],
      value: 0n,
      gasPrice,
    });
    const receipt = await submit(() => walletClient.writeContract(request));
    let swapLogs;
    try {
      swapLogs = parseEventLogs({
        abi: CORE_ACTOR_ABI,
        eventName: "SwapExecuted",
        logs: receipt.logs.filter((entry) => getAddress(entry.address) === config.pool),
        strict: true,
      }).filter(
        (entry) =>
          getAddress(entry.args.router) === account.address &&
          getAddress(entry.args.assetIn) === assetIn &&
          getAddress(entry.args.assetOut) === assetOut &&
          entry.args.exactIn &&
          entry.args.amountIn === amountIn,
      );
    } catch (cause) {
      throw new ConfirmedSwapLogError(receipt.transactionHash, "event decoding failed", cause);
    }
    if (swapLogs.length !== 1)
      throw new ConfirmedSwapLogError(receipt.transactionHash, "expected one matching event, found " + swapLogs.length);

    const actualAmountOut = swapLogs[0]!.args.amountOut;
    if (actualAmountOut <= 0n) throw new ConfirmedSwapLogError(receipt.transactionHash, "amountOut is not positive");
    return { ...submitted(receipt), amountIn, amountOut: actualAmountOut };
  }

  async function swapHistory(fromBlock: bigint): Promise<ObservedSwap[]> {
    if (fromBlock < 0n) throw new RangeError("pairing start block must be non-negative");
    const latestBlock = await publicClient.getBlockNumber();
    if (fromBlock > latestBlock) return [];

    const confirmationLag = BigInt(config.confirmations - 1);
    const confirmedTo = latestBlock >= confirmationLag ? latestBlock - confirmationLag : 0n;
    if (fromBlock > confirmedTo) throw new Error("pairing history begins inside the unconfirmed block tail");

    const anchorBefore = await publicClient.getBlock({ blockNumber: confirmedTo, includeTransactions: false });
    if (anchorBefore.hash === null) throw new Error("confirmed pairing-history anchor has no hash");

    const history: ObservedSwap[] = [];
    const chunkSize = 5_000n;
    for (let chunkStart = fromBlock; chunkStart <= confirmedTo; chunkStart += chunkSize) {
      const chunkEnd = chunkStart + chunkSize - 1n < confirmedTo ? chunkStart + chunkSize - 1n : confirmedTo;
      const events = await publicClient.getContractEvents({
        address: config.pool,
        abi: CORE_ACTOR_ABI,
        eventName: "SwapExecuted",
        args: { router: account.address },
        fromBlock: chunkStart,
        toBlock: chunkEnd,
        strict: true,
      });
      for (const event of events) {
        const { args, blockNumber, logIndex, transactionHash, transactionIndex } = event;
        if (
          blockNumber === null ||
          logIndex === null ||
          transactionHash === null ||
          transactionIndex === null ||
          args.assetIn === undefined ||
          args.assetOut === undefined ||
          args.amountIn === undefined ||
          args.amountOut === undefined ||
          args.exactIn !== true
        )
          throw new Error("actor SwapExecuted history contains an incomplete or non-exact-input event");
        history.push({
          transactionHash,
          blockNumber,
          transactionIndex,
          logIndex,
          assetIn: getAddress(args.assetIn),
          assetOut: getAddress(args.assetOut),
          amountIn: args.amountIn,
          amountOut: args.amountOut,
        });
      }
    }

    if (confirmedTo < latestBlock) {
      const unconfirmed = await publicClient.getContractEvents({
        address: config.pool,
        abi: CORE_ACTOR_ABI,
        eventName: "SwapExecuted",
        args: { router: account.address },
        fromBlock: confirmedTo + 1n,
        toBlock: latestBlock,
        strict: true,
      });
      if (unconfirmed.length > 0)
        throw new Error("actor has a SwapExecuted event awaiting the configured confirmations");
    }

    const anchorAfter = await publicClient.getBlock({ blockNumber: confirmedTo, includeTransactions: false });
    if (anchorAfter.hash === null || anchorAfter.hash !== anchorBefore.hash)
      throw new Error("pairing-history anchor changed while logs were being read");

    history.sort((left, right) =>
      left.blockNumber !== right.blockNumber
        ? left.blockNumber < right.blockNumber
          ? -1
          : 1
        : left.transactionIndex !== right.transactionIndex
          ? left.transactionIndex - right.transactionIndex
          : left.logIndex - right.logIndex,
    );
    return history;
  }

  return {
    account,
    publicClient,
    inspect,
    assertNoPendingTransactions,
    mint,
    approve,
    balance,
    allowance,
    quoteExactIn,
    swapExactIn,
    swapHistory,
  };
}

export type ActivityActor = ReturnType<typeof createActor>;
