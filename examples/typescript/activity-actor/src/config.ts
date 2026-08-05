import { getAddress, type Address, type Hex } from "viem";
import { z } from "zod";

const PRIVATE_KEY = /^0x[0-9a-fA-F]{64}$/;
const BYTES32 = /^0x[0-9a-fA-F]{64}$/;
const DECIMAL = /^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/;

const addressSchema = z.string().transform((value, context): Address => {
  try {
    return getAddress(value);
  } catch {
    context.issues.push({ code: "custom", message: "must be a valid EVM address", input: value });
    return z.NEVER;
  }
});

const hashSchema = z
  .string()
  .regex(BYTES32, "must be a 0x-prefixed bytes32")
  .transform((value): Hex => value.toLowerCase() as Hex);
const decimalSchema = z.string().regex(DECIMAL, "must be a non-negative decimal");

const environmentSchema = z.object({
  RPC_URL: z.url().optional().default("https://bsc-testnet-rpc.publicnode.com"),
  RECEIPT_POLLING_MILLISECONDS: z.coerce.number().int().min(100).max(10_000).optional().default(250),
  CHAIN_ID: z.coerce.number().int().optional().default(97),
  CORE_ADDRESS: addressSchema,
  CASH_ADDRESS: addressSchema,
  ASSET1_ADDRESS: addressSchema,
  ASSET2_ADDRESS: addressSchema,
  PAIRING_START_BLOCK: z.coerce.bigint().min(0n),
  PAIRING_MAX_REPLAY_BLOCKS: z.coerce.number().int().min(1_000).max(1_000_000).optional().default(50_000),
  EXPECTED_IMPLEMENTATION: addressSchema,
  EXPECTED_IMPLEMENTATION_CODE_HASH: hashSchema,
  EXPECTED_PROXY_CODE_HASH: hashSchema,
  ACTOR_ADDRESS: addressSchema.optional(),
  BROADCAST: z.stringbool().optional().default(false),
  AUTO_MINT: z.stringbool().optional().default(true),
  ALLOWANCE_BATCH_SWAPS: z.coerce.number().int().min(2).max(100_000).optional().default(1_000),
  MIN_SWAP_AMOUNT: decimalSchema.optional().default("0.001"),
  MAX_SWAP_AMOUNT: decimalSchema.optional().default("0.01"),
  SLIPPAGE_PPM: z.coerce.number().int().min(0).max(100_000).optional().default(5_000),
  MAX_OUTPUT_RESERVE_PPM: z.coerce.number().int().min(1).max(100_000).optional().default(1_000),
  MAX_SESSION_OUTPUT_RESERVE_PPM: z.coerce.number().int().min(1).max(100_000).optional().default(10_000),
  MIN_LANE_HEADROOM_BLOCKS: z.coerce.number().int().min(1).max(100).optional().default(2),
  MIN_DELAY_SECONDS: z.coerce.number().int().min(0).max(86_400).optional().default(0),
  MAX_DELAY_SECONDS: z.coerce.number().int().min(0).max(86_400).optional().default(0),
  RETRY_DELAY_SECONDS: z.coerce.number().int().min(1).max(86_400).optional().default(2),
  DEADLINE_SECONDS: z.coerce.number().int().min(30).max(3_600).optional().default(180),
  MIN_GAS_BALANCE_TBNB: decimalSchema.optional().default("0.01"),
  MAX_GAS_PRICE_GWEI: decimalSchema.optional().default("1"),
  MAX_SWAPS: z.coerce.number().int().min(2).max(100_000).optional().default(50),
  CONFIRMATIONS: z.coerce.number().int().min(1).max(12).optional().default(1),
  PAIRING_FINALITY_CONFIRMATIONS: z.coerce.number().int().min(2).max(64).optional().default(3),
  MAX_CONSECUTIVE_FAILURES: z.coerce.number().int().min(1).max(100).optional().default(5),
});

export interface ActorConfig {
  readonly rpcUrl: string;
  readonly receiptPollingMilliseconds: number;
  readonly chainId: number;
  readonly core: Address;
  readonly cash: Address;
  readonly asset1: Address;
  readonly asset2: Address;
  readonly pairingStartBlock: bigint;
  readonly pairingMaximumReplayBlocks: number;
  readonly expectedImplementation: Address;
  readonly expectedImplementationCodeHash: Hex;
  readonly expectedProxyCodeHash: Hex;
  readonly actorPrivateKey: Hex;
  readonly expectedActorAddress?: Address;
  readonly broadcast: boolean;
  readonly autoMint: boolean;
  readonly allowanceBatchSwaps: number;
  readonly minimumSwapAmount: string;
  readonly maximumSwapAmount: string;
  readonly slippagePpm: number;
  readonly maximumOutputReservePpm: number;
  readonly maximumSessionOutputReservePpm: number;
  readonly minimumLaneHeadroomBlocks: number;
  readonly minimumDelaySeconds: number;
  readonly maximumDelaySeconds: number;
  readonly retryDelaySeconds: number;
  readonly deadlineSeconds: number;
  readonly minimumGasBalance: string;
  readonly maximumGasPriceGwei: string;
  readonly maximumSwaps: number;
  readonly confirmations: number;
  readonly pairingFinalityConfirmations: number;
  readonly maximumConsecutiveFailures: number;
}

type Environment = Readonly<Record<string, string | undefined>>;

/** Reads public settings separately from the actor secret so validation never echoes it. */
export function readConfig(environment: Environment = process.env): ActorConfig {
  const privateKey = environment.ACTOR_PRIVATE_KEY;
  if (privateKey === undefined || !PRIVATE_KEY.test(privateKey))
    throw new Error("ACTOR_PRIVATE_KEY must contain exactly 32 testnet-only bytes encoded as 0x-prefixed hex");

  const parsed = environmentSchema.parse(environment);
  if (parsed.CHAIN_ID !== 97) throw new Error("activity actor is locked to BSC Testnet chain id 97");
  if (parsed.MIN_DELAY_SECONDS > parsed.MAX_DELAY_SECONDS)
    throw new Error("MIN_DELAY_SECONDS must not exceed MAX_DELAY_SECONDS");

  return {
    rpcUrl: parsed.RPC_URL,
    receiptPollingMilliseconds: parsed.RECEIPT_POLLING_MILLISECONDS,
    chainId: parsed.CHAIN_ID,
    core: parsed.CORE_ADDRESS,
    cash: parsed.CASH_ADDRESS,
    asset1: parsed.ASSET1_ADDRESS,
    asset2: parsed.ASSET2_ADDRESS,
    pairingStartBlock: parsed.PAIRING_START_BLOCK,
    pairingMaximumReplayBlocks: parsed.PAIRING_MAX_REPLAY_BLOCKS,
    expectedImplementation: parsed.EXPECTED_IMPLEMENTATION,
    expectedImplementationCodeHash: parsed.EXPECTED_IMPLEMENTATION_CODE_HASH,
    expectedProxyCodeHash: parsed.EXPECTED_PROXY_CODE_HASH,
    actorPrivateKey: privateKey as Hex,
    expectedActorAddress: parsed.ACTOR_ADDRESS,
    broadcast: parsed.BROADCAST,
    autoMint: parsed.AUTO_MINT,
    allowanceBatchSwaps: parsed.ALLOWANCE_BATCH_SWAPS,
    minimumSwapAmount: parsed.MIN_SWAP_AMOUNT,
    maximumSwapAmount: parsed.MAX_SWAP_AMOUNT,
    slippagePpm: parsed.SLIPPAGE_PPM,
    maximumOutputReservePpm: parsed.MAX_OUTPUT_RESERVE_PPM,
    maximumSessionOutputReservePpm: parsed.MAX_SESSION_OUTPUT_RESERVE_PPM,
    minimumLaneHeadroomBlocks: parsed.MIN_LANE_HEADROOM_BLOCKS,
    minimumDelaySeconds: parsed.MIN_DELAY_SECONDS,
    maximumDelaySeconds: parsed.MAX_DELAY_SECONDS,
    retryDelaySeconds: parsed.RETRY_DELAY_SECONDS,
    deadlineSeconds: parsed.DEADLINE_SECONDS,
    minimumGasBalance: parsed.MIN_GAS_BALANCE_TBNB,
    maximumGasPriceGwei: parsed.MAX_GAS_PRICE_GWEI,
    maximumSwaps: parsed.MAX_SWAPS,
    confirmations: parsed.CONFIRMATIONS,
    pairingFinalityConfirmations: parsed.PAIRING_FINALITY_CONFIRMATIONS,
    maximumConsecutiveFailures: parsed.MAX_CONSECUTIVE_FAILURES,
  };
}
