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
  CHAIN_ID: z.coerce.number().int().optional().default(97),
  POOL_ADDRESS: addressSchema.optional().default("0x11116c60551889C6c01DDAD3A1fB3Cc95CbeBBbB"),
  CASH_ADDRESS: addressSchema.optional().default("0x2c10647a0D96cab7fE26044CA6d3F854280dC906"),
  ASSET1_ADDRESS: addressSchema.optional().default("0x21f52a1d45DAb30b518b31CA8e44f91B588A8DEC"),
  ASSET2_ADDRESS: addressSchema.optional().default("0xcCE41dEACC72cd4C7b92358bf824eCA1f33Ec269"),
  PAIRING_START_BLOCK: z.coerce.bigint().min(0n).optional().default(123_101_134n),
  EXPECTED_IMPLEMENTATION: addressSchema.optional().default("0xCFa7de4418707d4FDC06e4634A4B2aE95Af528c7"),
  EXPECTED_IMPLEMENTATION_CODE_HASH: hashSchema
    .optional()
    .default("0xdd4f26f3b1ff31ea9aef19ddffd549ca8669c91fc4d0355e9677c6f5b2b96897"),
  EXPECTED_PROXY_CODE_HASH: hashSchema
    .optional()
    .default("0xf15a07c54ab3420101c38795fc919a27ffb05f1a0049070ba3b8f10bae32af97"),
  ACTOR_ADDRESS: addressSchema.optional(),
  BROADCAST: z.stringbool().optional().default(false),
  AUTO_MINT: z.stringbool().optional().default(true),
  MIN_SWAP_AMOUNT: decimalSchema.optional().default("0.001"),
  MAX_SWAP_AMOUNT: decimalSchema.optional().default("0.01"),
  SLIPPAGE_PPM: z.coerce.number().int().min(0).max(100_000).optional().default(5_000),
  MAX_OUTPUT_RESERVE_PPM: z.coerce.number().int().min(1).max(100_000).optional().default(1_000),
  MAX_SESSION_OUTPUT_RESERVE_PPM: z.coerce.number().int().min(1).max(100_000).optional().default(10_000),
  MIN_LANE_HEADROOM_BLOCKS: z.coerce.number().int().min(1).max(100).optional().default(2),
  MIN_DELAY_SECONDS: z.coerce.number().int().min(0).max(86_400).optional().default(20),
  MAX_DELAY_SECONDS: z.coerce.number().int().min(0).max(86_400).optional().default(90),
  RETRY_DELAY_SECONDS: z.coerce.number().int().min(1).max(86_400).optional().default(30),
  DEADLINE_SECONDS: z.coerce.number().int().min(30).max(3_600).optional().default(180),
  MIN_GAS_BALANCE_TBNB: decimalSchema.optional().default("0.01"),
  MAX_GAS_PRICE_GWEI: decimalSchema.optional().default("1"),
  MAX_SWAPS: z.coerce.number().int().min(2).max(100_000).optional().default(50),
  CONFIRMATIONS: z.coerce.number().int().min(1).max(12).optional().default(2),
  MAX_CONSECUTIVE_FAILURES: z.coerce.number().int().min(1).max(100).optional().default(5),
});

export interface ActorConfig {
  readonly rpcUrl: string;
  readonly chainId: number;
  readonly pool: Address;
  readonly cash: Address;
  readonly asset1: Address;
  readonly asset2: Address;
  readonly pairingStartBlock: bigint;
  readonly expectedImplementation: Address;
  readonly expectedImplementationCodeHash: Hex;
  readonly expectedProxyCodeHash: Hex;
  readonly actorPrivateKey: Hex;
  readonly expectedActorAddress?: Address;
  readonly broadcast: boolean;
  readonly autoMint: boolean;
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
    chainId: parsed.CHAIN_ID,
    pool: parsed.POOL_ADDRESS,
    cash: parsed.CASH_ADDRESS,
    asset1: parsed.ASSET1_ADDRESS,
    asset2: parsed.ASSET2_ADDRESS,
    pairingStartBlock: parsed.PAIRING_START_BLOCK,
    expectedImplementation: parsed.EXPECTED_IMPLEMENTATION,
    expectedImplementationCodeHash: parsed.EXPECTED_IMPLEMENTATION_CODE_HASH,
    expectedProxyCodeHash: parsed.EXPECTED_PROXY_CODE_HASH,
    actorPrivateKey: privateKey as Hex,
    expectedActorAddress: parsed.ACTOR_ADDRESS,
    broadcast: parsed.BROADCAST,
    autoMint: parsed.AUTO_MINT,
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
    maximumConsecutiveFailures: parsed.MAX_CONSECUTIVE_FAILURES,
  };
}
