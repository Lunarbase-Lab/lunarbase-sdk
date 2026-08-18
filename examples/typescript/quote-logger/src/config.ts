import { parseAddress, type Address, type FeeClass } from "@lunarbase-lab/pmm-v2-math";
import { z } from "zod";

const addressSchema = z.string().transform((value, context): Address => {
  try {
    return parseAddress(value);
  } catch {
    context.issues.push({ code: "custom", message: "must be a valid EVM address", input: value });
    return z.NEVER;
  }
});
const environmentSchema = z.object({
  RPC_URL: z.url(),
  WS_URL: z.url().optional(),
  SOURCE_PROFILE: z.enum(["evm", "base-flashblocks"]).optional().default("evm"),
  CORE_ADDRESS: addressSchema,
  FEE_CLASS: z.enum(["whitelisted", "non-whitelisted"]),
  VERIFIED_ROUTER_ADDRESS: addressSchema.optional(),
  DEPLOYMENT_BLOCK: z.coerce.bigint().nonnegative().optional().default(0n),
  LANE_ASSETS: z.string().optional(),
  QUOTE_AMOUNT: z.coerce.bigint().positive().optional().default(1_000_000_000_000_000_000n),
  QUOTE_INTERVAL_SECONDS: z.coerce.number().int().positive().optional().default(2),
});

export interface EnvironmentConfig {
  readonly rpcUrl: string;
  readonly wsUrl: string;
  readonly sourceProfile: "evm" | "base-flashblocks";
  readonly core: Address;
  readonly feeClass: FeeClass;
  readonly verifiedRouter: Address | undefined;
  readonly deploymentBlock: bigint;
  readonly explicitLaneAssets: readonly Address[];
  readonly quoteAmount: bigint;
  readonly quoteIntervalMilliseconds: number;
}

type Environment = Readonly<Record<string, string | undefined>>;

/**
 * Reads and validates the quote logger environment with Zod.
 *
 * Only the HTTP RPC endpoint and Core address are mandatory. All numeric
 * widths, booleans, URLs, and addresses are validated before the client starts.
 */
export function readEnvironment(environment: Environment = process.env): EnvironmentConfig {
  const parsed = environmentSchema.parse(environment);
  return {
    rpcUrl: parsed.RPC_URL,
    wsUrl: parsed.WS_URL ?? deriveWebSocketUrl(parsed.RPC_URL),
    sourceProfile: parsed.SOURCE_PROFILE,
    core: parsed.CORE_ADDRESS,
    feeClass: parsed.FEE_CLASS === "whitelisted" ? "Whitelisted" : "NonWhitelisted",
    verifiedRouter: parsed.VERIFIED_ROUTER_ADDRESS,
    deploymentBlock: parsed.DEPLOYMENT_BLOCK,
    explicitLaneAssets:
      parsed.LANE_ASSETS === undefined ? [] : parsed.LANE_ASSETS.split(",").map((value) => parseAddress(value.trim())),
    quoteAmount: parsed.QUOTE_AMOUNT,
    quoteIntervalMilliseconds: parsed.QUOTE_INTERVAL_SECONDS * 1_000,
  };
}

/** Derives a WebSocket URL through the platform URL implementation. */
export function deriveWebSocketUrl(rpcUrl: string): string {
  const url = new URL(rpcUrl);
  if (url.protocol === "https:") url.protocol = "wss:";
  else if (url.protocol === "http:") url.protocol = "ws:";
  else throw new Error("RPC_URL must use http: or https:");
  return url.toString();
}
