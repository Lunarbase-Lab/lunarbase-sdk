import { parseAddress, type Address } from "@lunarbase/math";
import { z } from "zod";

const DEMO_ROUTER = "0x000000000000000000000000000000000000dead";
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
  CORE_ADDRESS: addressSchema,
  ROUTER_ADDRESS: addressSchema.optional(),
  EXPECT_WHITELISTED: z.stringbool().optional().default(false),
  DEPLOYMENT_BLOCK: z.coerce.bigint().nonnegative().optional().default(0n),
  QUOTE_AMOUNT: z.coerce.bigint().positive().optional().default(1_000_000_000_000_000_000n),
  QUOTE_INTERVAL_SECONDS: z.coerce.number().int().positive().optional().default(2),
});

export interface EnvironmentConfig {
  readonly rpcUrl: string;
  readonly wsUrl: string;
  readonly core: Address;
  readonly router: Address;
  readonly expectWhitelisted: boolean;
  readonly deploymentBlock: bigint;
  readonly quoteAmount: bigint;
  readonly quoteIntervalMilliseconds: number;
  readonly usesDemoRouter: boolean;
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
  const router = parsed.ROUTER_ADDRESS ?? parseAddress(DEMO_ROUTER);
  return {
    rpcUrl: parsed.RPC_URL,
    wsUrl: parsed.WS_URL ?? deriveWebSocketUrl(parsed.RPC_URL),
    core: parsed.CORE_ADDRESS,
    router,
    expectWhitelisted: parsed.EXPECT_WHITELISTED,
    deploymentBlock: parsed.DEPLOYMENT_BLOCK,
    quoteAmount: parsed.QUOTE_AMOUNT,
    quoteIntervalMilliseconds: parsed.QUOTE_INTERVAL_SECONDS * 1_000,
    usesDemoRouter: parsed.ROUTER_ADDRESS === undefined,
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
