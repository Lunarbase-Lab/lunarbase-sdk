import { open } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { generatePrivateKey, privateKeyToAccount } from "viem/accounts";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const environmentPath = resolve(packageRoot, ".env");

async function main(): Promise<void> {
  const privateKey = generatePrivateKey();
  const account = privateKeyToAccount(privateKey);
  const contents = `# Generated locally for BSC Testnet. Never reuse this key on mainnet.
RPC_URL=https://bsc-testnet-rpc.publicnode.com
CHAIN_ID=97
POOL_ADDRESS=0x11116c60551889C6c01DDAD3A1fB3Cc95CbeBBbB
CASH_ADDRESS=0x2c10647a0D96cab7fE26044CA6d3F854280dC906
ASSET1_ADDRESS=0x21f52a1d45DAb30b518b31CA8e44f91B588A8DEC
ASSET2_ADDRESS=0xcCE41dEACC72cd4C7b92358bf824eCA1f33Ec269
PAIRING_START_BLOCK=123101134
EXPECTED_IMPLEMENTATION=0xCFa7de4418707d4FDC06e4634A4B2aE95Af528c7
EXPECTED_IMPLEMENTATION_CODE_HASH=0xdd4f26f3b1ff31ea9aef19ddffd549ca8669c91fc4d0355e9677c6f5b2b96897
EXPECTED_PROXY_CODE_HASH=0xf15a07c54ab3420101c38795fc919a27ffb05f1a0049070ba3b8f10bae32af97
ACTOR_PRIVATE_KEY=${privateKey}
ACTOR_ADDRESS=${account.address}
BROADCAST=false
AUTO_MINT=true
MIN_SWAP_AMOUNT=0.001
MAX_SWAP_AMOUNT=0.01
SLIPPAGE_PPM=5000
MAX_OUTPUT_RESERVE_PPM=1000
MAX_SESSION_OUTPUT_RESERVE_PPM=10000
MIN_LANE_HEADROOM_BLOCKS=2
MIN_DELAY_SECONDS=20
MAX_DELAY_SECONDS=90
RETRY_DELAY_SECONDS=30
DEADLINE_SECONDS=180
MIN_GAS_BALANCE_TBNB=0.01
MAX_GAS_PRICE_GWEI=1
MAX_SWAPS=50
CONFIRMATIONS=2
MAX_CONSECUTIVE_FAILURES=5
`;

  let handle;
  try {
    handle = await open(environmentPath, "wx", 0o600);
    await handle.writeFile(contents, { encoding: "utf8" });
    await handle.sync();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST")
      throw new Error(`${environmentPath} already exists; refusing to overwrite the actor key`, { cause: error });
    throw error;
  } finally {
    await handle?.close();
  }

  process.stdout.write(`Actor address: ${account.address}\n`);
  process.stdout.write(`Private key saved only to ${environmentPath} with mode 0600.\n`);
}

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
