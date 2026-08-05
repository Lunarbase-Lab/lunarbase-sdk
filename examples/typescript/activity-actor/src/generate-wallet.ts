import { open } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { generatePrivateKey, privateKeyToAccount } from "viem/accounts";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const environmentPath = resolve(packageRoot, ".env");

async function main(): Promise<void> {
  const privateKey = generatePrivateKey();
  const account = privateKeyToAccount(privateKey);
  const contents = `# Generated locally for BSC Testnet. Replace every deployment placeholder before use.
# Never reuse this key on mainnet.
RPC_URL=https://bsc-testnet-rpc.publicnode.com
RECEIPT_POLLING_MILLISECONDS=250
CHAIN_ID=97
CORE_ADDRESS=0x0000000000000000000000000000000000000001
CASH_ADDRESS=0x0000000000000000000000000000000000000002
ASSET1_ADDRESS=0x0000000000000000000000000000000000000003
ASSET2_ADDRESS=0x0000000000000000000000000000000000000004
PAIRING_START_BLOCK=0
PAIRING_MAX_REPLAY_BLOCKS=50000
EXPECTED_IMPLEMENTATION=0x0000000000000000000000000000000000000005
EXPECTED_IMPLEMENTATION_CODE_HASH=0x1111111111111111111111111111111111111111111111111111111111111111
EXPECTED_PROXY_CODE_HASH=0x2222222222222222222222222222222222222222222222222222222222222222
ACTOR_PRIVATE_KEY=${privateKey}
ACTOR_ADDRESS=${account.address}
BROADCAST=false
AUTO_MINT=true
ALLOWANCE_BATCH_SWAPS=1000
MIN_SWAP_AMOUNT=0.001
MAX_SWAP_AMOUNT=0.01
SLIPPAGE_PPM=5000
MAX_OUTPUT_RESERVE_PPM=1000
MAX_SESSION_OUTPUT_RESERVE_PPM=10000
MIN_LANE_HEADROOM_BLOCKS=2
MIN_DELAY_SECONDS=0
MAX_DELAY_SECONDS=0
RETRY_DELAY_SECONDS=2
DEADLINE_SECONDS=180
MIN_GAS_BALANCE_TBNB=0.01
MAX_GAS_PRICE_GWEI=1
MAX_SWAPS=50
CONFIRMATIONS=1
PAIRING_FINALITY_CONFIRMATIONS=3
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
