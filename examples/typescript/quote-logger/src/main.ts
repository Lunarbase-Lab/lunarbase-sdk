import { connectBase } from "@lunarbase/client-base";
import {
  JsonRpcHttpClient,
  MATH_COMPATIBILITY_VERSION,
  Network,
  quoteCriticalTopics,
  type ClientConnectConfig,
  type ConnectedQuoteClient,
} from "@lunarbase/client-core";
import { laneExists } from "@lunarbase/math";
import * as Hex from "ox/Hex";
import { readEnvironment } from "./config.js";
import { logQuoteBatch, writeLog } from "./logging.js";
import { buildQuoteRequests } from "./quotes.js";

const ZERO_HASH = Hex.fromNumber(0, { size: 32 });

async function main(): Promise<void> {
  const environment = readEnvironment();
  const rpc = new JsonRpcHttpClient(environment.rpcUrl);
  const chainId = await rpc.chainId();
  const deployment = {
    network: Network.Base,
    chainId,
    core: environment.core,
    router: environment.router,
    expectWhitelisted: environment.expectWhitelisted,
    deploymentBlock: environment.deploymentBlock,
    expectedRuntimeCodeHash: ZERO_HASH,
    contractCompatibilityVersion: MATH_COMPATIBILITY_VERSION,
    httpRpcUrl: environment.rpcUrl,
    realtimeSource: environment.wsUrl,
    explicitLaneAssets: [],
  };
  const config: ClientConnectConfig = {
    deployment,
    filter: { address: environment.core, topics: quoteCriticalTopics() },
    queueBound: 4096,
    reconnectDelayMilliseconds: 1_000,
  };
  if (environment.usesDemoRouter)
    writeLog("warn", "ROUTER_ADDRESS is unset; using a non-whitelisted demonstration fee profile", {
      router: environment.router,
    });
  writeLog("info", "connecting LunarBase client", {
    chainId,
    network: Network.Base,
    core: environment.core,
    router: environment.router,
    rpcWs: environment.wsUrl,
  });

  const client = await connectBase(config);
  const checkpoint = client.checkpoint();
  if (checkpoint === undefined) {
    await client.shutdown();
    throw new Error("client returned no bootstrap checkpoint");
  }
  const lanes = [...checkpoint.state.lanes.entries()]
    .filter(([, lane]) => laneExists(lane))
    .map(([asset]) => asset)
    .sort();
  if (lanes.length === 0) {
    await client.shutdown();
    throw new Error("no active lanes discovered; verify CORE_ADDRESS and DEPLOYMENT_BLOCK");
  }
  const requests = buildQuoteRequests(checkpoint.state.cash, lanes, environment.quoteAmount);
  writeLog("info", "client ready", {
    cash: checkpoint.state.cash,
    lanes: lanes.length,
    quoteAmount: environment.quoteAmount,
  });

  logCurrentQuotes(client, requests);
  const timer = setInterval(() => logCurrentQuotes(client, requests), environment.quoteIntervalMilliseconds);
  await waitForShutdown(client, timer);
}

function logCurrentQuotes(
  client: ConnectedQuoteClient,
  requests: Parameters<ConnectedQuoteClient["quoteMany"]>[0],
): void {
  try {
    logQuoteBatch(requests, client.quoteMany(requests));
  } catch (error) {
    writeLog("warn", "quote batch unavailable", { detail: errorMessage(error) });
  }
}

function waitForShutdown(client: ConnectedQuoteClient, timer: NodeJS.Timeout): Promise<void> {
  return new Promise((resolve, reject) => {
    let stopping = false;
    const stop = (signal: NodeJS.Signals) => {
      if (stopping) return;
      stopping = true;
      clearInterval(timer);
      process.off("SIGINT", stop);
      process.off("SIGTERM", stop);
      writeLog("info", "shutdown requested", { signal });
      void client.shutdown().then(resolve, reject);
    };
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);
  });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

main().catch((error: unknown) => {
  writeLog("error", "quote logger failed", { detail: errorMessage(error) });
  process.exitCode = 1;
});
