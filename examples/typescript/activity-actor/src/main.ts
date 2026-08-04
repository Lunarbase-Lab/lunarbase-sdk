import { formatEther, formatUnits, getAddress, parseEther, parseUnits, type Address } from "viem";
import {
  ConfirmedSwapLogError,
  createActor,
  TransactionOutcomeUnknownError,
  type ActivityActor,
  type PoolSnapshot,
  type TokenSnapshot,
} from "./actor.js";
import { readConfig, type ActorConfig } from "./config.js";
import { errorMessage, log } from "./logger.js";
import { acquireProcessLock } from "./process-lock.js";
import {
  directedPairs,
  findSafeReturnAmount,
  isWithinReserveCap,
  minimumOutput,
  randomBigIntInclusive,
  randomDelayMilliseconds,
  PairedSwapPlan,
  SessionReserveBudget,
  shuffled,
} from "./strategy.js";

interface Options {
  readonly inspect: boolean;
  readonly once: boolean;
  readonly live: boolean;
}

interface Candidate {
  readonly leg: "opening" | "return";
  readonly allowMint: boolean;
  readonly input: TokenSnapshot;
  readonly output: TokenSnapshot;
  readonly amountIn: bigint;
  readonly quotedOutput: bigint;
  readonly minimumOutput: bigint;
}

class ConfirmedSwapStateError extends Error {
  readonly hash: `0x${string}`;

  constructor(hash: `0x${string}`, cause: unknown) {
    super("confirmed swap " + hash + " could not be applied to local pairing or budget state", { cause });
    this.name = "ConfirmedSwapStateError";
    this.hash = hash;
  }
}

function readOptions(args: readonly string[]): Options {
  const supported = new Set(["--inspect", "--once", "--live"]);
  const unknown = args.filter((value) => !supported.has(value));
  if (unknown.length > 0) throw new Error(`unknown arguments: ${unknown.join(", ")}`);
  return {
    inspect: args.includes("--inspect"),
    once: args.includes("--once"),
    live: args.includes("--live"),
  };
}

function laneReady(snapshot: PoolSnapshot, token: TokenSnapshot, cash: Address): boolean {
  if (token.address === cash) return true;
  const lane = snapshot.lanes.get(token.address);
  return lane !== undefined && lane.exists && !lane.paused && lane.fresh && token.totalPrincipalAmount > 0n;
}

function routeReady(snapshot: PoolSnapshot, input: TokenSnapshot, output: TokenSnapshot, cash: Address): boolean {
  return (
    !snapshot.globallyPaused &&
    laneReady(snapshot, input, cash) &&
    laneReady(snapshot, output, cash) &&
    output.assetReserve > 0n
  );
}

function amountRange(config: ActorConfig, token: TokenSnapshot): readonly [bigint, bigint] {
  const minimum = parseUnits(config.minimumSwapAmount, token.decimals);
  const maximum = parseUnits(config.maximumSwapAmount, token.decimals);
  if (minimum <= 0n) throw new Error("MIN_SWAP_AMOUNT must be positive at token precision");
  if (maximum < minimum) throw new Error("MAX_SWAP_AMOUNT must not be below MIN_SWAP_AMOUNT");
  return [minimum, maximum];
}

async function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (milliseconds <= 0 || signal.aborted) return;
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, milliseconds);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

function logSnapshot(snapshot: PoolSnapshot, actor: Address): void {
  log("info", "pool_snapshot", {
    actor,
    implementation: snapshot.implementation,
    blockNumber: snapshot.blockNumber,
    globallyPaused: snapshot.globallyPaused,
    actorGasTbnb: formatEther(snapshot.actorGasBalance),
    tokens: snapshot.tokens.map((token) => ({
      id: token.id,
      address: token.address,
      symbol: token.symbol,
      decimals: token.decimals,
      mintable: token.mintAmount !== undefined,
      actorBalance: formatUnits(token.actorBalance, token.decimals),
      reserve: formatUnits(token.assetReserve, token.decimals),
      totalPrincipal: formatUnits(token.totalPrincipalAmount, token.decimals),
      lane:
        token.id === "CASH"
          ? undefined
          : (() => {
              const lane = snapshot.lanes.get(token.address);
              return lane === undefined
                ? undefined
                : {
                    exists: lane.exists,
                    paused: lane.paused,
                    fresh: lane.fresh,
                    latestUpdateBlock: lane.latestUpdateBlock,
                    blockDelay: lane.blockDelay,
                    validThroughBlock: lane.validThroughBlock,
                  };
            })(),
    })),
  });
}

function observeReserves(snapshot: PoolSnapshot, budget: SessionReserveBudget<Address>): void {
  for (const token of snapshot.tokens) budget.observe(token.address, token.assetReserve);
}

async function restorePairingPlan(
  config: ActorConfig,
  actor: ActivityActor,
): Promise<{ plan: PairedSwapPlan<Address> | undefined; eventCount: number }> {
  const supported = new Set<Address>([config.cash, config.asset1, config.asset2]);
  const history = await actor.swapHistory(config.pairingStartBlock);
  let plan: PairedSwapPlan<Address> | undefined;

  for (const event of history) {
    if (!supported.has(event.assetIn) || !supported.has(event.assetOut) || event.assetIn === event.assetOut)
      throw new Error("pairing history contains an unsupported or self-directed swap");
    if (event.amountIn <= 0n || event.amountOut <= 0n)
      throw new Error("pairing history contains a non-positive swap amount");

    if (plan === undefined) {
      plan = new PairedSwapPlan({ assetIn: event.assetIn, assetOut: event.assetOut });
      plan.recordConfirmed(event.assetIn, event.assetOut, event.amountOut);
      continue;
    }

    const pending = plan.pendingReturn;
    if (pending === undefined || event.amountIn > pending.maximumAmountIn)
      throw new Error("pairing history return exceeds its opening-leg output");
    plan.recordConfirmed(event.assetIn, event.assetOut, event.amountOut);
    plan = undefined;
  }
  return { plan, eventCount: history.length };
}

function completeCycleSwapLimit(maximumSwaps: number, startsWithReturn: boolean): number {
  const requiredParity = startsWithReturn ? 1 : 0;
  const limit = maximumSwaps % 2 === requiredParity ? maximumSwaps : maximumSwaps - 1;
  if (limit <= 0) throw new Error("MAX_SWAPS is too small to finish the current paired-swap cycle");
  return limit;
}

function advancePairingPlan(
  plan: PairedSwapPlan<Address> | undefined,
  candidate: Candidate,
  actualAmountOut: bigint,
): PairedSwapPlan<Address> | undefined {
  if (candidate.leg === "opening") {
    if (plan !== undefined) throw new Error("opening leg confirmed while a return leg was pending");
    const opened = new PairedSwapPlan({ assetIn: candidate.input.address, assetOut: candidate.output.address });
    opened.recordConfirmed(candidate.input.address, candidate.output.address, actualAmountOut);
    return opened;
  }

  const pending = plan?.pendingReturn;
  if (plan === undefined || pending === undefined || candidate.amountIn > pending.maximumAmountIn)
    throw new Error("return leg confirmed without a matching opening leg");
  plan.recordConfirmed(candidate.input.address, candidate.output.address, actualAmountOut);
  if (plan.pendingReturn !== undefined) throw new Error("confirmed return did not close the paired-swap cycle");
  return undefined;
}

async function main(): Promise<void> {
  const options = readOptions(process.argv.slice(2));
  const config = readConfig();
  const live = config.broadcast && options.live;
  const actor = createActor(config, options.live);
  if (config.expectedActorAddress !== undefined && getAddress(config.expectedActorAddress) !== actor.account.address)
    throw new Error("ACTOR_ADDRESS does not match the address derived from ACTOR_PRIVATE_KEY");

  if (config.broadcast !== options.live)
    log("warn", "broadcast_gate_closed", {
      broadcastEnvironment: config.broadcast,
      liveArgument: options.live,
      required: "BROADCAST=true and --live",
    });

  const controller = new AbortController();
  const stop = (signal: NodeJS.Signals) => {
    log("info", "shutdown_requested", { signal });
    controller.abort();
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);

  const releaseLock = live ? await acquireProcessLock(actor.account.address) : async () => {};
  try {
    if (live) await actor.assertNoPendingTransactions();

    let snapshot = await actor.inspect();
    logSnapshot(snapshot, actor.account.address);
    if (options.inspect) return;

    const recoveredPairing = await restorePairingPlan(config, actor);
    let pairingPlan = recoveredPairing.plan;
    const runSwapLimit = completeCycleSwapLimit(config.maximumSwaps, pairingPlan !== undefined);
    const recoveredReturn = pairingPlan?.pendingReturn;
    log("info", "pairing_state_recovered", {
      pairingStartBlock: config.pairingStartBlock,
      eventCount: recoveredPairing.eventCount,
      phase: recoveredReturn === undefined ? "opening" : "return",
      requiredAssetIn: recoveredReturn?.assetIn,
      requiredAssetOut: recoveredReturn?.assetOut,
      maximumReturnAmountIn: recoveredReturn?.maximumAmountIn,
      runSwapLimit,
    });

    const outputBudget = new SessionReserveBudget<Address>(config.maximumSessionOutputReservePpm);
    observeReserves(snapshot, outputBudget);

    let successfulSwaps = 0;
    let consecutiveFailures = 0;
    while (!controller.signal.aborted) {
      let completedIteration = false;
      try {
        snapshot = await actor.inspect();
        observeReserves(snapshot, outputBudget);
        const candidate = await selectCandidate(config, actor, snapshot, outputBudget, pairingPlan);
        if (candidate === undefined) {
          log("warn", "pool_not_ready", {
            blockNumber: snapshot.blockNumber,
            reason: "no route satisfies lane, principal, reserve, balance/mint, actor quote, and session-budget guards",
          });
        } else if (!live) {
          log("info", "dry_run_swap", {
            ...tradeLog(candidate),
            ...budgetLog(outputBudget, candidate.output),
          });
          successfulSwaps += 1;
        } else if (snapshot.actorGasBalance < parseEther(config.minimumGasBalance)) {
          log("error", "gas_balance_below_guard", {
            actualTbnb: formatEther(snapshot.actorGasBalance),
            requiredTbnb: config.minimumGasBalance,
          });
          controller.abort();
        } else {
          await prepareInput(config, actor, candidate);
          const refreshedSnapshot = await actor.inspect();
          observeReserves(refreshedSnapshot, outputBudget);
          const refreshedInput = refreshedSnapshot.tokens.find((token) => token.address === candidate.input.address);
          const refreshedOutput = refreshedSnapshot.tokens.find((token) => token.address === candidate.output.address);
          if (refreshedInput === undefined || refreshedOutput === undefined)
            throw new Error("configured trade token disappeared from the refreshed snapshot");
          if (!routeReady(refreshedSnapshot, refreshedInput, refreshedOutput, config.cash))
            throw new Error("route became unavailable while preparing actor balance or allowance");
          if (refreshedSnapshot.actorGasBalance < parseEther(config.minimumGasBalance))
            throw new Error("gas balance fell below the configured guard while preparing the trade");

          const refreshedQuote = await actor.quoteExactIn(
            candidate.amountIn,
            candidate.input.address,
            candidate.output.address,
          );
          if (!isWithinReserveCap(refreshedQuote, refreshedOutput.assetReserve, config.maximumOutputReservePpm))
            throw new Error("refreshed quote is zero or exceeds the configured output-reserve cap");
          if (!outputBudget.allows(refreshedOutput.address, refreshedQuote))
            throw new Error("refreshed quote exceeds the cumulative session output budget");
          const refreshedMinimum = minimumOutput(refreshedQuote, config.slippagePpm);
          if (refreshedMinimum === 0n) throw new Error("slippage minimum rounded to zero");

          const transaction = await actor.swapExactIn(
            candidate.input.address,
            candidate.output.address,
            candidate.amountIn,
            refreshedMinimum,
            refreshedSnapshot.blockTimestamp + BigInt(config.deadlineSeconds),
          );
          try {
            if (
              !isWithinReserveCap(transaction.amountOut, refreshedOutput.assetReserve, config.maximumOutputReservePpm)
            )
              throw new Error("confirmed output exceeds the configured output-reserve cap");
            outputBudget.record(refreshedOutput.address, transaction.amountOut);
            pairingPlan = advancePairingPlan(pairingPlan, candidate, transaction.amountOut);
          } catch (cause) {
            throw new ConfirmedSwapStateError(transaction.hash, cause);
          }
          log("info", "swap_confirmed", {
            ...tradeLog({ ...candidate, quotedOutput: refreshedQuote, minimumOutput: refreshedMinimum }),
            actualOutput: formatUnits(transaction.amountOut, refreshedOutput.decimals),
            nextLeg: pairingPlan === undefined ? "opening" : "return",
            ...budgetLog(outputBudget, refreshedOutput),
            hash: transaction.hash,
            blockNumber: transaction.blockNumber,
            gasUsed: transaction.gasUsed,
          });
          successfulSwaps += 1;
        }
        consecutiveFailures = 0;
        completedIteration = true;
      } catch (error) {
        if (error instanceof ConfirmedSwapLogError || error instanceof ConfirmedSwapStateError) {
          log("error", "confirmed_swap_reconciliation_failed", {
            hash: error.hash,
            detail: errorMessage(error),
            action: "writes halted; restart only after on-chain pairing history has been reconciled",
          });
          controller.abort();
          continue;
        }

        if (error instanceof TransactionOutcomeUnknownError) {
          log("error", "transaction_outcome_unknown", {
            hash: error.hash,
            detail:
              error.hash === undefined
                ? "writes halted; the RPC may have accepted the submission, so reconcile the actor pending nonce"
                : "writes halted; reconcile this hash and actor pending nonce before restart",
          });
          controller.abort();
          continue;
        }

        consecutiveFailures += 1;
        log("error", "activity_iteration_failed", {
          failures: consecutiveFailures,
          detail: errorMessage(error),
        });
        if (consecutiveFailures >= config.maximumConsecutiveFailures) {
          log("error", "circuit_breaker_open", { failures: consecutiveFailures });
          controller.abort();
        } else {
          await sleep(config.retryDelaySeconds * 1_000, controller.signal);
        }
      }

      if (options.once || successfulSwaps >= runSwapLimit) break;
      if (completedIteration)
        await sleep(randomDelayMilliseconds(config.minimumDelaySeconds, config.maximumDelaySeconds), controller.signal);
    }
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
    await releaseLock();
  }
}

async function selectCandidate(
  config: ActorConfig,
  actor: ActivityActor,
  snapshot: PoolSnapshot,
  outputBudget: SessionReserveBudget<Address>,
  pairingPlan: PairedSwapPlan<Address> | undefined,
): Promise<Candidate | undefined> {
  if (pairingPlan !== undefined) {
    const pending = pairingPlan.pendingReturn;
    if (pending === undefined) throw new Error("pairing plan exists without a pending return");
    const input = snapshot.tokens.find((token) => token.address === pending.assetIn);
    const output = snapshot.tokens.find((token) => token.address === pending.assetOut);
    if (input === undefined || output === undefined)
      throw new Error("pending return references a token missing from the pool snapshot");
    if (!routeReady(snapshot, input, output, config.cash)) return undefined;

    const maximumAmountIn = input.actorBalance < pending.maximumAmountIn ? input.actorBalance : pending.maximumAmountIn;
    const safe = await findSafeReturnAmount(
      maximumAmountIn,
      (amountIn) => actor.quoteExactIn(amountIn, input.address, output.address),
      (quotedOutput) =>
        isWithinReserveCap(quotedOutput, output.assetReserve, config.maximumOutputReservePpm) &&
        outputBudget.allows(output.address, quotedOutput),
    );
    if (safe === undefined) return undefined;
    const boundedMinimum = minimumOutput(safe.quotedOutput, config.slippagePpm);
    if (boundedMinimum === 0n) return undefined;
    return {
      leg: "return",
      allowMint: false,
      input,
      output,
      amountIn: safe.amountIn,
      quotedOutput: safe.quotedOutput,
      minimumOutput: boundedMinimum,
    };
  }

  const pairs = shuffled(directedPairs(snapshot.tokens));
  for (const pair of pairs) {
    if (!routeReady(snapshot, pair.assetIn, pair.assetOut, config.cash)) continue;
    const [minimum, configuredMaximum] = amountRange(config, pair.assetIn);
    const canAutoMint = config.autoMint && pair.assetIn.mintAmount !== undefined;
    const available = canAutoMint ? configuredMaximum : pair.assetIn.actorBalance;
    const maximum = available < configuredMaximum ? available : configuredMaximum;
    if (maximum < minimum) continue;

    const randomAmount = randomBigIntInclusive(minimum, maximum);
    const randomCandidate = await quoteCandidate(
      config,
      actor,
      outputBudget,
      "opening",
      canAutoMint,
      pair.assetIn,
      pair.assetOut,
      randomAmount,
    );
    if (randomCandidate !== undefined) return randomCandidate;
    if (randomAmount !== minimum) {
      const minimumCandidate = await quoteCandidate(
        config,
        actor,
        outputBudget,
        "opening",
        canAutoMint,
        pair.assetIn,
        pair.assetOut,
        minimum,
      );
      if (minimumCandidate !== undefined) return minimumCandidate;
    }
  }
  return undefined;
}

async function quoteCandidate(
  config: ActorConfig,
  actor: ActivityActor,
  outputBudget: SessionReserveBudget<Address>,
  leg: Candidate["leg"],
  allowMint: boolean,
  input: TokenSnapshot,
  output: TokenSnapshot,
  amountIn: bigint,
): Promise<Candidate | undefined> {
  const quotedOutput = await actor.quoteExactIn(amountIn, input.address, output.address);
  if (!isWithinReserveCap(quotedOutput, output.assetReserve, config.maximumOutputReservePpm)) return undefined;
  if (!outputBudget.allows(output.address, quotedOutput)) return undefined;
  const boundedMinimum = minimumOutput(quotedOutput, config.slippagePpm);
  if (boundedMinimum === 0n) return undefined;
  return { leg, allowMint, input, output, amountIn, quotedOutput, minimumOutput: boundedMinimum };
}

async function assertRouteStillReady(config: ActorConfig, actor: ActivityActor, candidate: Candidate): Promise<void> {
  const snapshot = await actor.inspect();
  const input = snapshot.tokens.find((token) => token.address === candidate.input.address);
  const output = snapshot.tokens.find((token) => token.address === candidate.output.address);
  if (input === undefined || output === undefined || !routeReady(snapshot, input, output, config.cash))
    throw new Error("route became unavailable before a preparation transaction");
  const quote = await actor.quoteExactIn(candidate.amountIn, input.address, output.address);
  if (!isWithinReserveCap(quote, output.assetReserve, config.maximumOutputReservePpm))
    throw new Error("route quote became unavailable before a preparation transaction");
}

async function prepareInput(config: ActorConfig, actor: ActivityActor, candidate: Candidate): Promise<void> {
  let balance = await actor.balance(candidate.input.address);
  if (balance < candidate.amountIn && candidate.allowMint && candidate.input.mintAmount !== undefined) {
    await assertRouteStillReady(config, actor, candidate);
    const transaction = await actor.mint(candidate.input.address);
    log("info", "mock_mint_confirmed", {
      token: candidate.input.id,
      tokenAddress: candidate.input.address,
      hash: transaction.hash,
      blockNumber: transaction.blockNumber,
      gasUsed: transaction.gasUsed,
    });
    balance = await actor.balance(candidate.input.address);
  }
  if (balance < candidate.amountIn) throw new Error(`insufficient ${candidate.input.symbol} balance`);

  if ((await actor.allowance(candidate.input.address)) < candidate.amountIn) {
    await assertRouteStillReady(config, actor, candidate);
    const transaction = await actor.approve(candidate.input.address, candidate.amountIn);
    log("info", "approval_confirmed", {
      token: candidate.input.id,
      tokenAddress: candidate.input.address,
      spender: config.pool,
      amount: formatUnits(candidate.amountIn, candidate.input.decimals),
      hash: transaction.hash,
      blockNumber: transaction.blockNumber,
      gasUsed: transaction.gasUsed,
    });
  }
}

function budgetLog(budget: SessionReserveBudget<Address>, token: TokenSnapshot): Readonly<Record<string, unknown>> {
  const status = budget.status(token.address);
  return {
    sessionOutputSpent: formatUnits(status.spent, token.decimals),
    sessionOutputLimit: formatUnits(status.limit, token.decimals),
  };
}

function tradeLog(candidate: Candidate): Readonly<Record<string, unknown>> {
  return {
    leg: candidate.leg,
    pair: `${candidate.input.symbol}/${candidate.output.symbol}`,
    assetIn: candidate.input.address,
    assetOut: candidate.output.address,
    amountIn: formatUnits(candidate.amountIn, candidate.input.decimals),
    quotedOutput: formatUnits(candidate.quotedOutput, candidate.output.decimals),
    minimumOutput: formatUnits(candidate.minimumOutput, candidate.output.decimals),
  };
}

main().catch((error: unknown) => {
  log("error", "actor_failed", { detail: errorMessage(error) });
  process.exitCode = 1;
});
