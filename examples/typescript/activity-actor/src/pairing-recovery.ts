import type { Address } from "viem";
import type { ObservedSwap } from "./actor.js";
import type { PairingPhase } from "./pairing-state.js";
import { PairedSwapPlan } from "./strategy.js";

/** Recreates the in-memory plan represented by an end-of-block checkpoint. */
export function pairingPlanFromPhase(phase: PairingPhase): PairedSwapPlan<Address> | undefined {
  if (phase.kind === "opening") return undefined;
  return new PairedSwapPlan(
    { assetIn: phase.assetOut, assetOut: phase.assetIn },
    {
      assetIn: phase.assetIn,
      assetOut: phase.assetOut,
      maximumAmountIn: phase.maximumAmountIn,
    },
  );
}

/** Serializes the current in-memory plan without private key material. */
export function pairingPhaseFromPlan(plan: PairedSwapPlan<Address> | undefined): PairingPhase {
  const pending = plan?.pendingReturn;
  return pending === undefined
    ? { kind: "opening" }
    : {
        kind: "return",
        assetIn: pending.assetIn,
        assetOut: pending.assetOut,
        maximumAmountIn: pending.maximumAmountIn,
      };
}

/** A bounded age reset deliberately forgets all prior pairing history. */
export function pairingPhaseAfterHistoryReset(): PairingPhase {
  return { kind: "opening" };
}

/** Applies canonical, ordered actor swaps after the checkpoint cursor. */
export function replayPairingHistory(
  initialPhase: PairingPhase,
  events: readonly ObservedSwap[],
  supported: ReadonlySet<Address>,
): PairedSwapPlan<Address> | undefined {
  if (
    initialPhase.kind === "return" &&
    (!supported.has(initialPhase.assetIn) ||
      !supported.has(initialPhase.assetOut) ||
      initialPhase.assetIn === initialPhase.assetOut)
  )
    throw new Error("pairing checkpoint contains an unsupported or self-directed return");

  let plan = pairingPlanFromPhase(initialPhase);
  for (const event of events) {
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
  return plan;
}
