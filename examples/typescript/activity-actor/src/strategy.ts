import { randomBytes, randomInt } from "node:crypto";

export const PPM = 1_000_000n;

export interface DirectedPair<T> {
  readonly assetIn: T;
  readonly assetOut: T;
}

export interface ReverseIntent<T> extends DirectedPair<T> {
  readonly maximumAmountIn: bigint;
}

export interface SafeReturnAmount {
  readonly amountIn: bigint;
  readonly quotedOutput: bigint;
}

export interface ReserveBudgetStatus {
  readonly baseline: bigint;
  readonly spent: bigint;
  readonly limit: bigint;
}

const UINT256_MAX = (1n << 256n) - 1n;

function samePair<T>(pair: DirectedPair<T>, assetIn: T, assetOut: T): boolean {
  return pair.assetIn === assetIn && pair.assetOut === assetOut;
}

function requirePositiveUint256(value: bigint, label: string): void {
  if (value <= 0n || value > UINT256_MAX) throw new RangeError(`${label} must be a positive uint256`);
}

/**
 * Tracks one fixed pair as opening then return legs. State advances only after
 * an exact matching confirmed swap, so failed attempts cannot change direction.
 */
export class PairedSwapPlan<Key> {
  readonly opening: DirectedPair<Key>;
  #pendingReturn: ReverseIntent<Key> | undefined;

  constructor(opening: DirectedPair<Key>, pendingReturn?: ReverseIntent<Key>) {
    if (opening.assetIn === opening.assetOut) throw new RangeError("opening assets must be different");
    this.opening = { ...opening };

    if (pendingReturn !== undefined) {
      requirePositiveUint256(pendingReturn.maximumAmountIn, "reverse maximumAmountIn");
      if (pendingReturn.assetIn !== opening.assetOut || pendingReturn.assetOut !== opening.assetIn)
        throw new Error("pending return must exactly reverse the opening pair");
      this.#pendingReturn = { ...pendingReturn };
    }
  }

  get pendingReturn(): ReverseIntent<Key> | undefined {
    return this.#pendingReturn === undefined ? undefined : { ...this.#pendingReturn };
  }

  recordConfirmed(assetIn: Key, assetOut: Key, actualAmountOut: bigint): void {
    requirePositiveUint256(actualAmountOut, "actualAmountOut");

    const pendingReturn = this.#pendingReturn;
    if (pendingReturn === undefined) {
      if (!samePair(this.opening, assetIn, assetOut))
        throw new Error("confirmed swap does not match the required opening leg");
      this.#pendingReturn = {
        assetIn: this.opening.assetOut,
        assetOut: this.opening.assetIn,
        maximumAmountIn: actualAmountOut,
      };
      return;
    }

    if (!samePair(pendingReturn, assetIn, assetOut))
      throw new Error("confirmed swap does not match the pending return leg");
    this.#pendingReturn = undefined;
  }
}

/**
 * Tries a received output amount as the return input, then halves it until a
 * non-zero quote satisfies all caller-provided reserve and budget guards.
 */
export async function findSafeReturnAmount(
  maximumAmountIn: bigint,
  quote: (amountIn: bigint) => bigint | Promise<bigint>,
  allows: (quotedOutput: bigint) => boolean | Promise<boolean>,
): Promise<SafeReturnAmount | undefined> {
  if (maximumAmountIn < 0n || maximumAmountIn > UINT256_MAX) throw new RangeError("maximumAmountIn must be a uint256");

  let amountIn = maximumAmountIn;
  for (let attempt = 0; attempt < 256 && amountIn > 0n; attempt += 1) {
    const quotedOutput = await quote(amountIn);
    if (quotedOutput < 0n || quotedOutput > UINT256_MAX) throw new RangeError("quote must return a uint256");
    if (quotedOutput > 0n && (await allows(quotedOutput))) return { amountIn, quotedOutput };
    amountIn >>= 1n;
  }
  return undefined;
}

/** Builds every ordered pair without self-routes. */
export function directedPairs<T>(values: readonly T[]): Array<DirectedPair<T>> {
  return values.flatMap((assetIn) =>
    values.filter((assetOut) => assetOut !== assetIn).map((assetOut) => ({ assetIn, assetOut })),
  );
}

/** Returns a shuffled copy using the platform CSPRNG. */
export function shuffled<T>(values: readonly T[]): T[] {
  const result = [...values];
  for (let index = result.length - 1; index > 0; index -= 1) {
    const other = randomInt(index + 1);
    [result[index], result[other]] = [result[other]!, result[index]!];
  }
  return result;
}

/** Samples a bigint uniformly from an inclusive range. */
export function randomBigIntInclusive(minimum: bigint, maximum: bigint): bigint {
  if (minimum < 0n || maximum < minimum) throw new RangeError("invalid bigint random range");
  const range = maximum - minimum + 1n;
  if (range === 1n) return minimum;
  const bits = range.toString(2).length;
  const bytes = Math.ceil(bits / 8);
  const mask = (1n << BigInt(bits)) - 1n;
  for (;;) {
    const sample = BigInt(`0x${randomBytes(bytes).toString("hex")}`) & mask;
    if (sample < range) return minimum + sample;
  }
}

/** Applies conventional parts-per-million slippage and rounds down. */
export function minimumOutput(quotedOutput: bigint, slippagePpm: number): bigint {
  if (!Number.isSafeInteger(slippagePpm) || slippagePpm < 0 || slippagePpm >= Number(PPM))
    throw new RangeError("slippagePpm must be an integer in [0, 1_000_000)");
  return (quotedOutput * (PPM - BigInt(slippagePpm))) / PPM;
}

function requireCapPpm(capPpm: number): void {
  if (!Number.isSafeInteger(capPpm) || capPpm <= 0 || capPpm > Number(PPM))
    throw new RangeError("capPpm must be an integer in (0, 1_000_000]");
}

/** Prevents one swap from consuming too much of the current output reserve. */
export function isWithinReserveCap(output: bigint, outputReserve: bigint, capPpm: number): boolean {
  requireCapPpm(capPpm);
  return output > 0n && output <= (outputReserve * BigInt(capPpm)) / PPM;
}

/**
 * Conservatively limits cumulative quoted output against the first positive
 * reserve observed during one process run.
 */
export class SessionReserveBudget<Key> {
  readonly #capPpm: number;
  readonly #baseline = new Map<Key, bigint>();
  readonly #spent = new Map<Key, bigint>();

  constructor(capPpm: number) {
    requireCapPpm(capPpm);
    this.#capPpm = capPpm;
  }

  observe(key: Key, reserve: bigint): void {
    if (reserve < 0n) throw new RangeError("reserve must be non-negative");
    const baseline = this.#baseline.get(key);
    if ((baseline === undefined || baseline === 0n) && reserve > 0n) this.#baseline.set(key, reserve);
  }

  allows(key: Key, output: bigint): boolean {
    if (output <= 0n) return false;
    const { spent, limit } = this.status(key);
    return spent + output <= limit;
  }

  record(key: Key, output: bigint): void {
    if (!this.allows(key, output)) throw new RangeError("output exceeds the session reserve budget");
    this.#spent.set(key, this.status(key).spent + output);
  }

  status(key: Key): ReserveBudgetStatus {
    const baseline = this.#baseline.get(key) ?? 0n;
    return {
      baseline,
      spent: this.#spent.get(key) ?? 0n,
      limit: (baseline * BigInt(this.#capPpm)) / PPM,
    };
  }
}

/** Inclusive jitter for loop and retry delays. */
export function randomDelayMilliseconds(minimumSeconds: number, maximumSeconds: number): number {
  if (
    !Number.isSafeInteger(minimumSeconds) ||
    !Number.isSafeInteger(maximumSeconds) ||
    minimumSeconds < 0 ||
    maximumSeconds < minimumSeconds
  )
    throw new RangeError("invalid delay range");
  return randomInt(minimumSeconds, maximumSeconds + 1) * 1_000;
}
