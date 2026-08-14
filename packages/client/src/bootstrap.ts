/** Deployment and checkpoint compatibility validation. */
import { BPS, parseAddress } from "@lunarbase-lab/pmm-v2-math";
import { decodeLaneSlot0 } from "@lunarbase-lab/pmm-v2-math/slot0";
import * as Hash from "ox/Hash";
import * as Hex from "ox/Hex";
import { Commitment, IndexerError, MATH_COMPATIBILITY_VERSION, Network, SCHEMA_VERSION } from "./model.js";
import type { Checkpoint, DeploymentConfig } from "./model.js";

const normalized = (value: string): string => value.toLowerCase();

/** Validates deployment identity before any source task starts. */
export function validateDeploymentConfig(config: DeploymentConfig): void {
  if (!Object.values(Network).includes(config.network))
    throw new IndexerError("SOURCE", "network must be a supported source family");
  if (config.feeClass !== "Whitelisted" && config.feeClass !== "NonWhitelisted")
    throw new IndexerError("SOURCE", "feeClass must be Whitelisted or NonWhitelisted");
  if (!Array.isArray(config.explicitLaneAssets))
    throw new IndexerError("SOURCE", "explicitLaneAssets must be an array");
  if (!isUint(config.chainId, U64_MAX) || config.chainId === 0n)
    throw new IndexerError("SOURCE", "chainId must be a positive u64");
  if (!isUint(config.deploymentBlock, U64_MAX))
    throw new IndexerError("SOURCE", "deploymentBlock must be a non-negative u64");
  try {
    const core = parseAddress(config.core);
    const implementation = parseAddress(config.expectedImplementation);
    const verifiedRouter = config.verifiedRouter === undefined ? undefined : parseAddress(config.verifiedRouter);
    if (
      Hex.toBigInt(core) === 0n ||
      (verifiedRouter !== undefined && Hex.toBigInt(verifiedRouter) === 0n) ||
      Hex.toBigInt(implementation) === 0n
    )
      throw new Error("zero address");
  } catch {
    throw new IndexerError(
      "SOURCE",
      "Core, optional verified router, and implementation must be valid non-zero addresses",
    );
  }
  if (
    !Hash.validate(config.expectedImplementationCodeHash) ||
    Hex.toBigInt(config.expectedImplementationCodeHash) === 0n
  )
    throw new IndexerError("SOURCE", "expected implementation code hash must be non-zero bytes32");
  if (config.contractCompatibilityVersion !== MATH_COMPATIBILITY_VERSION)
    throw new IndexerError("SOURCE", `contract compatibility mismatch: expected ${MATH_COMPATIBILITY_VERSION}`);
  const lanes = new Set<string>();
  for (const asset of config.explicitLaneAssets) {
    let parsed: string;
    try {
      parsed = parseAddress(asset).toLowerCase();
      if (Hex.toBigInt(asset) === 0n) throw new Error("zero address");
    } catch {
      throw new IndexerError("SOURCE", "explicit lane assets must be valid non-zero addresses");
    }
    if (lanes.has(parsed)) throw new IndexerError("SOURCE", "explicit lane assets must be unique");
    lanes.add(parsed);
  }
}

/** Checks checkpoint identity before asking RPC whether the block is canonical. */
export function checkpointMatchesDeployment(checkpoint: Checkpoint, config: DeploymentConfig): boolean {
  try {
    return (
      checkpoint.schemaVersion === SCHEMA_VERSION &&
      checkpoint.mathCompatibilityVersion === MATH_COMPATIBILITY_VERSION &&
      checkpoint.mathCompatibilityVersion === config.contractCompatibilityVersion &&
      normalized(checkpoint.expectedImplementation) === normalized(config.expectedImplementation) &&
      normalized(checkpoint.expectedImplementationCodeHash) === normalized(config.expectedImplementationCodeHash) &&
      checkpoint.chainId === config.chainId &&
      checkpoint.network === config.network &&
      normalized(checkpoint.core) === normalized(config.core) &&
      checkpoint.deploymentBlock === config.deploymentBlock &&
      sameAddressSet(checkpoint.explicitLaneAssets, config.explicitLaneAssets) &&
      checkpointHasValidStructure(checkpoint)
    );
  } catch {
    return false;
  }
}

/** Validates restart-state structure before it reaches the quote reducer. */
export function checkpointHasValidStructure(checkpoint: Checkpoint): boolean {
  try {
    if (
      !Object.values(Network).includes(checkpoint.network) ||
      !isUint(checkpoint.chainId, U64_MAX) ||
      checkpoint.chainId === 0n ||
      !isUint(checkpoint.deploymentBlock, U64_MAX) ||
      !isNonZeroAddress(checkpoint.expectedImplementation) ||
      !isHash(checkpoint.expectedImplementationCodeHash) ||
      !isNonZeroAddress(checkpoint.core) ||
      !Array.isArray(checkpoint.explicitLaneAssets)
    )
      return false;

    const explicitLanes = new Set<string>();
    for (const asset of checkpoint.explicitLaneAssets) {
      if (!isNonZeroAddress(asset)) return false;
      const assetKey = addressKey(asset);
      if (explicitLanes.has(assetKey)) return false;
      explicitLanes.add(assetKey);
    }

    const cursor = checkpoint.cursor;
    if (
      cursor.chainId !== checkpoint.chainId ||
      !isUint(cursor.blockNumber, U64_MAX) ||
      cursor.blockNumber < checkpoint.deploymentBlock ||
      !isUint(cursor.executionBlockNumber, U64_MAX) ||
      !isHash(cursor.blockHash) ||
      !Object.values(Commitment).includes(cursor.commitment) ||
      (cursor.transactionIndex === undefined) !== (cursor.logIndex === undefined) ||
      !isOptionalUint(cursor.transactionIndex, U32_MAX) ||
      !isOptionalUint(cursor.logIndex, U32_MAX) ||
      !isOptionalUint(cursor.sourceSequence, U64_MAX) ||
      !isOptionalUint(cursor.sourceSubIndex, U32_MAX) ||
      (cursor.sourceSubIndex !== undefined && cursor.sourceSequence === undefined)
    )
      return false;

    const state = checkpoint.state;
    if (
      !isNonZeroAddress(state.cash) ||
      !isUint(state.cashReserve, U128_MAX) ||
      !(state.lanes instanceof Map) ||
      !isUint(state.blacklistFeeMultiplier, U256_MAX)
    )
      return false;

    const cash = addressKey(state.cash);
    const laneAssets = new Set<string>();
    for (const [asset, lane] of state.lanes) {
      if (!isNonZeroAddress(asset)) return false;
      const assetKey = addressKey(asset);
      if (assetKey === cash || laneAssets.has(assetKey)) return false;
      laneAssets.add(assetKey);
      if (
        !isUint(lane.slot0, U256_MAX) ||
        !isUint(lane.assetReserve, U128_MAX) ||
        !isUint(lane.totalPrincipalAmount, U128_MAX)
      )
        return false;
      const slot0 = decodeLaneSlot0(lane.slot0);
      if (!slot0.exists || slot0.askFeeBps > BPS || slot0.bidFeeBps > BPS || BigInt(slot0.slippageKBps) > BPS)
        return false;
    }

    return true;
  } catch {
    return false;
  }
}

const U32_MAX = (1n << 32n) - 1n;
const U64_MAX = (1n << 64n) - 1n;
const U128_MAX = (1n << 128n) - 1n;
const U256_MAX = (1n << 256n) - 1n;

function isUint(value: unknown, maximum: bigint): value is bigint {
  return typeof value === "bigint" && value >= 0n && value <= maximum;
}

function isOptionalUint(value: unknown, maximum: bigint): boolean {
  return value === undefined || isUint(value, maximum);
}

function isNonZeroAddress(value: string): boolean {
  const parsed = parseAddress(value);
  return Hex.toBigInt(parsed) !== 0n;
}

function isHash(value: string | undefined): boolean {
  return value !== undefined && Hash.validate(value) && Hex.toBigInt(value) !== 0n;
}

function addressKey(value: string): string {
  return parseAddress(value).toLowerCase();
}

function sameAddressSet(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length) return false;
  const expected = new Set(right.map(addressKey));
  return left.every((asset) => expected.has(addressKey(asset)));
}
