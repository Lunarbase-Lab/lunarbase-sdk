/** Deployment and checkpoint compatibility validation. */
import { parseAddress } from "@lunarbase/math";
import * as Hash from "ox/Hash";
import * as Hex from "ox/Hex";
import { IndexerError, MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION } from "./model.js";
import type { Checkpoint, DeploymentConfig } from "./model.js";

const normalized = (value: string): string => value.toLowerCase();

/** Validates deployment identity before any source task starts. */
export function validateDeploymentConfig(config: DeploymentConfig): void {
  if (config.chainId <= 0n) throw new IndexerError("SOURCE", "chainId must be positive");
  try {
    const core = parseAddress(config.core);
    const router = parseAddress(config.router);
    if (Hex.toBigInt(core) === 0n || Hex.toBigInt(router) === 0n) throw new Error("zero address");
  } catch {
    throw new IndexerError("SOURCE", "Core and router must be valid non-zero addresses");
  }
  if (!Hash.validate(config.expectedRuntimeCodeHash) || Hex.toBigInt(config.expectedRuntimeCodeHash) === 0n)
    throw new IndexerError("SOURCE", "expected Core runtime code hash must be non-zero bytes32");
  if (config.contractCompatibilityVersion !== MATH_COMPATIBILITY_VERSION)
    throw new IndexerError("SOURCE", `contract compatibility mismatch: expected ${MATH_COMPATIBILITY_VERSION}`);
  if (config.httpRpcUrl.length === 0 || config.realtimeSource.length === 0)
    throw new IndexerError("SOURCE", "RPC and realtime source are required");
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

/** Checks local v3 identity before asking RPC whether the block is canonical. */
export function checkpointMatchesDeployment(checkpoint: Checkpoint, config: DeploymentConfig): boolean {
  return (
    checkpoint.schemaVersion === SCHEMA_VERSION &&
    checkpoint.mathCompatibilityVersion === MATH_COMPATIBILITY_VERSION &&
    normalized(checkpoint.expectedRuntimeCodeHash) === normalized(config.expectedRuntimeCodeHash) &&
    checkpoint.chainId === config.chainId &&
    normalized(checkpoint.core) === normalized(config.core) &&
    normalized(checkpoint.router) === normalized(config.router)
  );
}
