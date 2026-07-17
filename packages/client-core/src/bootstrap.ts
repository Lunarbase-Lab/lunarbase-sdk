/** Deployment and checkpoint compatibility validation. */
import { IndexerError, MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION } from "./model.js";
import type { Checkpoint, DeploymentConfig } from "./model.js";

const normalized = (value: string): string => value.toLowerCase();

/** Validates deployment identity before any source task starts. */
export function validateDeploymentConfig(config: DeploymentConfig): void {
  if (
    config.chainId <= 0n ||
    config.core.length === 0 ||
    config.router.length === 0 ||
    config.httpRpcUrl.length === 0 ||
    config.realtimeSource.length === 0 ||
    config.contractCompatibilityVersion.length === 0
  )
    throw new IndexerError("SOURCE", "invalid deployment configuration");
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
