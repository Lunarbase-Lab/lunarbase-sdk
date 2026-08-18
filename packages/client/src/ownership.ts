/** Owned immutable copies for asynchronous runtime boundaries. */
import { parseAddress } from "@lunarbase-lab/pmm-v2-math";
import type { BackfillRequest, ContractFilter, DeploymentConfig } from "./model.js";
import { validateDeploymentConfig } from "./bootstrap.js";

/** Validates, canonicalizes, copies, and freezes one deployment identity. */
export function ownDeploymentConfig(config: DeploymentConfig): DeploymentConfig {
  validateDeploymentConfig(config);
  return Object.freeze({
    ...config,
    core: parseAddress(config.core),
    verifiedRouter: config.verifiedRouter === undefined ? undefined : parseAddress(config.verifiedRouter),
    expectedImplementation: parseAddress(config.expectedImplementation),
    expectedImplementationCodeHash:
      config.expectedImplementationCodeHash.toLowerCase() as DeploymentConfig["expectedImplementationCodeHash"],
    explicitLaneAssets: Object.freeze(config.explicitLaneAssets.map(parseAddress)),
  });
}

/** Canonicalizes and detaches a source filter from caller-owned arrays. */
export function ownContractFilter(filter: ContractFilter): ContractFilter {
  return Object.freeze({
    address: parseAddress(filter.address),
    topics: Object.freeze([...filter.topics]),
  });
}

/** Detaches an inclusive recovery request before its first asynchronous call. */
export function ownBackfillRequest(request: BackfillRequest): BackfillRequest {
  return Object.freeze({
    fromBlock: request.fromBlock,
    toBlock: request.toBlock,
    filter: ownContractFilter(request.filter),
  });
}
