/** Arbitrum Nitro client package. */
export * from "./normalizer.js";
export * from "./transport.js";

import { Network, NetworkSource, type NormalizedBackend } from "@lunarbase/client-core";

/** Runtime-facing Arbitrum source backed by executed Nitro state. */
export class ArbitrumNitroSource extends NetworkSource {
  constructor(backend: NormalizedBackend) {
    super(Network.Arbitrum, backend);
  }
}
