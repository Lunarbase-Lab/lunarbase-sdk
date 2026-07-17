/** Base Flashblocks client package. */
export * from "./normalizer.js";
export * from "./transport.js";

import { Network, NetworkSource, type NormalizedBackend } from "@lunarbase/client-core";

/** Runtime-facing Base source backed by a Flashblocks backend. */
export class BaseFlashblocksSource extends NetworkSource {
  constructor(backend: NormalizedBackend) {
    super(Network.Base, backend);
  }
}
