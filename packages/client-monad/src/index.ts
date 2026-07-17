/** Monad execution-events client package. */
export * from "./transport.js";

import { MonadExecutionEngine } from "@lunarbase/client-core";
import { MonadSidecarBackend } from "./transport.js";

/** Runtime-facing Monad source using the universal execution engine. */
export class MonadExecutionEventsSource extends MonadExecutionEngine {
  constructor(backend: MonadSidecarBackend) {
    super(backend, backend, backend.chainId);
  }
}
