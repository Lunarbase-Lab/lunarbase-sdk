import type {
  BackfillRequest,
  ChainCursor,
  ChainEventSource,
  ChainUpdate,
  ContractFilter,
  ContractLog,
  Network,
  QuoteEvent,
} from "./model.js";

/** Provider adapter exposing snapshots, canonical backfill, and normalized updates. */
export interface NormalizedBackend {
  snapshotCursor(network: Network): Promise<ChainCursor>;
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]>;
  subscribe(network: Network, filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate>;
}

/** Runtime-facing source facade around a network-specific backend. */
export class NetworkSource implements ChainEventSource {
  constructor(
    readonly network: Network,
    private readonly backend: NormalizedBackend,
  ) {}

  /** Returns the backend's authoritative snapshot cursor. */
  snapshotCursor(): Promise<ChainCursor> {
    return this.backend.snapshotCursor(this.network);
  }

  /** Reads canonical logs through the backend. */
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]> {
    return this.backend.backfill(request);
  }

  /** Opens normalized realtime updates for this network. */
  subscribe(filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    return this.backend.subscribe(this.network, filter, signal);
  }
}

/** Creates the generic source facade used by the common runtime. */
export function makeNetworkSource(network: Network, backend: NormalizedBackend): NetworkSource {
  return new NetworkSource(network, backend);
}

/** Holds provisional decoded events until canonical logs can be compared. */
export class ProvisionalOverlay {
  private baseCursor?: ChainCursor;
  private values: Array<readonly [ChainCursor, QuoteEvent]> = [];

  /** Starts an overlay at a canonical base cursor. */
  begin(baseCursor: ChainCursor): void {
    this.baseCursor = baseCursor;
    this.values = [];
  }

  /** Appends one provisional decoded event. */
  push(cursor: ChainCursor, event: QuoteEvent): void {
    this.values.push([cursor, event]);
  }

  /** Returns provisional events in application order. */
  updates(): readonly (readonly [ChainCursor, QuoteEvent])[] {
    return this.values;
  }

  /** Fails if canonical replay differs from the provisional overlay. */
  verifyCanonical(canonical: readonly (readonly [ChainCursor, QuoteEvent])[]): void {
    const stable = (value: unknown) =>
      JSON.stringify(value, (_key, item) => (typeof item === "bigint" ? `${item}n` : item));
    if (stable(this.values) !== stable(canonical)) throw new Error("provisional overlay diverged from canonical logs");
  }

  /** Verifies, clears, and returns the canonical cursor after commit. */
  commitCanonical(canonical: readonly (readonly [ChainCursor, QuoteEvent])[]): ChainCursor | undefined {
    this.verifyCanonical(canonical);
    const cursor = canonical.length > 0 ? canonical[canonical.length - 1]?.[0] : this.baseCursor;
    this.clear();
    return cursor;
  }

  /** Discards the provisional overlay. */
  discard(): void {
    this.clear();
  }

  /** Clears base cursor and provisional events. */
  clear(): void {
    this.baseCursor = undefined;
    this.values = [];
  }
}
