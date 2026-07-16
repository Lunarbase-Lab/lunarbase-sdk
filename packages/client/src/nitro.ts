import type { BackfillRequest, ChainCursor, ChainUpdate, ContractFilter, ContractLog, Network } from "./model.js";
import { Network as NetworkValue } from "./model.js";
import { RpcError, JsonRpcHttpClient } from "./rpc.js";
import { WsRpcBackend, type WsRpcConfig } from "./ws.js";
import type { NormalizedBackend } from "./sources.js";

/** Executed Nitro source; raw sequencer feed data is intentionally not accepted. */
export class ArbitrumNitroBackend implements NormalizedBackend {
  private readonly inner: WsRpcBackend;
  constructor(readonly rpc: JsonRpcHttpClient, readonly wsEndpoint: string, readonly network: Network, readonly chainId: bigint, readonly snapshotTag = "finalized", config: Partial<WsRpcConfig> = {}, readonly requireEvmParentContext = true) {
    this.inner = new WsRpcBackend(rpc, wsEndpoint, network, chainId, snapshotTag, config);
  }
  snapshotCursor(network: Network): Promise<ChainCursor> { return this.inner.snapshotCursor(network); }
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]> { return this.inner.backfill(request); }
  subscribe(network: Network, filter: ContractFilter, signal?: AbortSignal): AsyncIterable<ChainUpdate> {
    if (network !== NetworkValue.Arbitrum || network !== this.network) throw new RpcError("INVALID", "Arbitrum Nitro backend network mismatch");
    const inner = this.inner.subscribe(network, filter, signal);
    const requireContext = this.requireEvmParentContext;
    return (async function* () {
      for await (const update of inner) {
        if (requireContext && update.kind === "Head" && update.cursor.sourceSequence === undefined) {
          yield { kind: "Gap", cursor: update.cursor, reason: "Arbitrum Nitro head omitted l1BlockNumber/EVM parent context" };
          return;
        }
        yield update;
      }
    })();
  }
}
