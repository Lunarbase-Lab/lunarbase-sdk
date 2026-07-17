import { type ChainUpdate, type Commitment, type ContractLog } from "@lunarbase/client-core";

/** Nitro context containing L2 and EVM-visible parent block numbers. */
export interface ArbitrumExecutionContext {
  readonly l2BlockNumber: bigint;
  readonly evmParentBlockNumber: bigint;
}

/** Executed Nitro head with quote-relevant parent-chain context. */
export interface ArbitrumHead {
  readonly context: ArbitrumExecutionContext;
  readonly blockHash?: string;
  readonly commitment: Commitment;
}

/** Converts executed Nitro records into normalized runtime updates. */
export class ArbitrumNitroNormalizer {
  /** Creates a normalizer for one Arbitrum chain id. */
  constructor(readonly chainId: bigint) {}

  /** Maps Nitro's EVM-visible parent context into the cursor sequence. */
  normalizeHead(head: ArbitrumHead): ChainUpdate {
    return {
      kind: "Head",
      cursor: {
        chainId: this.chainId,
        blockNumber: head.context.l2BlockNumber,
        blockHash: head.blockHash,
        sourceSequence: head.context.evmParentBlockNumber,
        commitment: head.commitment,
      },
    };
  }

  /** Validates and passes through an executed Nitro log. */
  normalizeLog(log: ContractLog): ChainUpdate {
    if (log.cursor.chainId !== this.chainId) throw new Error("Arbitrum chain id mismatch");
    return { kind: "Log", log };
  }
}
