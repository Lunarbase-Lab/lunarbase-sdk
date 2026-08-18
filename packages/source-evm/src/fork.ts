/** Bounded fork resolution primitives for durable EVM event delivery. */
import { Commitment, type BlockRef, type ChainCursor } from "@lunarbase-lab/pmm-v2-client";
import type * as Hex from "ox/Hex";
import { RpcError, RpcHttpBackend, parseHash } from "./rpc.js";

/** Conservative charge including two objects, bigints, and both hash strings. */
export const BLOCK_REF_RETAINED_BYTES = 512;

/** Count and retained-byte bounds for an unfinalized canonical window. */
export interface ForkWindowLimits {
  readonly maxBlocks: number;
  readonly maxBytes: number;
}

/** Default fork window bounds. */
export const DEFAULT_FORK_WINDOW_LIMITS: ForkWindowLimits = Object.freeze({
  maxBlocks: 4096,
  maxBytes: 2 * 1024 * 1024,
});

/** Stable fail-closed fork error categories. */
export type ForkErrorCode =
  | "INVALID_IDENTITY"
  | "DISCONNECTED"
  | "INVALID_LIMITS"
  | "BLOCK_BUDGET"
  | "BYTE_BUDGET"
  | "ANCESTOR_OUTSIDE_WINDOW"
  | "DEPTH_EXCEEDED"
  | "FINALIZED_CONFLICT"
  | "STALE_RESOLUTION"
  | "RPC";

/** Fail-closed fork-window or resolution error. */
export class ForkError extends Error {
  constructor(
    readonly code: ForkErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "ForkError";
  }
}

/** Deterministic correction plan computed without mutating canonical state. */
export interface ForkResolution {
  readonly commonAncestor: BlockRef;
  readonly oldTip: BlockRef;
  readonly newTip: BlockRef;
  /** Abandoned blocks in ascending height order, excluding the ancestor. */
  readonly oldBranch: readonly BlockRef[];
  /** Replacement blocks in ascending height order, excluding the ancestor. */
  readonly newBranch: readonly BlockRef[];
}

/**
 * Bounded canonical header window.
 *
 * Normal append is O(1). Finality advances a start index instead of using
 * `Array.shift()` and compacts only occasionally outside the ingestion path.
 */
export class CanonicalWindow {
  private storage: BlockRef[] = [];
  private start = 0;
  private finalizedBlock?: BlockRef;
  readonly limits: ForkWindowLimits;

  constructor(limits: ForkWindowLimits = DEFAULT_FORK_WINDOW_LIMITS) {
    if (
      !positiveInteger(limits.maxBlocks) ||
      !positiveInteger(limits.maxBytes) ||
      limits.maxBytes < BLOCK_REF_RETAINED_BYTES
    )
      throw new ForkError("INVALID_LIMITS", "canonical window limits must be positive");
    this.limits = Object.freeze({ ...limits });
  }

  /** Number of retained blocks. */
  get length(): number {
    return this.storage.length - this.start;
  }

  /** Conservative retained-byte charge used for admission control. */
  get retainedBytes(): number {
    return this.length * BLOCK_REF_RETAINED_BYTES;
  }

  /** Current canonical tip, if seeded. */
  get tip(): BlockRef | undefined {
    return this.storage.at(-1);
  }

  /** Highest finalized boundary retained by this window. */
  get finalized(): BlockRef | undefined {
    return this.finalizedBlock;
  }

  /** Iterates retained blocks from oldest to newest without copying them. */
  *blocks(): IterableIterator<BlockRef> {
    for (let index = this.start; index < this.storage.length; index += 1) yield this.storage[index]!;
  }

  /** Appends one contiguous block. Exact duplicate tips are ignored. */
  pushHead(input: BlockRef): boolean {
    const block = ownCompleteBlock(input);
    const tip = this.tip;
    if (tip !== undefined) {
      validateSameChain(tip, block);
      if (sameBlock(tip, block)) return false;
      if (block.cursor.blockNumber !== tip.cursor.blockNumber + 1n || block.parentHash !== tip.cursor.blockHash)
        throw new ForkError("DISCONNECTED", "block is disconnected from the retained canonical window");
    }
    this.preflight(this.length + 1);
    this.storage.push(block);
    return true;
  }

  /** Replaces a same-height progressive tip sharing the same parent. */
  replaceProgressiveTip(input: BlockRef): void {
    const block = ownCompleteBlock(input);
    const tip = this.tip;
    if (tip === undefined) throw new ForkError("DISCONNECTED", "canonical window is empty");
    validateSameChain(tip, block);
    if (this.finalizedBlock !== undefined && this.finalizedBlock.cursor.blockNumber >= block.cursor.blockNumber)
      throw new ForkError("FINALIZED_CONFLICT", "progressive head would replace finalized history");
    if (tip.cursor.blockNumber !== block.cursor.blockNumber || tip.parentHash !== block.parentHash)
      throw new ForkError("DISCONNECTED", "progressive head does not share the retained tip parent");
    this.storage[this.storage.length - 1] = block;
  }

  /** Advances finality and prunes only history strictly older than the boundary. */
  advanceFinalized(input: BlockRef): void {
    const block = ownCompleteBlock(input);
    if (block.cursor.commitment !== Commitment.Finalized)
      throw new ForkError("INVALID_IDENTITY", "finalized watermark lacks finalized commitment");
    const previous = this.finalizedBlock;
    if (previous !== undefined) {
      validateSameChain(previous, block);
      if (
        block.cursor.blockNumber < previous.cursor.blockNumber ||
        (block.cursor.blockNumber === previous.cursor.blockNumber &&
          block.cursor.blockHash !== previous.cursor.blockHash)
      )
        throw new ForkError("FINALIZED_CONFLICT", "finalized watermark regressed or changed hash");
    }
    const index = this.position(requiredHash(block));
    if (index < 0) throw new ForkError("ANCESTOR_OUTSIDE_WINDOW", "finalized block is outside the retained window");
    const retained = this.storage[index]!;
    validateSameChain(retained, block);
    if (
      retained.cursor.blockNumber !== block.cursor.blockNumber ||
      retained.cursor.executionBlockNumber !== block.cursor.executionBlockNumber ||
      retained.parentHash !== block.parentHash
    )
      throw new ForkError("INVALID_IDENTITY", "finalized block does not match retained identity");
    this.start = index;
    this.storage[index] = block;
    this.finalizedBlock = block;
    this.compactAfterFinality();
  }

  /** Atomically switches the view after its durable correction commits. */
  applyResolution(resolution: ForkResolution): void {
    const tip = this.tip;
    if (tip === undefined || !sameBlock(tip, resolution.oldTip))
      throw new ForkError("STALE_RESOLUTION", "fork resolution was computed against another tip");
    const ancestorIndex = this.position(requiredHash(resolution.commonAncestor));
    if (ancestorIndex < 0 || !sameBlock(this.storage[ancestorIndex]!, resolution.commonAncestor))
      throw new ForkError("STALE_RESOLUTION", "common ancestor is no longer retained");
    const currentOldBranch = this.storage.slice(ancestorIndex + 1);
    if (!sameBranch(currentOldBranch, resolution.oldBranch))
      throw new ForkError("STALE_RESOLUTION", "abandoned branch no longer matches the window");
    const replacement = resolution.newBranch.map(ownCompleteBlock);
    this.validateReplacement(resolution.commonAncestor, resolution.newTip, replacement);
    this.preflight(ancestorIndex - this.start + 1 + replacement.length);
    this.storage.length = ancestorIndex + 1;
    this.storage.push(...replacement);
  }

  private validateReplacement(commonAncestor: BlockRef, newTip: BlockRef, branch: readonly BlockRef[]): void {
    if (this.finalizedBlock !== undefined && commonAncestor.cursor.blockNumber < this.finalizedBlock.cursor.blockNumber)
      throw new ForkError("FINALIZED_CONFLICT", "fork crosses the finalized watermark");
    let parent = commonAncestor;
    for (const block of branch) {
      validateSameChain(parent, block);
      if (block.cursor.blockNumber !== parent.cursor.blockNumber + 1n || block.parentHash !== parent.cursor.blockHash)
        throw new ForkError("DISCONNECTED", "replacement branch contains a parent discontinuity");
      parent = block;
    }
    if (!sameBlock(parent, newTip))
      throw new ForkError("STALE_RESOLUTION", "replacement branch does not end at new tip");
  }

  private preflight(blocks: number): void {
    if (blocks > this.limits.maxBlocks) throw new ForkError("BLOCK_BUDGET", "canonical window block budget exceeded");
    if (blocks * BLOCK_REF_RETAINED_BYTES > this.limits.maxBytes)
      throw new ForkError("BYTE_BUDGET", "canonical window byte budget exceeded");
  }

  private position(hash: Hex.Hex): number {
    for (let index = this.start; index < this.storage.length; index += 1)
      if (this.storage[index]!.cursor.blockHash === hash) return index;
    return -1;
  }

  private compactAfterFinality(): void {
    if (this.start < 1024 || this.start * 2 < this.storage.length) return;
    this.storage = this.storage.slice(this.start);
    this.start = 0;
  }

  /** Internal exact-hash lookup used by the rare fork resolver. */
  findByHash(hash: Hex.Hex): { readonly block: BlockRef; readonly index: number } | undefined {
    const index = this.position(hash);
    return index < 0 ? undefined : { block: this.storage[index]!, index };
  }

  /** Internal branch copy used only while constructing a correction. */
  branchAfter(index: number): readonly BlockRef[] {
    return this.storage.slice(index + 1);
  }
}

/** Bounded exact-hash walker used only after a head-link discontinuity. */
export class ForkResolver {
  constructor(
    private readonly backend: RpcHttpBackend,
    private readonly maxDepth: number,
  ) {
    if (!positiveInteger(maxDepth)) throw new ForkError("INVALID_LIMITS", "fork depth must be positive");
  }

  /** Computes a correction plan without mutating the retained window. */
  async resolve(window: CanonicalWindow, input: BlockRef): Promise<ForkResolution> {
    const newTip = ownCompleteBlock(input);
    const oldTip = window.tip;
    if (oldTip === undefined) throw new ForkError("ANCESTOR_OUTSIDE_WINDOW", "canonical window is empty");
    validateSameChain(oldTip, newTip);
    if (newTip.cursor.chainId !== this.backend.chainId)
      throw new ForkError("INVALID_IDENTITY", "resolver chain id mismatch");
    if (sameBlock(oldTip, newTip))
      return { commonAncestor: oldTip, oldTip, newTip: oldTip, oldBranch: [], newBranch: [] };
    const finalized = window.finalized;
    if (
      finalized !== undefined &&
      newTip.cursor.blockNumber <= finalized.cursor.blockNumber &&
      newTip.cursor.blockHash !== finalized.cursor.blockHash
    )
      throw new ForkError("FINALIZED_CONFLICT", "replacement tip conflicts with finalized history");

    const descending: BlockRef[] = [newTip];
    let commonAncestor: BlockRef;
    let ancestorIndex: number;
    for (;;) {
      const child = descending[descending.length - 1]!;
      const parentHash = requiredParentHash(child);
      const retained = window.findByHash(parentHash);
      if (retained !== undefined) {
        if (child.cursor.blockNumber !== retained.block.cursor.blockNumber + 1n)
          throw new ForkError("DISCONNECTED", "replacement parent height is inconsistent");
        commonAncestor = retained.block;
        ancestorIndex = retained.index;
        break;
      }
      if (descending.length >= this.maxDepth) throw new ForkError("DEPTH_EXCEEDED", "fork resolution depth exceeded");
      if (finalized !== undefined && child.cursor.blockNumber <= finalized.cursor.blockNumber + 1n)
        throw new ForkError("FINALIZED_CONFLICT", "fork crosses the finalized watermark");
      if (child.cursor.blockNumber === 0n)
        throw new ForkError("ANCESTOR_OUTSIDE_WINDOW", "common ancestor is outside the retained window");
      let parent: BlockRef;
      try {
        parent = ownCompleteBlock(await this.backend.blockRefByHash(parentHash, child.cursor.commitment));
      } catch (error) {
        if (error instanceof ForkError) throw error;
        const detail = error instanceof Error ? error.message : String(error);
        throw new ForkError("RPC", `exact-hash fork lookup failed: ${detail}`, { cause: error });
      }
      if (parent.cursor.blockNumber + 1n !== child.cursor.blockNumber)
        throw new ForkError("DISCONNECTED", "replacement branch height is inconsistent");
      descending.push(parent);
    }

    const oldBranch = window.branchAfter(ancestorIndex);
    if (oldBranch.length > this.maxDepth) throw new ForkError("DEPTH_EXCEEDED", "abandoned branch is too deep");
    return {
      commonAncestor,
      oldTip,
      newTip,
      oldBranch,
      newBranch: descending.reverse(),
    };
  }
}

function positiveInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

function ownCompleteBlock(block: BlockRef): BlockRef {
  const blockHash = requiredHash(block);
  const parentHash = requiredParentHash(block);
  if (block.cursor.transactionIndex !== undefined || block.cursor.logIndex !== undefined)
    throw new ForkError("INVALID_IDENTITY", "block reference contains event coordinates");
  const cursor: ChainCursor = Object.freeze({ ...block.cursor, blockHash });
  return Object.freeze({ cursor, parentHash });
}

function requiredHash(block: BlockRef): Hex.Hex {
  if (block.cursor.blockHash === undefined) throw new ForkError("INVALID_IDENTITY", "block hash is absent");
  try {
    return parseHash(block.cursor.blockHash, "block hash");
  } catch (error) {
    throw new ForkError("INVALID_IDENTITY", "block hash is invalid", { cause: error });
  }
}

function requiredParentHash(block: BlockRef): Hex.Hex {
  if (block.parentHash === undefined) throw new ForkError("INVALID_IDENTITY", "parent hash is absent");
  try {
    return parseHash(block.parentHash, "parent hash");
  } catch (error) {
    throw new ForkError("INVALID_IDENTITY", "parent hash is invalid", { cause: error });
  }
}

function validateSameChain(left: BlockRef, right: BlockRef): void {
  if (left.cursor.chainId !== right.cursor.chainId) throw new ForkError("INVALID_IDENTITY", "block chain id mismatch");
}

function sameBlock(left: BlockRef, right: BlockRef): boolean {
  return (
    left.cursor.chainId === right.cursor.chainId &&
    left.cursor.blockNumber === right.cursor.blockNumber &&
    left.cursor.executionBlockNumber === right.cursor.executionBlockNumber &&
    left.cursor.blockHash === right.cursor.blockHash &&
    left.cursor.commitment === right.cursor.commitment &&
    left.cursor.sourceSequence === right.cursor.sourceSequence &&
    left.cursor.sourceSubIndex === right.cursor.sourceSubIndex &&
    left.parentHash === right.parentHash
  );
}

function sameBranch(left: readonly BlockRef[], right: readonly BlockRef[]): boolean {
  return left.length === right.length && left.every((block, index) => sameBlock(block, right[index]!));
}

/** Returns true when a correction leaves the retained tip unchanged. */
export function isNoopResolution(resolution: ForkResolution): boolean {
  return resolution.oldBranch.length === 0 && resolution.newBranch.length === 0;
}

/** Narrows transport failures when callers need separate health metrics. */
export function isForkRpcError(error: unknown): boolean {
  return error instanceof ForkError && error.code === "RPC" && error.cause instanceof RpcError;
}
