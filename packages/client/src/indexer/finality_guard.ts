/** Monotonic finalized identity policy shared by correction and recovery. */
import { Commitment, IndexerError } from "../model.js";
import type { ChainCorrection, ChainCursor, Checkpoint } from "../model.js";

export class FinalityGuard {
  private floor?: ChainCursor;

  /** Retains a finalized observation and rejects identity equivocation. */
  observe(cursor: ChainCursor): boolean {
    if (cursor.commitment !== Commitment.Finalized) return false;
    if (!isNonzeroHash(cursor.blockHash))
      throw new IndexerError("GAP", "finalized cursor requires a non-zero block hash");
    if (this.floor && cursor.blockNumber < this.floor.blockNumber) return false;
    if (this.floor && cursor.blockNumber === this.floor.blockNumber && !sameIdentity(cursor, this.floor))
      throw new IndexerError("GAP", "conflicting finalized block identity");
    this.floor = { ...cursor, transactionIndex: undefined, logIndex: undefined };
    return true;
  }

  /** Copies a proven floor into a fresh recovery candidate. */
  retain(cursor: ChainCursor | undefined): void {
    if (cursor) this.observe(cursor);
  }

  /** Returns a detached finalized cursor. */
  cursor(): ChainCursor | undefined {
    return this.floor && { ...this.floor };
  }

  /** Rejects a correction whose ancestor crosses finalized history. */
  validateCorrection(correction: ChainCorrection): void {
    if (!this.floor) return;
    const ancestor = correction.commonAncestor.cursor;
    if (ancestor.blockNumber < this.floor.blockNumber)
      throw new IndexerError("GAP", "correction would roll back finalized state");
    if (ancestor.blockNumber === this.floor.blockNumber && !sameIdentity(ancestor, this.floor))
      throw new IndexerError("GAP", "correction ancestor conflicts with finalized state");
  }

  /** Ensures a replacement snapshot cannot regress or weaken finality. */
  validateSnapshot(cursor: ChainCursor): void {
    if (!this.floor) return;
    if (cursor.blockNumber < this.floor.blockNumber)
      throw new IndexerError("GAP", "recovery snapshot regresses finalized state");
    if (cursor.blockNumber !== this.floor.blockNumber) return;
    if (!sameIdentity(cursor, this.floor))
      throw new IndexerError("INVALID_REQUEST", "recovery snapshot conflicts with finalized state");
    if (cursor.commitment !== Commitment.Finalized)
      throw new IndexerError("GAP", "recovery snapshot weakens finalized commitment");
  }

  /** Builds an identity-only proof using deployment fields from a stable checkpoint. */
  checkpoint(checkpoint: Checkpoint | undefined): Checkpoint | undefined {
    return checkpoint && this.floor ? { ...checkpoint, cursor: { ...this.floor } } : undefined;
  }
}

function sameIdentity(left: ChainCursor, right: ChainCursor): boolean {
  return (
    left.chainId === right.chainId &&
    left.blockNumber === right.blockNumber &&
    left.executionBlockNumber === right.executionBlockNumber &&
    left.blockHash !== undefined &&
    right.blockHash !== undefined &&
    left.blockHash.toLowerCase() === right.blockHash.toLowerCase()
  );
}

function isNonzeroHash(hash: string | undefined): hash is string {
  return hash !== undefined && /^0x[0-9a-f]{64}$/i.test(hash) && !/^0x0{64}$/i.test(hash);
}
