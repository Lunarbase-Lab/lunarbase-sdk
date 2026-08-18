/** Cursor identity and coverage policy shared by bootstrap and live reduction. */
import type { Address } from "@lunarbase-lab/pmm-v2-math";
import { Commitment, commitmentRank, IndexerError } from "../model.js";
import type { ChainCursor, ContractLog } from "../model.js";
import { compareCursor } from "../source.js";

/** Validates normalized log identity before any ordering shortcut or decode. */
export function validateCoreLogIdentity(log: ContractLog, expectedCore: Address, expectedChainId: bigint): void {
  if (log.address !== expectedCore)
    throw new IndexerError("REDUCER", "contract log address does not match deployment Core");
  if (log.cursor.chainId !== expectedChainId)
    throw new IndexerError("REDUCER", "contract log cursor chain id mismatch");
}

/** Determines whether a snapshot already represents an update's ordered position and confidence. */
export function snapshotCovers(update: ChainCursor, snapshot: ChainCursor): boolean {
  if (update.chainId !== snapshot.chainId) throw new IndexerError("REDUCER", "cursor chain id mismatch");
  if (update.blockNumber < snapshot.blockNumber) return true;
  if (update.blockNumber > snapshot.blockNumber) return false;
  if (update.blockHash === undefined || snapshot.blockHash === undefined)
    throw new IndexerError("GAP", "same-block handoff has no hash identity; canonical recovery required");
  if (update.blockHash.toLowerCase() !== snapshot.blockHash.toLowerCase()) return false;
  if (update.executionBlockNumber !== snapshot.executionBlockNumber)
    throw new IndexerError("GAP", "same-block handoff execution context mismatch; canonical recovery required");
  return commitmentRank(snapshot.commitment) >= commitmentRank(update.commitment);
}

/** Rejects reapplication below a complete canonical state boundary. */
export function canonicalFloorCoversLog(update: ChainCursor, floor: ChainCursor): boolean {
  if (update.chainId !== floor.chainId) throw new IndexerError("REDUCER", "cursor chain id mismatch");
  if (update.blockNumber < floor.blockNumber) return true;
  if (update.blockNumber > floor.blockNumber) return false;
  if (update.blockHash === undefined || floor.blockHash === undefined)
    throw new IndexerError("GAP", "same-block realtime log has no canonical hash identity");
  if (update.blockHash.toLowerCase() !== floor.blockHash.toLowerCase())
    throw new IndexerError("REDUCER", "block hash mismatch");
  if (update.executionBlockNumber !== floor.executionBlockNumber)
    throw new IndexerError("GAP", "same-block realtime log execution context mismatch");
  const floorIsBlockComplete = floor.transactionIndex === undefined && floor.logIndex === undefined;
  return floorIsBlockComplete || compareCursor(update, floor) <= 0;
}

/** Requires an externally supplied stable floor to match the reducer state exactly. */
export function canonicalFloorMatchesCurrent(floor: ChainCursor, current: ChainCursor): boolean {
  if (
    floor.chainId !== current.chainId ||
    floor.blockNumber !== current.blockNumber ||
    floor.executionBlockNumber !== current.executionBlockNumber ||
    !floor.blockHash ||
    !current.blockHash ||
    floor.blockHash.toLowerCase() !== current.blockHash.toLowerCase() ||
    commitmentRank(floor.commitment) > commitmentRank(current.commitment)
  )
    return false;
  const floorPositioned = floor.transactionIndex !== undefined || floor.logIndex !== undefined;
  if (!floorPositioned) return true;
  return (
    floor.transactionIndex !== undefined &&
    floor.logIndex !== undefined &&
    floor.transactionIndex === current.transactionIndex &&
    floor.logIndex === current.logIndex
  );
}

/** Tests immutable block identity without allocating normalized strings. */
export function cursorHasIdentity(current: ChainCursor | undefined, observed: ChainCursor): boolean {
  return current !== undefined && sameCursorIdentity(current, observed);
}

/** Tests whether a published cursor is at or beyond a correction tip. */
export function cursorCoversCorrectionTip(current: ChainCursor, tip: ChainCursor): boolean {
  if (current.chainId !== tip.chainId || current.blockNumber < tip.blockNumber) return false;
  return current.blockNumber > tip.blockNumber || sameCursorIdentity(current, tip);
}

/** Compares immutable block and execution identity. */
export function sameCursorIdentity(left: ChainCursor, right: ChainCursor): boolean {
  return (
    left.chainId === right.chainId &&
    left.blockNumber === right.blockNumber &&
    left.executionBlockNumber === right.executionBlockNumber &&
    left.blockHash !== undefined &&
    right.blockHash !== undefined &&
    left.blockHash.toLowerCase() === right.blockHash.toLowerCase()
  );
}

const U32_MAX = (1n << 32n) - 1n;
const U64_MAX = (1n << 64n) - 1n;

/** Validates the block-level identity used as a complete source snapshot boundary. */
export function validateSnapshotCursor(cursor: ChainCursor, expectedChainId: bigint): void {
  if (cursor.chainId !== expectedChainId) throw new IndexerError("SOURCE", "snapshot cursor chain id mismatch");
  if (!isUint(cursor.blockNumber, U64_MAX)) throw new IndexerError("SOURCE", "snapshot block is not uint64");
  if (!isUint(cursor.executionBlockNumber, U64_MAX))
    throw new IndexerError("SOURCE", "snapshot execution block is not uint64");
  const hash = cursor.blockHash;
  if (hash === undefined || !/^0x[0-9a-f]{64}$/i.test(hash) || /^0x0{64}$/i.test(hash))
    throw new IndexerError("SOURCE", "snapshot cursor requires a non-zero block hash");
  if (cursor.transactionIndex !== undefined || cursor.logIndex !== undefined)
    throw new IndexerError("SOURCE", "snapshot cursor must use a block-level position");
  if (
    cursor.commitment !== Commitment.Realtime &&
    cursor.commitment !== Commitment.Canonical &&
    cursor.commitment !== Commitment.Finalized
  )
    throw new IndexerError("SOURCE", "snapshot cursor commitment is invalid");
  if (cursor.sourceSequence !== undefined && !isUint(cursor.sourceSequence, U64_MAX))
    throw new IndexerError("SOURCE", "snapshot source sequence is not uint64");
  if (cursor.sourceSubIndex !== undefined && !isUint(cursor.sourceSubIndex, U32_MAX))
    throw new IndexerError("SOURCE", "snapshot source sub-index is not uint32");
  if (cursor.sourceSubIndex !== undefined && cursor.sourceSequence === undefined)
    throw new IndexerError("SOURCE", "snapshot source sub-index requires a source sequence");
}

function isUint(value: unknown, maximum: bigint): value is bigint {
  return typeof value === "bigint" && value >= 0n && value <= maximum;
}
