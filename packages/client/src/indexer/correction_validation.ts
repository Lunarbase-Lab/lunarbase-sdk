/** Strict identity and resource validation for resolved fork corrections. */
import type { Address } from "@lunarbase-lab/pmm-v2-math";
import { chainCorrectionProtocolBytes, Commitment, IndexerError } from "../model.js";
import type { BlockRef, ChainCorrection, ChainCursor, ContractLog } from "../model.js";
import { compareCursor } from "../source.js";
import {
  MAX_CORRECTION_HISTORY_BLOCKS as MAX_BRANCH_BLOCKS,
  MAX_CORRECTION_HISTORY_BYTES as MAX_RETAINED_BYTES,
} from "./correction_journal.js";

const MAX_REPLACEMENT_LOGS = 8_192;
const U32_MAX = (1n << 32n) - 1n;
const U64_MAX = (1n << 64n) - 1n;

/** Rejects an invalid envelope and determines whether live state needs replacement. */
export function validateCorrection(
  correction: ChainCorrection,
  current: ChainCursor | undefined,
  expectedCore: Address,
  expectedChainId: bigint,
): boolean {
  validateCorrectionEnvelope(correction, expectedCore, expectedChainId);
  return validateCorrectionState(correction, current);
}

/** Matches an already validated envelope against the current published state. */
export function validateCorrectionState(correction: ChainCorrection, current: ChainCursor | undefined): boolean {
  if (!current) gap("correction arrived before quote state bootstrap");
  if (sameIdentity(current!, correction.newTip.cursor)) return false;
  if (
    current!.commitment === Commitment.Finalized ||
    correction.oldTip.cursor.commitment === Commitment.Finalized ||
    correction.oldBranch.some((block) => block.cursor.commitment === Commitment.Finalized)
  )
    gap("correction cannot replace finalized branch state");
  assertIdentity(current!, correction.oldTip.cursor, "correction old tip does not match published state");
  return true;
}

/** Validates a bounded correction envelope without matching it to live state. */
export function validateCorrectionEnvelope(
  correction: ChainCorrection,
  expectedCore: Address,
  expectedChainId: bigint,
): void {
  if (!isRecord(correction)) gap("correction envelope is not an object");
  if (
    !Array.isArray(correction.oldBranch) ||
    !Array.isArray(correction.newBranch) ||
    !Array.isArray(correction.replacementLogs)
  )
    gap("correction branch and log collections must be arrays");
  if (correction.oldBranch.length > MAX_BRANCH_BLOCKS || correction.newBranch.length > MAX_BRANCH_BLOCKS)
    gap("correction branch exceeds 128-block protocol limit");
  if (correction.replacementLogs.length > MAX_REPLACEMENT_LOGS) gap("correction exceeds 8192-log protocol limit");

  for (const block of [correction.commonAncestor, correction.oldTip, correction.newTip])
    validateBlock(block, expectedChainId);
  if (sameChainHeightHash(correction.oldTip.cursor, correction.newTip.cursor))
    gap("correction old and new tips have the same block identity");
  validateBranch(correction.commonAncestor, correction.oldTip, correction.oldBranch);
  validateBranch(correction.commonAncestor, correction.newTip, correction.newBranch);
  validateHashBindings(correction);
  validateLogs(correction, expectedCore, expectedChainId);
  if (chainCorrectionProtocolBytes(correction) > MAX_RETAINED_BYTES)
    gap("correction exceeds 16 MiB retained-byte protocol limit");
}

function validateBranch(ancestor: BlockRef, tip: BlockRef, branch: readonly BlockRef[]): void {
  if (branch.length === 0) {
    assertBlockRef(ancestor, tip, "empty correction branch does not end at its tip");
    return;
  }
  let parent = ancestor;
  for (const block of branch) {
    validateBlock(block, ancestor.cursor.chainId);
    if (block.cursor.blockNumber !== parent.cursor.blockNumber + 1n) gap("correction branch is not block-contiguous");
    if (!parent.cursor.blockHash || !block.parentHash || !same(block.parentHash, parent.cursor.blockHash))
      gap("correction branch parent linkage is invalid");
    parent = block;
  }
  assertBlockRef(parent, tip, "correction branch does not end at its declared tip");
}

function validateHashBindings(correction: ChainCorrection): void {
  const seen = new Map<string, BlockRef>();
  const bind = (block: BlockRef): void => {
    const key = block.cursor.blockHash!.toLowerCase();
    const previous = seen.get(key);
    if (
      previous &&
      (previous.cursor.chainId !== block.cursor.chainId ||
        previous.cursor.blockNumber !== block.cursor.blockNumber ||
        previous.cursor.executionBlockNumber !== block.cursor.executionBlockNumber ||
        !sameOptionalHash(previous.parentHash, block.parentHash))
    )
      gap("correction reuses a block hash with conflicting identity");
    seen.set(key, block);
  };

  bind(correction.commonAncestor);
  for (const block of correction.oldBranch) bind(block);
  for (const block of correction.newBranch) bind(block);
}

function validateLogs(correction: ChainCorrection, core: Address, chainId: bigint): void {
  const newBlocks = new Map<bigint, BlockRef>();
  for (const block of correction.newBranch) newBlocks.set(block.cursor.blockNumber, block);
  let previous: ChainCursor | undefined;
  for (const log of correction.replacementLogs) {
    validateLogShape(log);
    if (log.address.toLowerCase() !== core.toLowerCase()) gap("correction log address does not match deployment Core");
    if (log.cursor.chainId !== chainId) gap("correction log chain id mismatch");
    if (log.removed) gap("correction replacement contains a removed log");
    const hasTransactionIndex = log.cursor.transactionIndex !== undefined;
    const hasLogIndex = log.cursor.logIndex !== undefined;
    if (hasTransactionIndex !== hasLogIndex) gap("correction replacement log has an incomplete transaction position");
    if (!hasTransactionIndex && log.cursor.sourceSequence === undefined)
      gap("correction replacement log has no deterministic ordering identity");
    const expectedBlock = newBlocks.get(log.cursor.blockNumber);
    if (!expectedBlock || !log.cursor.blockHash || !same(expectedBlock.cursor.blockHash!, log.cursor.blockHash))
      gap("correction replacement log is outside the declared new branch");
    if (log.cursor.executionBlockNumber !== expectedBlock.cursor.executionBlockNumber)
      gap("correction replacement log execution block does not match its branch");
    if (log.cursor.commitment !== expectedBlock.cursor.commitment)
      gap("correction replacement log commitment does not match its branch");
    if (previous && compareCursor(log.cursor, previous) <= 0)
      gap("correction replacement logs are not strictly ordered");
    previous = log.cursor;
  }
}

function validateLogShape(log: ContractLog): void {
  if (!isRecord(log) || !isRecord(log.cursor)) gap("correction replacement log is malformed");
  validateCursorShape(log.cursor, "correction log cursor");
  if (!isAddress(log.address)) gap("correction replacement log address is malformed");
  if (!Array.isArray(log.topics) || log.topics.some((topic) => !isB256(topic)))
    gap("correction replacement log topics are malformed");
  if (!isDataHex(log.data)) gap("correction replacement log data is malformed");
  if (typeof log.removed !== "boolean") gap("correction replacement log removed flag is malformed");
  if (!log.cursor.blockHash || isZeroB256(log.cursor.blockHash))
    gap("correction replacement log has an invalid block hash");
}

function validateBlock(block: BlockRef, chainId: bigint): void {
  if (!isRecord(block) || !isRecord(block.cursor)) gap("correction block is malformed");
  validateCursorShape(block.cursor, "correction block cursor");
  if (block.cursor.chainId !== chainId) gap("correction block chain id mismatch");
  const hash = block.cursor.blockHash;
  if (!hash) gap("correction block has no hash identity");
  if (isZeroB256(hash)) gap("correction block has an invalid hash identity");
  if (block.cursor.transactionIndex !== undefined || block.cursor.logIndex !== undefined)
    gap("correction BlockRef must use a block-level cursor");
  if (block.parentHash !== undefined && !isB256(block.parentHash)) gap("correction block parent hash is malformed");
}

function validateCursorShape(cursor: ChainCursor, label: string): void {
  if (!isRecord(cursor)) gap(`${label} is malformed`);
  if (!isUint(cursor.chainId, U64_MAX)) gap(`${label} chain id is not uint64`);
  if (!isUint(cursor.blockNumber, U64_MAX)) gap(`${label} block number is not uint64`);
  if (!isUint(cursor.executionBlockNumber, U64_MAX)) gap(`${label} execution block is not uint64`);
  if (cursor.blockHash !== undefined && !isB256(cursor.blockHash)) gap(`${label} block hash is malformed`);
  validateOptionalUint(cursor.transactionIndex, U32_MAX, `${label} transaction index is not uint32`);
  validateOptionalUint(cursor.logIndex, U32_MAX, `${label} log index is not uint32`);
  validateOptionalUint(cursor.sourceSequence, U64_MAX, `${label} source sequence is not uint64`);
  validateOptionalUint(cursor.sourceSubIndex, U32_MAX, `${label} source sub-index is not uint32`);
  if (
    cursor.commitment !== Commitment.Realtime &&
    cursor.commitment !== Commitment.Canonical &&
    cursor.commitment !== Commitment.Finalized
  )
    gap(`${label} commitment is invalid`);
}

function validateOptionalUint(value: unknown, maximum: bigint, message: string): void {
  if (value !== undefined && !isUint(value, maximum)) gap(message);
}

function isUint(value: unknown, maximum: bigint): value is bigint {
  return typeof value === "bigint" && value >= 0n && value <= maximum;
}

function isAddress(value: unknown): value is string {
  return typeof value === "string" && /^0x[0-9a-f]{40}$/i.test(value);
}

function isB256(value: unknown): value is string {
  return typeof value === "string" && /^0x[0-9a-f]{64}$/i.test(value);
}

function isZeroB256(value: string): boolean {
  return /^0x0{64}$/i.test(value);
}

function isDataHex(value: unknown): value is string {
  return typeof value === "string" && /^0x(?:[0-9a-f]{2})*$/i.test(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object";
}

function assertIdentity(left: ChainCursor, right: ChainCursor, message: string): void {
  if (!sameIdentity(left, right)) gap(message);
}

function assertBlockRef(left: BlockRef, right: BlockRef, message: string): void {
  const a = left.cursor;
  const b = right.cursor;
  if (
    !sameIdentity(a, b) ||
    a.transactionIndex !== b.transactionIndex ||
    a.logIndex !== b.logIndex ||
    a.sourceSequence !== b.sourceSequence ||
    a.sourceSubIndex !== b.sourceSubIndex ||
    a.commitment !== b.commitment ||
    !sameOptionalHash(left.parentHash, right.parentHash)
  )
    gap(message);
}

function sameChainHeightHash(left: ChainCursor, right: ChainCursor): boolean {
  return (
    left.chainId === right.chainId &&
    left.blockNumber === right.blockNumber &&
    left.blockHash !== undefined &&
    right.blockHash !== undefined &&
    same(left.blockHash, right.blockHash)
  );
}

function sameIdentity(left: ChainCursor, right: ChainCursor): boolean {
  return !(
    left.chainId !== right.chainId ||
    left.blockNumber !== right.blockNumber ||
    !left.blockHash ||
    left.executionBlockNumber !== right.executionBlockNumber ||
    !right.blockHash ||
    !same(left.blockHash, right.blockHash)
  );
}

function same(left: string, right: string): boolean {
  return left.toLowerCase() === right.toLowerCase();
}

function sameOptionalHash(left: string | undefined, right: string | undefined): boolean {
  return left === undefined ? right === undefined : right !== undefined && same(left, right);
}

function gap(message: string): never {
  throw new IndexerError("GAP", message);
}
