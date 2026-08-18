/** Deterministic bounded-memory identity for an applied correction envelope. */
import { keccak_256 } from "@noble/hashes/sha3";
import type { BlockRef, ChainCorrection, ChainCursor, ContractLog } from "../model.js";

const ENCODER = new TextEncoder();
const TEXT_CHUNK_CHARACTERS = 4_096;
type HashState = ReturnType<typeof keccak_256.create>;

/** Hashes every normalized correction field without materializing one giant buffer. */
export function correctionFingerprint(correction: ChainCorrection): string {
  const hash = keccak_256.create();
  writeText(hash, "lunarbase-correction-v1");
  writeBlock(hash, correction.commonAncestor);
  writeBlock(hash, correction.oldTip);
  writeBlock(hash, correction.newTip);
  writeBlocks(hash, correction.oldBranch);
  writeBlocks(hash, correction.newBranch);
  writeNumber(hash, correction.replacementLogs.length);
  for (const log of correction.replacementLogs) writeLog(hash, log);
  return toHex(hash.digest());
}

function writeBlocks(hash: HashState, blocks: readonly BlockRef[]): void {
  writeNumber(hash, blocks.length);
  for (const block of blocks) writeBlock(hash, block);
}

function writeBlock(hash: HashState, block: BlockRef): void {
  writeCursor(hash, block.cursor);
  writeOptionalHex(hash, block.parentHash);
}

function writeLog(hash: HashState, log: ContractLog): void {
  writeHex(hash, log.address);
  writeNumber(hash, log.topics.length);
  for (const topic of log.topics) writeHex(hash, topic);
  writeHex(hash, log.data);
  writeText(hash, log.removed ? "1" : "0");
  writeCursor(hash, log.cursor);
}

function writeCursor(hash: HashState, cursor: ChainCursor): void {
  writeBigint(hash, cursor.chainId);
  writeBigint(hash, cursor.blockNumber);
  writeBigint(hash, cursor.executionBlockNumber);
  writeOptionalHex(hash, cursor.blockHash);
  writeOptionalBigint(hash, cursor.transactionIndex);
  writeOptionalBigint(hash, cursor.logIndex);
}

function writeBigint(hash: HashState, value: bigint): void {
  writeText(hash, value.toString(10));
}

function writeOptionalBigint(hash: HashState, value: bigint | undefined): void {
  writeText(hash, value === undefined ? "none" : `some:${value.toString(10)}`);
}

function writeNumber(hash: HashState, value: number): void {
  writeText(hash, value.toString(10));
}

function writeOptionalHex(hash: HashState, value: string | undefined): void {
  if (value === undefined) writeText(hash, "none");
  else {
    writeText(hash, "some");
    writeHex(hash, value);
  }
}

function writeHex(hash: HashState, value: string): void {
  writeText(hash, value, true);
}

function writeText(hash: HashState, value: string, lowercase = false): void {
  hash.update(ENCODER.encode(`${value.length}:`));
  for (let offset = 0; offset < value.length; offset += TEXT_CHUNK_CHARACTERS) {
    const chunk = value.slice(offset, offset + TEXT_CHUNK_CHARACTERS);
    hash.update(ENCODER.encode(lowercase ? chunk.toLowerCase() : chunk));
  }
}

function toHex(bytes: Uint8Array): string {
  let value = "0x";
  for (const byte of bytes) value += byte.toString(16).padStart(2, "0");
  return value;
}
