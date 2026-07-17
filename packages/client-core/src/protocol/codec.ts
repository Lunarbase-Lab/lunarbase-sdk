/** Versioned cross-language checkpoint and update codec. */
import type { Address, ChainCursor, Checkpoint, QuoteState, ChainUpdate } from "../model.js";
import { Commitment as CommitmentValue } from "../model.js";
import type { LaneState } from "@lunarbase/math";

const MAGIC = new Uint8Array([0x4c, 0x42, 0x51, 0x31]);
/** Decodes exactly `length` bytes from a hexadecimal string. */
export function hexBytes(value: string, length: number): Uint8Array {
  const normalized = value.startsWith("0x") ? value.slice(2) : value;
  if (normalized.length !== length * 2 || !/^[0-9a-f]+$/i.test(normalized))
    throw new Error(`expected ${length}-byte hex value`);
  const result = new Uint8Array(length);
  for (let i = 0; i < length; i += 1) result[i] = Number.parseInt(normalized.slice(i * 2, i * 2 + 2), 16);
  return result;
}
/** Encodes bytes as lowercase `0x`-prefixed hexadecimal. */
export function hexString(value: Uint8Array): string {
  return `0x${[...value].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}
/** Encodes an unsigned bigint into a fixed-width big-endian byte sequence. */
export function fixedBigint(value: bigint, bytes: number): Uint8Array {
  if (value < 0n || value > (1n << BigInt(bytes * 8)) - 1n) throw new Error("integer does not fit fixed width");
  return hexBytes(value.toString(16).padStart(bytes * 2, "0"), bytes);
}
/** Converts a bounded bigint to a JavaScript number without precision loss. */
export function fixedNumber(value: bigint, max: number): number {
  if (value < 0n || value > BigInt(max)) throw new Error("integer does not fit fixed width");
  return Number(value);
}

class Writer {
  private readonly parts: Uint8Array[] = [];
  bytes(value: Uint8Array): void {
    this.parts.push(value);
  }
  u8(value: number): void {
    this.bytes(new Uint8Array([value]));
  }
  bool(value: boolean): void {
    this.u8(value ? 1 : 0);
  }
  u16(value: number): void {
    const data = new Uint8Array(2);
    new DataView(data.buffer).setUint16(0, value, false);
    this.bytes(data);
  }
  u32(value: number): void {
    const data = new Uint8Array(4);
    new DataView(data.buffer).setUint32(0, value, false);
    this.bytes(data);
  }
  u64(value: bigint): void {
    const data = new Uint8Array(8);
    new DataView(data.buffer).setBigUint64(0, value, false);
    this.bytes(data);
  }
  u256(value: bigint): void {
    this.bytes(fixedBigint(value, 32));
  }
  string(value: string): void {
    const data = new TextEncoder().encode(value);
    this.u32(data.length);
    this.bytes(data);
  }
  optional<T>(value: T | undefined, write: (value: T) => void): void {
    this.bool(value !== undefined);
    if (value !== undefined) write(value);
  }
  cursor(cursor: ChainCursor): void {
    this.u64(cursor.chainId);
    this.u64(cursor.blockNumber);
    this.optional(cursor.blockHash, (value) => this.bytes(hexBytes(value, 32)));
    this.optional(cursor.transactionIndex, (value) => this.u32(fixedNumber(value, 0xffff_ffff)));
    this.optional(cursor.logIndex, (value) => this.u32(fixedNumber(value, 0xffff_ffff)));
    this.optional(cursor.sourceSequence, (value) => this.u64(value));
    this.optional(cursor.sourceSubIndex, (value) => this.u32(fixedNumber(value, 0xffff_ffff)));
    this.u8(
      cursor.commitment === CommitmentValue.Realtime ? 0 : cursor.commitment === CommitmentValue.Canonical ? 1 : 2,
    );
  }
  state(state: QuoteState): void {
    this.bytes(hexBytes(state.cash, 20));
    this.u64(state.stateVersion);
    this.u256(state.blacklistFeeMultiplier);
    const lanes = [...state.lanes].sort(([left], [right]) => left.toLowerCase().localeCompare(right.toLowerCase()));
    this.u32(lanes.length);
    for (const [asset, lane] of lanes) {
      this.bytes(hexBytes(asset, 20));
      this.u256(lane.slot0);
      this.bool(lane.exists);
      this.bool(lane.paused);
      this.u8(fixedNumber(lane.blockDelay, 0xff));
      this.u32(fixedNumber(lane.slippageKBps, 0xffff_ffff));
    }
    const principals = [...state.totalPrincipalAmount].sort(([left], [right]) =>
      left.toLowerCase().localeCompare(right.toLowerCase()),
    );
    this.u32(principals.length);
    for (const [asset, amount] of principals) {
      this.bytes(hexBytes(asset, 20));
      this.u256(amount);
    }
    const whitelist = [...state.whitelist].sort(([left], [right]) =>
      left.toLowerCase().localeCompare(right.toLowerCase()),
    );
    this.u32(whitelist.length);
    for (const [router, value] of whitelist) {
      this.bytes(hexBytes(router, 20));
      this.bool(value);
    }
    const partners = [...state.partnerFeeBps]
      .map(([key, fee]) => {
        const [router, asset] = key.split(":");
        return { router, asset, fee };
      })
      .sort((left, right) => `${left.router}:${left.asset}`.localeCompare(`${right.router}:${right.asset}`));
    this.u32(partners.length);
    for (const partner of partners) {
      this.bytes(hexBytes(partner.router, 20));
      this.bytes(hexBytes(partner.asset, 20));
      this.u256(partner.fee);
    }
  }
  finish(magic = false): Uint8Array {
    const length = this.parts.reduce((total, part) => total + part.length, magic ? 4 : 0);
    const result = new Uint8Array(length);
    let offset = 0;
    if (magic) {
      result.set(MAGIC);
      offset = 4;
    }
    for (const part of this.parts) {
      result.set(part, offset);
      offset += part.length;
    }
    return result;
  }
}
class Reader {
  private offset = 0;
  constructor(
    private readonly value: Uint8Array,
    magic = false,
  ) {
    if (magic && (value.length < 4 || value.slice(0, 4).some((byte, i) => byte !== MAGIC[i])))
      throw new Error("invalid checkpoint codec magic");
    if (magic) this.offset = 4;
  }
  bytes(length: number): Uint8Array {
    const result = this.value.slice(this.offset, this.offset + length);
    if (result.length !== length) throw new Error("truncated binary payload");
    this.offset += length;
    return result;
  }
  u8(): number {
    return this.bytes(1)[0];
  }
  bool(): boolean {
    const value = this.u8();
    if (value > 1) throw new Error("invalid boolean");
    return value === 1;
  }
  u16(): number {
    return new DataView(this.bytes(2).buffer).getUint16(0, false);
  }
  u32(): number {
    return new DataView(this.bytes(4).buffer).getUint32(0, false);
  }
  u64(): bigint {
    return new DataView(this.bytes(8).buffer).getBigUint64(0, false);
  }
  u256(): bigint {
    return BigInt(`0x${hexString(this.bytes(32)).slice(2)}`);
  }
  string(): string {
    return new TextDecoder().decode(this.bytes(this.u32()));
  }
  optional<T>(read: () => T): T | undefined {
    return this.bool() ? read() : undefined;
  }
  cursor(): ChainCursor {
    const chainId = this.u64();
    const blockNumber = this.u64();
    const blockHash = this.optional(() => hexString(this.bytes(32)));
    const transactionIndex = this.optional(() => BigInt(this.u32()));
    const logIndex = this.optional(() => BigInt(this.u32()));
    const sourceSequence = this.optional(() => this.u64());
    const sourceSubIndex = this.optional(() => BigInt(this.u32()));
    const value = this.u8();
    if (value > 2) throw new Error("invalid commitment");
    return {
      chainId,
      blockNumber,
      blockHash,
      transactionIndex,
      logIndex,
      sourceSequence,
      sourceSubIndex,
      commitment:
        value === 0 ? CommitmentValue.Realtime : value === 1 ? CommitmentValue.Canonical : CommitmentValue.Finalized,
    };
  }
  state(): QuoteState {
    const cash = hexString(this.bytes(20));
    const stateVersion = this.u64();
    const blacklistFeeMultiplier = this.u256();
    const state: {
      cash: Address;
      lanes: Map<Address, LaneState>;
      totalPrincipalAmount: Map<Address, bigint>;
      whitelist: Map<Address, boolean>;
      blacklistFeeMultiplier: bigint;
      partnerFeeBps: Map<string, bigint>;
      stateVersion: bigint;
    } = {
      cash,
      lanes: new Map(),
      totalPrincipalAmount: new Map(),
      whitelist: new Map(),
      blacklistFeeMultiplier,
      partnerFeeBps: new Map(),
      stateVersion,
    };
    for (let i = 0, length = this.u32(); i < length; i += 1) {
      const asset = hexString(this.bytes(20));
      state.lanes.set(asset, {
        slot0: this.u256(),
        exists: this.bool(),
        paused: this.bool(),
        blockDelay: BigInt(this.u8()),
        slippageKBps: BigInt(this.u32()),
      });
    }
    for (let i = 0, length = this.u32(); i < length; i += 1)
      state.totalPrincipalAmount.set(hexString(this.bytes(20)), this.u256());
    for (let i = 0, length = this.u32(); i < length; i += 1)
      state.whitelist.set(hexString(this.bytes(20)), this.bool());
    for (let i = 0, length = this.u32(); i < length; i += 1)
      state.partnerFeeBps.set(`${hexString(this.bytes(20))}:${hexString(this.bytes(20))}`, this.u256());
    return state;
  }
  done(): boolean {
    return this.offset === this.value.length;
  }
}
/** Encodes a compatibility-checked checkpoint with the Rust-compatible format. */
export function encodeCheckpoint(checkpoint: Checkpoint): Uint8Array {
  const writer = new Writer();
  writer.u16(Number(checkpoint.schemaVersion));
  writer.string(checkpoint.mathCompatibilityVersion);
  writer.bytes(hexBytes(checkpoint.expectedRuntimeCodeHash, 32));
  writer.cursor(checkpoint.cursor);
  writer.state(checkpoint.state);
  return writer.finish(true);
}
/** Decodes and validates a complete Rust-compatible checkpoint payload. */
export function decodeCheckpoint(bytes: Uint8Array): Checkpoint {
  const reader = new Reader(bytes, true);
  const schemaVersion = BigInt(reader.u16());
  const mathCompatibilityVersion = reader.string();
  const expectedRuntimeCodeHash = hexString(reader.bytes(32));
  const cursor = reader.cursor();
  const state = reader.state();
  if (!reader.done()) throw new Error("trailing checkpoint bytes");
  return { schemaVersion, mathCompatibilityVersion, expectedRuntimeCodeHash, cursor, state };
}

/** Encodes one normalized chain update for the bounded persistence stream. */
export function encodeUpdate(update: ChainUpdate): Uint8Array {
  const writer = new Writer();
  const cursor = (value: ChainCursor) => writer.cursor(value);
  switch (update.kind) {
    case "Head":
      writer.u8(0);
      cursor(update.cursor);
      break;
    case "Log": {
      writer.u8(1);
      writer.bytes(hexBytes(update.log.address, 20));
      writer.u32(update.log.topics.length);
      for (const topic of update.log.topics) writer.u256(topic);
      const data = hexBytes(update.log.data, (update.log.data.length - 2) / 2);
      writer.u32(data.length);
      writer.bytes(data);
      writer.bool(update.log.removed);
      cursor(update.log.cursor);
      break;
    }
    case "Reorg":
      writer.u8(2);
      cursor(update.oldHead);
      cursor(update.newHead);
      break;
    case "Gap":
      writer.u8(3);
      writer.optional(update.cursor, cursor);
      writer.string(update.reason);
      break;
    case "SourceHealth":
      writer.u8(4);
      writer.bool(update.healthy);
      writer.string(update.detail);
      break;
  }
  return writer.finish();
}
/** Decodes one normalized update and rejects truncated or trailing bytes. */
export function decodeUpdate(bytes: Uint8Array): ChainUpdate {
  const reader = new Reader(bytes);
  const variant = reader.u8();
  let result: ChainUpdate;
  switch (variant) {
    case 0:
      result = { kind: "Head", cursor: reader.cursor() };
      break;
    case 1: {
      const address = hexString(reader.bytes(20));
      const topics = Array.from({ length: reader.u32() }, () => reader.u256());
      const data = hexString(reader.bytes(reader.u32()));
      const removed = reader.bool();
      result = { kind: "Log", log: { address, topics, data, removed, cursor: reader.cursor() } };
      break;
    }
    case 2:
      result = { kind: "Reorg", oldHead: reader.cursor(), newHead: reader.cursor() };
      break;
    case 3:
      result = { kind: "Gap", cursor: reader.optional(() => reader.cursor()), reason: reader.string() };
      break;
    case 4:
      result = { kind: "SourceHealth", healthy: reader.bool(), detail: reader.string() };
      break;
    default:
      throw new Error("invalid update variant");
  }
  if (!reader.done()) throw new Error("trailing update bytes");
  return result;
}
