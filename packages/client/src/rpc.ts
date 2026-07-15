import { assertU256, parseAddress, U128_MAX, U256_MAX, type Address, type QuoteState, type Word } from "@lunarbase/math";
import type { BackfillRequest, BootstrapSnapshot, ChainCursor, ContractLog, DeploymentConfig, Network, SnapshotProvider } from "./model.js";
import { Commitment as CommitmentValue, Network as NetworkValue } from "./model.js";
import type { NormalizedBackend } from "./sources.js";

const SELECTOR_CASH = "0x961be391";
const SELECTOR_LANE = "0xd1bacd10";
const SELECTOR_RESERVES = "0xd66bd524";
const SELECTOR_WHITELIST = "0x9b19251a";
const SELECTOR_BLACKLIST_FEE_MULTIPLIER = "0x93b6ab27";
const SELECTOR_PARTNERS = "0xaa5f434c";
const LANE_ADDED_TOPIC = 0x1c61848d54083be4bfb8a26449add9f919cf1efd4ca608005f7f3f6aa0cef958n;
const LANE_REMOVED_TOPIC = 0xdaa054a7d9aa74d7b3ee43f36a9a292169f22fbf60106608accc3161633fba98n;

export class RpcError extends Error {
  constructor(readonly code: "HTTP" | "JSON" | "REMOTE" | "INVALID", message: string) { super(message); this.name = "RpcError"; }
}

export class JsonRpcHttpClient {
  private nextId = 1n;
  constructor(readonly endpoint: string, private readonly fetcher: typeof fetch = fetch) {}

  async call(method: string, params: unknown): Promise<unknown> {
    const id = this.nextId;
    this.nextId += 1n;
    const response = await this.fetcher(this.endpoint, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: id.toString(), method, params }) });
    let value: any;
    try { value = await response.json(); } catch (error) { throw new RpcError("JSON", error instanceof Error ? error.message : "invalid JSON-RPC response"); }
    if (!response.ok) throw new RpcError("HTTP", `HTTP ${response.status}: ${JSON.stringify(value)}`);
    if (value.error) throw new RpcError("REMOTE", `${value.error.code ?? -1}: ${value.error.message ?? "unknown RPC error"}`);
    if (!("result" in value)) throw new RpcError("INVALID", "JSON-RPC response has no result");
    return value.result;
  }

  async callAt(to: Address, data: string, blockTag: string): Promise<string> { return expectString(await this.call("eth_call", [{ to, data }, blockTag]), "eth_call result"); }
  async getCode(address: Address, blockTag: string): Promise<Uint8Array> { return parseHexBytes(expectString(await this.call("eth_getCode", [address, blockTag]), "eth_getCode result")); }
  async blockCursor(blockTag: string, chainId: bigint, commitment: ChainCursor["commitment"]): Promise<ChainCursor> {
    const block = await this.call("eth_getBlockByNumber", [blockTag, false]) as Record<string, unknown> | null;
    if (!block) throw new RpcError("INVALID", "eth_getBlockByNumber returned null");
    return { chainId, blockNumber: parseHexU64(block.number, "block.number"), blockHash: block.hash === null || block.hash === undefined ? undefined : parseHash(block.hash, "block.hash"), commitment };
  }
  async getLogs(request: BackfillRequest, chainId: bigint, commitment: ChainCursor["commitment"]): Promise<ContractLog[]> {
    const filter: Record<string, unknown> = { address: request.filter.address, fromBlock: hexU64(request.fromBlock), toBlock: hexU64(request.toBlock) };
    if (request.filter.topics.length > 0) filter.topics = request.filter.topics.map(wordHex);
    const logs = await this.call("eth_getLogs", [filter]);
    if (!Array.isArray(logs)) throw new RpcError("INVALID", "eth_getLogs result is not an array");
    return logs.map((value) => parseRpcLog(value, chainId, commitment));
  }
}

export class RpcHttpBackend implements NormalizedBackend {
  constructor(readonly rpc: JsonRpcHttpClient, readonly network: Network, readonly chainId: bigint, readonly snapshotTag = "finalized") {}
  snapshotCursor(network: Network): Promise<ChainCursor> { if (network !== this.network) return Promise.reject(new RpcError("INVALID", "RPC backend network mismatch")); return this.rpc.blockCursor(this.snapshotTag, this.chainId, this.snapshotTag === "finalized" ? CommitmentValue.Finalized : CommitmentValue.Canonical); }
  backfill(request: BackfillRequest): Promise<readonly ContractLog[]> { return this.rpc.getLogs(request, this.chainId, CommitmentValue.Canonical); }
  async *subscribe(): AsyncIterable<never> { throw new RpcError("INVALID", "HTTP RPC backend has no realtime subscription"); }
}

export class RpcSnapshotProvider implements SnapshotProvider {
  constructor(readonly rpc: JsonRpcHttpClient, readonly snapshotTag = "finalized") {}
  async snapshot(config: DeploymentConfig, laneAssets: readonly Address[], routers: readonly Address[]): Promise<BootstrapSnapshot> {
    const commitment = this.snapshotTag === "finalized" ? CommitmentValue.Finalized : CommitmentValue.Canonical;
    const cursor = await this.rpc.blockCursor(this.snapshotTag, config.chainId, commitment);
    if (cursor.blockNumber < config.deploymentBlock) throw new RpcError("INVALID", "snapshot block precedes deployment block");
    const runtimeCodeHash = keccak256Hex(await this.rpc.getCode(config.core, this.snapshotTag));
    if (!isZeroHash(config.expectedRuntimeCodeHash) && runtimeCodeHash.toLowerCase() !== config.expectedRuntimeCodeHash.toLowerCase()) throw new RpcError("INVALID", "runtime code hash mismatch");
    const requestedAssets = laneAssets.length > 0 ? laneAssets : config.explicitLaneAssets;
    const assets = await this.resolveLaneAssets(config, requestedAssets, cursor.blockNumber);
    const cash = decodeAddressWord(await this.rpc.callAt(config.core, SELECTOR_CASH, this.snapshotTag));
    const state = { cash, blacklistFeeMultiplier: decodeWord(await this.rpc.callAt(config.core, SELECTOR_BLACKLIST_FEE_MULTIPLIER, this.snapshotTag)), lanes: new Map<Address, { slot0: Word; exists: boolean; paused: boolean; blockDelay: bigint; slippageKBps: bigint }>(), totalPrincipalAmount: new Map<Address, bigint>(), whitelist: new Map<Address, boolean>(), partnerFeeBps: new Map<string, bigint>(), stateVersion: 0n };
    for (const asset of assets) {
      const lane = decodeWords(await this.rpc.callAt(config.core, selectorAddress(SELECTOR_LANE, asset), this.snapshotTag), 5);
      const reserves = decodeWords(await this.rpc.callAt(config.core, selectorAddress(SELECTOR_RESERVES, asset), this.snapshotTag), 5);
      if (lane[3] > 0xffn || lane[4] > 0xffff_ffffn) throw new RpcError("INVALID", "lane metadata exceeds ABI width");
      state.lanes.set(asset, { slot0: lane[0], exists: decodeBool(lane[1]), paused: decodeBool(lane[2]), blockDelay: lane[3], slippageKBps: lane[4] });
      if (reserves[4] > U128_MAX) throw new RpcError("INVALID", "principal exceeds uint128");
      state.totalPrincipalAmount.set(asset, reserves[4]);
    }
    const partnerAssets = [...new Set([...assets, cash])];
    for (const router of (routers.length > 0 ? routers : config.eagerRouters)) {
      state.whitelist.set(router, decodeBool(decodeWord(await this.rpc.callAt(config.core, selectorAddress(SELECTOR_WHITELIST, router), this.snapshotTag))));
      for (const asset of partnerAssets) state.partnerFeeBps.set(`${router.toLowerCase()}:${asset.toLowerCase()}`, decodeWord(await this.rpc.callAt(config.core, selectorTwoAddresses(SELECTOR_PARTNERS, router, asset), this.snapshotTag), 1));
    }
    return { state, cursor, runtimeCodeHash };
  }

  private async resolveLaneAssets(config: DeploymentConfig, explicit: readonly Address[], snapshotBlock: bigint): Promise<Address[]> {
    const history: ContractLog[] = [];
    for (const topic of [LANE_ADDED_TOPIC, LANE_REMOVED_TOPIC]) history.push(...await this.rpc.getLogs({ fromBlock: config.deploymentBlock, toBlock: snapshotBlock, filter: { address: config.core, topics: [topic] } }, config.chainId, CommitmentValue.Canonical));
    history.sort((left, right) => compareCursor(left.cursor, right.cursor));
    const active = new Set<Address>();
    for (const log of history) { const topic = log.topics[0]; const asset = log.topics[1] === undefined ? undefined : addressWord(log.topics[1]); if (!asset) continue; if (topic === LANE_ADDED_TOPIC) active.add(asset); else if (topic === LANE_REMOVED_TOPIC) active.delete(asset); }
    if (explicit.length === 0) return [...active];
    if (explicit.some((asset) => !active.has(asset))) throw new RpcError("INVALID", "explicit lane asset was not active in deployment history");
    return [...explicit];
  }
}

export function parseRpcLog(value: unknown, chainId: bigint, commitment: ChainCursor["commitment"]): ContractLog {
  const log = value as Record<string, unknown>;
  if (!log || typeof log !== "object" || !Array.isArray(log.topics)) throw new RpcError("INVALID", "invalid eth_getLogs entry");
  return { address: parseAddress(expectString(log.address, "log.address")), topics: log.topics.map((topic) => parseWord(topic, "log.topic")), data: expectString(log.data, "log.data"), removed: log.removed === true, cursor: { chainId, blockNumber: parseHexU64(log.blockNumber, "log.blockNumber"), blockHash: log.blockHash === null || log.blockHash === undefined ? undefined : parseHash(log.blockHash, "log.blockHash"), transactionIndex: parseHexU64(log.transactionIndex, "log.transactionIndex"), logIndex: parseHexU64(log.logIndex, "log.logIndex"), commitment } };
}
function selectorAddress(selector: string, address: Address): string { return `${selector}${"0".repeat(24)}${address.slice(2).toLowerCase()}`; }
function selectorTwoAddresses(selector: string, first: Address, second: Address): string { return `${selector}${"0".repeat(24)}${first.slice(2).toLowerCase()}${"0".repeat(24)}${second.slice(2).toLowerCase()}`; }
function parseHexBytes(value: string): Uint8Array { if (!/^0x(?:[0-9a-f]{2})*$/i.test(value)) throw new RpcError("INVALID", "invalid even-length hex"); const result = new Uint8Array((value.length - 2) / 2); for (let index = 0; index < result.length; index += 1) result[index] = Number.parseInt(value.slice(2 + index * 2, 4 + index * 2), 16); return result; }
function parseWord(value: unknown, field: string): Word { return assertU256(BigInt(expectString(value, field)), field); }
function decodeWords(value: string, expected: number): Word[] { const bytes = parseHexBytes(value); if (bytes.length !== expected * 32) throw new RpcError("INVALID", `expected ${expected} ABI words`); return Array.from({ length: expected }, (_, index) => { let result = 0n; for (const byte of bytes.slice(index * 32, index * 32 + 32)) result = (result << 8n) | BigInt(byte); return result; }); }
function decodeWord(value: string, index = 0): Word { return decodeWords(value, index + 1)[index]; }
function decodeAddressWord(value: string): Address { return addressWord(decodeWord(value)); }
function addressWord(value: Word): Address { const hex = value.toString(16).padStart(64, "0"); if (hex.slice(0, 24) !== "0".repeat(24)) throw new RpcError("INVALID", "ABI address is not padded"); return parseAddress(`0x${hex.slice(24)}`); }
function decodeBool(value: Word): boolean { if (value === 0n) return false; if (value === 1n) return true; throw new RpcError("INVALID", "ABI boolean is not 0 or 1"); }
function expectString(value: unknown, field: string): string { if (typeof value !== "string") throw new RpcError("INVALID", `${field} is not a string`); return value; }
export function parseHexU64(value: unknown, field: string): bigint { const text = expectString(value, field); if (!/^0x[0-9a-f]+$/i.test(text)) throw new RpcError("INVALID", `${field} is not hex`); const result = BigInt(text); if (result > (1n << 64n) - 1n) throw new RpcError("INVALID", `${field} exceeds uint64`); return result; }
export function parseHash(value: unknown, field: string): string { const text = expectString(value, field); if (!/^0x[0-9a-f]{64}$/i.test(text)) throw new RpcError("INVALID", `${field} is not bytes32`); return text.toLowerCase(); }
function hexU64(value: bigint): string { if (value < 0n || value > (1n << 64n) - 1n) throw new RpcError("INVALID", "uint64 overflow"); return `0x${value.toString(16)}`; }
function wordHex(value: Word): string { return `0x${value.toString(16).padStart(64, "0")}`; }
function compareCursor(left: ChainCursor, right: ChainCursor): number { for (const [a, b] of [[left.blockNumber, right.blockNumber], [left.transactionIndex ?? 0n, right.transactionIndex ?? 0n], [left.logIndex ?? 0n, right.logIndex ?? 0n]] as const) { if (a < b) return -1; if (a > b) return 1; } return 0; }
function isZeroHash(value: string): boolean { return /^0x0{64}$/i.test(value); }

const MASK64 = (1n << 64n) - 1n;
const ROUND_CONSTANTS = [1n, 0x8082n, 0x800000000000808an, 0x8000000080008000n, 0x808bn, 0x80000001n, 0x8000000080008081n, 0x8000000000008009n, 0x8an, 0x88n, 0x80008009n, 0x8000000an, 0x8000808bn, 0x800000000000008bn, 0x8000000000008089n, 0x8000000000008003n, 0x8000000000008002n, 0x8000000000000080n, 0x800an, 0x800000008000000an, 0x8000000080008081n, 0x8000000000008080n, 0x80000001n, 0x8000000080008008n];
function keccak256Hex(input: Uint8Array): string { const rate = 136; const state = Array<bigint>(25).fill(0n); const padded = new Uint8Array(Math.ceil((input.length + 1) / rate) * rate); padded.set(input); padded[input.length] = 0x01; padded[padded.length - 1] |= 0x80; for (let offset = 0; offset < padded.length; offset += rate) { for (let lane = 0; lane < rate / 8; lane += 1) state[lane] ^= littleEndian(padded, offset + lane * 8); keccakF(state); } const output = new Uint8Array(32); for (let lane = 0; lane < 4; lane += 1) writeLittleEndian(output, lane * 8, state[lane]); return `0x${[...output].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`; }
function littleEndian(bytes: Uint8Array, offset: number): bigint { let value = 0n; for (let index = 7; index >= 0; index -= 1) value = (value << 8n) | BigInt(bytes[offset + index]); return value; }
function writeLittleEndian(bytes: Uint8Array, offset: number, value: bigint): void { for (let index = 0; index < 8; index += 1) { bytes[offset + index] = Number(value & 0xffn); value >>= 8n; } }
function rotate(value: bigint, bits: number): bigint { if (bits === 0) return value; return ((value << BigInt(bits)) | (value >> BigInt(64 - bits))) & MASK64; }
function keccakF(state: bigint[]): void {
  for (const round of ROUND_CONSTANTS) {
    const c = Array<bigint>(5).fill(0n);
    for (let x = 0; x < 5; x += 1) for (let y = 0; y < 5; y += 1) c[x] ^= state[x + 5 * y];
    const d = c.map((_value, x) => c[(x + 4) % 5] ^ rotate(c[(x + 1) % 5], 1));
    for (let x = 0; x < 5; x += 1) for (let y = 0; y < 5; y += 1) state[x + 5 * y] ^= d[x];
    let current = state[1];
    let x = 1;
    let y = 0;
    for (let t = 0; t < 24; t += 1) {
      const nextX = y;
      const nextY = (2 * x + 3 * y) % 5;
      const index = nextX + 5 * nextY;
      const previous = state[index];
      state[index] = rotate(current, ((t + 1) * (t + 2) / 2) % 64);
      current = previous;
      x = nextX;
      y = nextY;
    }
    const b = Array<bigint>(5).fill(0n);
    for (let y = 0; y < 5; y += 1) {
      for (let x = 0; x < 5; x += 1) b[x] = state[x + 5 * y];
      for (let x = 0; x < 5; x += 1) state[x + 5 * y] = (b[x] ^ ((~b[(x + 1) % 5]) & b[(x + 2) % 5])) & MASK64;
    }
    state[0] ^= round;
  }
}

export { keccak256Hex };
