import { randomUUID } from "node:crypto";
import { open, readFile, rename, unlink } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { getAddress, type Address, type Hash } from "viem";
import { z } from "zod";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UINT = /^(?:0|[1-9][0-9]*)$/;
const BYTES32 = /^0x[0-9a-fA-F]{64}$/;

const persistedStateSchema = z
  .object({
    version: z.literal(1),
    chainId: z.number().int().positive(),
    pool: z.string(),
    actor: z.string(),
    cursor: z
      .object({
        blockNumber: z.string().regex(UINT),
        blockHash: z.string().regex(BYTES32),
      })
      .strict(),
    phase: z.discriminatedUnion("kind", [
      z.object({ kind: z.literal("opening") }).strict(),
      z
        .object({
          kind: z.literal("return"),
          assetIn: z.string(),
          assetOut: z.string(),
          maximumAmountIn: z.string().regex(UINT),
        })
        .strict(),
    ]),
  })
  .strict();

export type PairingPhase =
  | { readonly kind: "opening" }
  | {
      readonly kind: "return";
      readonly assetIn: Address;
      readonly assetOut: Address;
      readonly maximumAmountIn: bigint;
    };

export interface PairingCursor {
  readonly blockNumber: bigint;
  readonly blockHash: Hash;
}

export interface PairingCheckpoint {
  readonly cursor: PairingCursor;
  readonly phase: PairingPhase;
}

export interface PairingStateIdentity {
  readonly chainId: number;
  readonly pool: Address;
  readonly actor: Address;
}

export interface PairingStateStore {
  readonly path: string;
  load(): Promise<PairingCheckpoint | undefined>;
  save(checkpoint: PairingCheckpoint): Promise<void>;
}

/** Creates an actor-scoped, atomic local checkpoint store containing no key material. */
export function createPairingStateStore(identity: PairingStateIdentity, path?: string): PairingStateStore {
  const statePath = path ?? resolve(packageRoot, `.pairing-state-${identity.actor.toLowerCase()}.json`);
  return {
    path: statePath,
    async load(): Promise<PairingCheckpoint | undefined> {
      let contents: string;
      try {
        contents = await readFile(statePath, "utf8");
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
        throw error;
      }

      let persisted: z.infer<typeof persistedStateSchema>;
      try {
        persisted = persistedStateSchema.parse(JSON.parse(contents));
      } catch (cause) {
        throw new Error(`pairing checkpoint at ${statePath} is malformed`, { cause });
      }
      if (
        persisted.chainId !== identity.chainId ||
        getAddress(persisted.pool) !== identity.pool ||
        getAddress(persisted.actor) !== identity.actor
      )
        throw new Error(`pairing checkpoint at ${statePath} belongs to another deployment or actor`);

      const phase: PairingPhase =
        persisted.phase.kind === "opening"
          ? { kind: "opening" }
          : {
              kind: "return",
              assetIn: getAddress(persisted.phase.assetIn),
              assetOut: getAddress(persisted.phase.assetOut),
              maximumAmountIn: BigInt(persisted.phase.maximumAmountIn),
            };
      return {
        cursor: {
          blockNumber: BigInt(persisted.cursor.blockNumber),
          blockHash: persisted.cursor.blockHash.toLowerCase() as Hash,
        },
        phase,
      };
    },
    async save(checkpoint: PairingCheckpoint): Promise<void> {
      if (checkpoint.cursor.blockNumber < 0n) throw new RangeError("pairing checkpoint block must be non-negative");
      if (checkpoint.phase.kind === "return" && checkpoint.phase.maximumAmountIn <= 0n)
        throw new RangeError("pairing checkpoint return amount must be positive");

      const persisted = {
        version: 1,
        chainId: identity.chainId,
        pool: identity.pool,
        actor: identity.actor,
        cursor: {
          blockNumber: checkpoint.cursor.blockNumber.toString(),
          blockHash: checkpoint.cursor.blockHash,
        },
        phase:
          checkpoint.phase.kind === "opening"
            ? { kind: "opening" as const }
            : {
                kind: "return" as const,
                assetIn: checkpoint.phase.assetIn,
                assetOut: checkpoint.phase.assetOut,
                maximumAmountIn: checkpoint.phase.maximumAmountIn.toString(),
              },
      };
      persistedStateSchema.parse(persisted);

      const temporaryPath = `${statePath}.${process.pid}.${randomUUID()}.tmp`;
      let handle;
      try {
        handle = await open(temporaryPath, "wx", 0o600);
        await handle.writeFile(`${JSON.stringify(persisted)}\n`, "utf8");
        await handle.sync();
        await handle.close();
        handle = undefined;
        await rename(temporaryPath, statePath);

        const directory = await open(dirname(statePath), "r");
        try {
          await directory.sync();
        } finally {
          await directory.close();
        }
      } catch (error) {
        await handle?.close();
        await unlink(temporaryPath).catch((unlinkError: NodeJS.ErrnoException) => {
          if (unlinkError.code !== "ENOENT") throw unlinkError;
        });
        throw error;
      }
    },
  };
}
