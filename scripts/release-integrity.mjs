import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

export const npmTarballIntegrity = (contents) => "sha512-" + createHash("sha512").update(contents).digest("base64");

export const crateArchiveChecksum = (contents) => createHash("sha256").update(contents).digest("hex");

export const assertNpmRegistryArtifact = (metadata, expected) => {
  if (!metadata || typeof metadata !== "object") {
    throw new Error(expected.name + "@" + expected.version + " returned invalid npm metadata");
  }
  if (metadata.name !== expected.name || metadata.version !== expected.version) {
    throw new Error(
      "npm registry identity mismatch: expected " +
        expected.name +
        "@" +
        expected.version +
        ", found " +
        String(metadata.name) +
        "@" +
        String(metadata.version),
    );
  }

  const remoteIntegrity = metadata.dist?.integrity;
  if (typeof remoteIntegrity !== "string" || !remoteIntegrity.split(/\s+/).includes(expected.integrity)) {
    throw new Error(
      "npm registry integrity mismatch for " +
        expected.name +
        "@" +
        expected.version +
        ": expected " +
        expected.integrity +
        ", found " +
        (remoteIntegrity ?? "<missing>"),
    );
  }
};

export const assertCratesIoArtifact = (metadata, expected) => {
  const published = metadata?.version;
  if (!published || typeof published !== "object") {
    throw new Error(expected.name + "@" + expected.version + " returned invalid crates.io metadata");
  }
  if (published.num !== expected.version || (published.crate && published.crate !== expected.name)) {
    throw new Error("crates.io identity mismatch for " + expected.name + "@" + expected.version);
  }
  if (published.yanked !== false) {
    throw new Error(expected.name + "@" + expected.version + " is yanked on crates.io");
  }
  if (published.checksum !== expected.checksum) {
    throw new Error(
      "crates.io checksum mismatch for " +
        expected.name +
        "@" +
        expected.version +
        ": expected " +
        expected.checksum +
        ", found " +
        (published.checksum ?? "<missing>"),
    );
  }
};

export const assertTopologicalOrder = (orderedNames, dependencyMap, label) => {
  const positions = new Map();
  orderedNames.forEach((name, index) => {
    if (positions.has(name)) throw new Error(label + " publish order contains duplicate " + name);
    positions.set(name, index);
  });

  for (const name of orderedNames) {
    const position = positions.get(name);
    for (const dependency of dependencyMap.get(name) ?? []) {
      const dependencyPosition = positions.get(dependency);
      if (dependencyPosition === undefined) continue;
      if (dependencyPosition >= position) {
        throw new Error(
          label +
            " publish order is not topological: " +
            name +
            " depends on " +
            dependency +
            ", which is not published earlier",
        );
      }
    }
  }
};

const runCli = async () => {
  const [command, ...args] = process.argv.slice(2);
  if (command === "verify-crate-response") {
    const [name, version, checksum] = args;
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    const metadata = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    assertCratesIoArtifact(metadata, { name, version, checksum });
    return;
  }
  if (command === "sha256") {
    const [file] = args;
    process.stdout.write(crateArchiveChecksum(await readFile(file)) + "\n");
    return;
  }
  throw new Error("unknown release-integrity command: " + String(command));
};

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runCli().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
