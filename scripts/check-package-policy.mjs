import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";

const expectedNpmPackages = new Map([
  ["client", "@lunarbase-lab/pmm-v2-client"],
  ["math", "@lunarbase-lab/pmm-v2-math"],
  ["source-arbitrum", "@lunarbase-lab/pmm-v2-source-arbitrum"],
  ["source-evm", "@lunarbase-lab/pmm-v2-source-evm"],
  ["source-monad", "@lunarbase-lab/pmm-v2-source-monad"],
]);

const privateNpmPackages = new Set(["source-monad"]);

const expectedCargoPackages = new Map([
  ["lunarbase-client", "lunarbase-pmm-v2-client"],
  ["lunarbase-math", "lunarbase-pmm-v2-math"],
  ["lunarbase-source-arbitrum", "lunarbase-pmm-v2-source-arbitrum"],
  ["lunarbase-source-evm", "lunarbase-pmm-v2-source-evm"],
]);

const expectedNpmKeywords = new Map([
  ["math", ["lunarbase", "pmm", "defi", "evm", "quote-math"]],
  ["client", ["lunarbase", "pmm", "defi", "evm", "realtime"]],
  ["source-evm", ["lunarbase", "evm", "base", "rpc", "websocket"]],
  ["source-monad", ["lunarbase", "monad", "evm", "rpc", "websocket"]],
  ["source-arbitrum", ["lunarbase", "arbitrum", "nitro", "evm", "rpc"]],
]);

const expectedCargoMetadata = new Map([
  [
    "lunarbase-math",
    { keywords: ["lunarbase", "pmm", "defi", "evm", "quote"], categories: ["algorithms", "mathematics"] },
  ],
  [
    "lunarbase-client",
    { keywords: ["lunarbase", "pmm", "defi", "evm", "realtime"], categories: ["api-bindings", "asynchronous"] },
  ],
  [
    "lunarbase-source-evm",
    {
      keywords: ["lunarbase", "evm", "base", "rpc", "websocket"],
      categories: ["api-bindings", "asynchronous", "network-programming"],
    },
  ],
  [
    "lunarbase-source-arbitrum",
    {
      keywords: ["lunarbase", "arbitrum", "nitro", "evm", "rpc"],
      categories: ["api-bindings", "asynchronous", "network-programming"],
    },
  ],
]);

const expectedPrepackScript =
  "node --input-type=module --eval \"import { access } from 'node:fs/promises'; await Promise.all(['dist/index.js', 'dist/index.d.ts'].map((file) => access(file)))\"";

function packageSection(source) {
  const heading = source.match(/^\[package\]\s*$/m);
  if (!heading || heading.index === undefined) return null;

  const rest = source.slice(heading.index + heading[0].length);
  const nextHeading = rest.search(/^\[/m);
  return nextHeading === -1 ? rest : rest.slice(0, nextHeading);
}

function tomlString(section, key) {
  const escapedKey = key.replaceAll(".", "\\.");
  return section.match(new RegExp(`^${escapedKey}\\s*=\\s*"([^"]+)"`, "m"))?.[1];
}

function tomlStringArray(section, key) {
  const escapedKey = key.replaceAll(".", "\\.");
  const value = section.match(new RegExp(`^${escapedKey}\\s*=\\s*\\[([^\\]]*)\\]`, "m"))?.[1];
  return value === undefined ? undefined : [...value.matchAll(/"([^"]+)"/g)].map((match) => match[1]);
}

export function checkPackagePolicy({ root, addFailure, checkWording, repoPath }) {
  const packagesRoot = resolve(root, "packages");
  const packageDirectories = readdirSync(packagesRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && existsSync(join(packagesRoot, entry.name, "package.json")))
    .map((entry) => entry.name)
    .sort();

  const expectedDirectories = [...expectedNpmPackages.keys()].sort();
  if (packageDirectories.join("\n") !== expectedDirectories.join("\n")) {
    addFailure(
      `workspace npm package directories must be exactly: ${expectedDirectories.join(", ")}; found: ${packageDirectories.join(", ")}`,
    );
  }

  for (const [directory, expectedName] of expectedNpmPackages) {
    const manifestPath = join(packagesRoot, directory, "package.json");
    if (!existsSync(manifestPath)) continue;

    let manifest;
    try {
      manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    } catch (error) {
      addFailure(`${repoPath(manifestPath)}: invalid JSON: ${error instanceof Error ? error.message : String(error)}`);
      continue;
    }

    if (manifest.name !== expectedName) {
      addFailure(`${repoPath(manifestPath)}: npm package name must be ${expectedName}; found ${String(manifest.name)}`);
    }
    const expectedPrivate = privateNpmPackages.has(directory);
    if ((manifest.private === true) !== expectedPrivate) {
      addFailure(`${repoPath(manifestPath)}: private must be ${String(expectedPrivate)}`);
    }
    if (expectedPrivate && manifest.publishConfig !== undefined) {
      addFailure(`${repoPath(manifestPath)}: private workspace package cannot define publishConfig`);
    }
    if (!expectedPrivate && manifest.publishConfig?.access !== "public") {
      addFailure(`${repoPath(manifestPath)}: public package publishConfig.access must be public`);
    }
    if (JSON.stringify(manifest.keywords) !== JSON.stringify(expectedNpmKeywords.get(directory))) {
      addFailure(`${repoPath(manifestPath)}: public keywords do not match the package registry profile`);
    }
    if (
      JSON.stringify(Object.keys(manifest.scripts ?? {}).sort()) !== JSON.stringify(["prepack"]) ||
      manifest.scripts?.prepack !== expectedPrepackScript
    ) {
      addFailure(`${repoPath(manifestPath)}: scripts must contain only the self-contained prepack check`);
    }
    if (typeof manifest.description === "string") {
      checkWording(repoPath(manifestPath), 1, manifest.description);
    }
  }

  const cratesRoot = resolve(root, "crates");
  const crateDirectories = readdirSync(cratesRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && existsSync(join(cratesRoot, entry.name, "Cargo.toml")))
    .map((entry) => entry.name)
    .sort();

  const publicCrates = [];

  for (const directory of crateDirectories) {
    const manifestPath = join(cratesRoot, directory, "Cargo.toml");
    const section = packageSection(readFileSync(manifestPath, "utf8"));
    if (section === null) {
      addFailure(`${repoPath(manifestPath)}: missing [package] section`);
      continue;
    }

    const name = tomlString(section, "name");
    const isPrivate = /^\s*publish\s*=\s*false\s*$/m.test(section);
    if (!isPrivate) publicCrates.push(directory);

    if (!isPrivate && !/^lunarbase-pmm-v2-[a-z0-9-]+$/.test(name ?? "")) {
      addFailure(
        `${repoPath(manifestPath)}: public Cargo package name must use lunarbase-pmm-v2-*; found ${String(name)}`,
      );
    }

    const description = tomlString(section, "description");
    if (!isPrivate && description !== undefined) {
      checkWording(repoPath(manifestPath), 1, description);
    }

    const expectedName = expectedCargoPackages.get(directory);
    if (expectedName !== undefined && name !== expectedName) {
      addFailure(`${repoPath(manifestPath)}: Cargo package name must be ${expectedName}; found ${String(name)}`);
    }

    const expectedMetadata = expectedCargoMetadata.get(directory);
    if (expectedMetadata !== undefined) {
      const keywords = tomlStringArray(section, "keywords");
      const categories = tomlStringArray(section, "categories");
      if (JSON.stringify(keywords) !== JSON.stringify(expectedMetadata.keywords)) {
        addFailure(`${repoPath(manifestPath)}: public keywords do not match the crate registry profile`);
      }
      if (JSON.stringify(categories) !== JSON.stringify(expectedMetadata.categories)) {
        addFailure(`${repoPath(manifestPath)}: categories do not match the crate registry profile`);
      }
    }
  }

  const expectedPublicCrates = [...expectedCargoPackages.keys()].sort();
  if (publicCrates.join("\n") !== expectedPublicCrates.join("\n")) {
    addFailure(
      `public Cargo package directories must be exactly: ${expectedPublicCrates.join(", ")}; found: ${publicCrates.join(", ")}`,
    );
  }
}
