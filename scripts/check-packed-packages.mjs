import { execFileSync } from "node:child_process";
import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import { releaseNpmPackages } from "./release-packages.mjs";

const dist = resolve(process.cwd(), "dist");
const rootLicense = await readFile(resolve(process.cwd(), "LICENSE"), "utf8");
const expectedKeywords = new Map([
  ["@lunarbase-lab/pmm-v2-math", ["lunarbase", "pmm", "defi", "evm", "quote-math"]],
  ["@lunarbase-lab/pmm-v2-client", ["lunarbase", "pmm", "defi", "evm", "realtime"]],
  ["@lunarbase-lab/pmm-v2-source-evm", ["lunarbase", "evm", "base", "rpc", "websocket"]],
  ["@lunarbase-lab/pmm-v2-source-arbitrum", ["lunarbase", "arbitrum", "nitro", "evm", "rpc"]],
]);
const archives = (await readdir(dist)).filter((entry) => entry.endsWith(".tgz")).map((entry) => join(dist, entry));
if (releaseNpmPackages.length !== 4) {
  throw new Error("release inventory must contain exactly four npm packages");
}
if (archives.length !== releaseNpmPackages.length) {
  throw new Error("expected exactly " + releaseNpmPackages.length + " npm archives, found " + archives.length);
}

const packed = [];

for (const archive of archives) {
  const packageJson = execFileSync("tar", ["-xOf", archive, "package/package.json"], {
    encoding: "utf8",
  });
  const manifest = JSON.parse(packageJson);
  const listing = execFileSync("tar", ["-tzf", archive], { encoding: "utf8" }).split("\n").filter(Boolean);
  if (listing.some((entry) => !entry.startsWith("package/") || entry.includes("../"))) {
    throw new Error(archive + " contains an unsafe archive path");
  }
  if (!releaseNpmPackages.some(({ name }) => name === manifest.name)) {
    throw new Error(archive + " contains unexpected package " + manifest.name);
  }
  packed.push({ archive, listing, manifest });
}

for (const expected of releaseNpmPackages) {
  const source = JSON.parse(await readFile(join(expected.directory, "package.json"), "utf8"));
  const matches = packed.filter(
    ({ manifest }) => manifest.name === expected.name && manifest.version === source.version,
  );
  if (matches.length !== 1) {
    throw new Error("expected one packed " + expected.name + "@" + source.version + ", found " + matches.length);
  }

  const [{ archive, listing, manifest }] = matches;
  if (
    manifest.description !== source.description ||
    JSON.stringify(manifest.keywords) !== JSON.stringify(source.keywords) ||
    manifest.license !== source.license ||
    manifest.type !== source.type ||
    JSON.stringify(manifest.repository) !== JSON.stringify(source.repository) ||
    JSON.stringify(manifest.exports) !== JSON.stringify(source.exports)
  ) {
    throw new Error(expected.name + " packed public metadata does not match its source manifest");
  }
  if (manifest.private || manifest.publishConfig?.access !== "public") {
    throw new Error(expected.name + " packed manifest is not public");
  }
  if (typeof manifest.description !== "string" || manifest.description.trim() === "") {
    throw new Error(expected.name + " packed manifest has no public description");
  }
  if (JSON.stringify(manifest.keywords) !== JSON.stringify(expectedKeywords.get(expected.name))) {
    throw new Error(expected.name + " packed manifest has unexpected public keywords");
  }
  const packedScripts = manifest.scripts ?? {};
  const unexpectedScript = Object.keys(packedScripts).find((name) => name !== "prepack");
  if (unexpectedScript || (packedScripts.prepack !== undefined && packedScripts.prepack !== source.scripts?.prepack)) {
    throw new Error(expected.name + " packed manifest exposes a repository-only script");
  }
  if (manifest.license !== "MIT OR Apache-2.0") {
    throw new Error(expected.name + " packed manifest has unexpected license metadata");
  }
  if (
    manifest.repository?.type !== "git" ||
    manifest.repository?.url !== "git+https://github.com/Lunarbase-Lab/lunarbase-sdk.git" ||
    manifest.repository?.directory !== expected.directory
  ) {
    throw new Error(expected.name + " packed manifest has incomplete repository metadata");
  }
  if (manifest.type !== "module") {
    throw new Error(expected.name + " must be published as an ES module");
  }
  if (JSON.stringify(manifest).includes("workspace:")) {
    throw new Error(expected.name + " still contains a workspace: dependency");
  }
  const exported = manifest.exports?.["."];
  if (exported?.import !== "./dist/index.js" || exported?.types !== "./dist/index.d.ts") {
    throw new Error(expected.name + " packed manifest has invalid exports");
  }
  for (const [subpath, conditions] of Object.entries(manifest.exports ?? {})) {
    for (const [condition, target] of Object.entries(conditions)) {
      if (typeof target !== "string" || !target.startsWith("./")) {
        throw new Error(expected.name + " has an invalid " + condition + " target for " + subpath);
      }
      const packedTarget = "package/" + target.slice(2);
      if (!listing.includes(packedTarget)) {
        throw new Error(expected.name + " archive is missing " + subpath + " " + condition + " target " + packedTarget);
      }
    }
  }
  if (!listing.includes("package/README.md") || !listing.includes("package/LICENSE")) {
    throw new Error(expected.name + " archive must contain README.md and LICENSE");
  }
  const readme = execFileSync("tar", ["-xOf", archive, "package/README.md"], { encoding: "utf8" });
  const license = execFileSync("tar", ["-xOf", archive, "package/LICENSE"], { encoding: "utf8" });
  const [sourceReadme, sourceLicense] = await Promise.all([
    readFile(join(expected.directory, "README.md"), "utf8"),
    readFile(join(expected.directory, "LICENSE"), "utf8"),
  ]);
  if (readme.trim() === "" || license.trim() === "" || readme !== sourceReadme || license !== sourceLicense) {
    throw new Error(expected.name + " archive contains an empty or source-mismatched README.md or LICENSE");
  }
  if (sourceLicense !== rootLicense) {
    throw new Error(expected.name + " LICENSE must exactly match the repository LICENSE");
  }
  if (!listing.includes("package/dist/index.js") || !listing.includes("package/dist/index.d.ts")) {
    throw new Error(expected.name + " archive is missing its JavaScript or type entry point");
  }
  const forbidden = listing.find(
    (entry) =>
      /(?:^|\/)(?:__tests__|tests?)(?:\/|$)/i.test(entry) ||
      /\.(?:test|spec)\.[^/]+$/i.test(entry) ||
      /quote[-_]?oracle/i.test(entry),
  );
  if (forbidden) {
    throw new Error(expected.name + " archive contains private/test entry " + forbidden);
  }

  for (const dependencyName of releaseNpmPackages.map(({ name }) => name)) {
    const dependencyVersion = manifest.dependencies?.[dependencyName];
    if (dependencyVersion !== undefined && dependencyVersion !== manifest.version) {
      throw new Error(expected.name + " must pin " + dependencyName + " to release version " + manifest.version);
    }
  }
  console.log(expected.name + "@" + source.version + ": " + archive);
}
