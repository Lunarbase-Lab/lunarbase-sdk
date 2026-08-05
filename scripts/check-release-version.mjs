import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

import { assertTopologicalOrder } from "./release-integrity.mjs";
import { releaseCrates, releaseNpmPackages } from "./release-packages.mjs";

const readJson = async (file) => JSON.parse(await readFile(file, "utf8"));
const rootPackage = await readJson("package.json");
const releaseTag = process.argv[2] ?? process.env.RELEASE_TAG ?? `v${rootPackage.version}`;

const semverMatch = releaseTag.match(
  /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/,
);
if (!semverMatch) {
  throw new Error("release tag must be v-prefixed SemVer without build metadata: " + releaseTag);
}

const expectedVersion = releaseTag.slice(1);
const isPrerelease = semverMatch[4] !== undefined;
const githubPrerelease = process.env.GITHUB_RELEASE_PRERELEASE;
if (githubPrerelease === "true" && !isPrerelease) {
  throw new Error("GitHub prerelease requires a prerelease SemVer tag");
}
if (githubPrerelease === "false" && isPrerelease) {
  throw new Error("stable GitHub release cannot use a prerelease SemVer tag");
}

if (!rootPackage.private || rootPackage.version !== expectedVersion) {
  throw new Error("root package must stay private and use version " + expectedVersion);
}

const rootCargo = await readFile("Cargo.toml", "utf8");
const workspacePackage = rootCargo.match(/\[workspace\.package\]([\s\S]*?)(?=\n\[|$)/)?.[1];
const cargoVersion = workspacePackage?.match(/^version = "([^"]+)"$/m)?.[1];
if (cargoVersion !== expectedVersion) {
  throw new Error("Cargo workspace version " + (cargoVersion ?? "<missing>") + " != " + expectedVersion);
}

const memberSection = rootCargo.match(/members = \[([\s\S]*?)\]\n/)?.[1] ?? "";
const memberPaths = [...memberSection.matchAll(/"([^"]+)"/g)].map((match) => match[1]);
const discoveredCrates = [];
const cargoManifests = new Map();

for (const memberPath of memberPaths) {
  const manifestPath = join(memberPath, "Cargo.toml");
  const manifest = await readFile(manifestPath, "utf8");
  const packageSection = manifest.match(/\[package\]([\s\S]*?)(?=\n\[|$)/)?.[1];
  if (!packageSection) continue;
  const name = packageSection.match(/^name = "([^"]+)"$/m)?.[1];
  if (!name) throw new Error(manifestPath + " has no package name");
  cargoManifests.set(name, manifest);
  const publishDisabled = /^publish = false$/m.test(packageSection);
  if (!publishDisabled) {
    discoveredCrates.push(name);
    if (!/^version\.workspace = true$/m.test(packageSection)) {
      throw new Error(name + " must inherit the workspace release version");
    }
  }
}

const assertSameSet = (actual, expected, label) => {
  const actualSorted = [...actual].sort();
  const expectedSorted = [...expected].sort();
  if (JSON.stringify(actualSorted) !== JSON.stringify(expectedSorted)) {
    throw new Error(
      label + " inventory mismatch: actual=" + actualSorted.join(",") + " expected=" + expectedSorted.join(","),
    );
  }
};

assertSameSet(
  discoveredCrates,
  releaseCrates.map(({ name }) => name),
  "crates.io",
);

const escapeRegExp = (value) => value.replace(/[.*+?^$()|[\]\\{}]/g, "\\$&");
for (const { name: crate, workspaceDependency } of releaseCrates) {
  const dependencyPattern = new RegExp(
    "^" +
      escapeRegExp(workspaceDependency) +
      ' = \\{ package = "' +
      escapeRegExp(crate) +
      '", version = "' +
      escapeRegExp(expectedVersion) +
      '", path = ',
    "m",
  );
  if (!dependencyPattern.test(rootCargo)) {
    throw new Error(
      "workspace dependency " + workspaceDependency + " must map to " + crate + " and pin version " + expectedVersion,
    );
  }
}

const crateByWorkspaceDependency = new Map(
  releaseCrates.map(({ name, workspaceDependency }) => [workspaceDependency, name]),
);
const releaseCrateNames = new Set(releaseCrates.map(({ name }) => name));
const cargoDependencies = new Map();
for (const { name: crate } of releaseCrates) {
  const manifest = cargoManifests.get(crate);
  if (!manifest) throw new Error("missing manifest for release crate " + crate);
  const dependencies = new Set();
  for (const match of manifest.matchAll(
    /^([A-Za-z0-9_-]+)(?:\.workspace\s*=\s*true|\s*=\s*\{[^}]*\bworkspace\s*=\s*true[^}]*\})$/gm,
  )) {
    const dependency = crateByWorkspaceDependency.get(match[1]);
    if (dependency) dependencies.add(dependency);
  }
  for (const match of manifest.matchAll(/\bpackage\s*=\s*"([^"]+)"/g)) {
    if (releaseCrateNames.has(match[1])) dependencies.add(match[1]);
  }
  cargoDependencies.set(crate, dependencies);
}
assertTopologicalOrder(
  releaseCrates.map(({ name }) => name),
  cargoDependencies,
  "crates.io",
);

const packageDirectories = (await readdir("packages", { withFileTypes: true }))
  .filter((entry) => entry.isDirectory())
  .map((entry) => "packages/" + entry.name);
const discoveredNpm = [];
const npmManifests = new Map();

for (const directory of packageDirectories) {
  const manifest = await readJson(join(directory, "package.json"));
  if (manifest.private) continue;
  discoveredNpm.push(manifest.name);
  npmManifests.set(manifest.name, manifest);
  if (manifest.version !== expectedVersion) {
    throw new Error(manifest.name + " version " + manifest.version + " != " + expectedVersion);
  }
  if (manifest.publishConfig?.access !== "public") {
    throw new Error(manifest.name + " must publish with public access");
  }
  const expectedEntry = releaseNpmPackages.find((entry) => entry.name === manifest.name);
  if (!expectedEntry || expectedEntry.directory !== directory) {
    throw new Error("unexpected npm release package " + manifest.name + " at " + directory);
  }
}

assertSameSet(
  discoveredNpm,
  releaseNpmPackages.map((entry) => entry.name),
  "npm",
);

const releaseNpmNames = new Set(releaseNpmPackages.map(({ name }) => name));
const npmDependencies = new Map();
for (const { name } of releaseNpmPackages) {
  const manifest = npmManifests.get(name);
  if (!manifest) throw new Error("missing manifest for release npm package " + name);
  const dependencies = new Set();
  for (const field of ["dependencies", "optionalDependencies", "peerDependencies"]) {
    for (const dependency of Object.keys(manifest[field] ?? {})) {
      if (releaseNpmNames.has(dependency)) dependencies.add(dependency);
    }
  }
  npmDependencies.set(name, dependencies);
}
assertTopologicalOrder(
  releaseNpmPackages.map(({ name }) => name),
  npmDependencies,
  "npm",
);

console.log(
  JSON.stringify({
    releaseTag,
    version: expectedVersion,
    npmDistTag: isPrerelease ? "next" : "latest",
    crates: releaseCrates.length,
    npmPackages: releaseNpmPackages.length,
  }),
);
