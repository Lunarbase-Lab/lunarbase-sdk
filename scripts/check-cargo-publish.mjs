import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

import { releaseCrates } from "./release-packages.mjs";

const rootCargo = await readFile("Cargo.toml", "utf8");
const rootLicense = await readFile("LICENSE", "utf8");
const memberSection = rootCargo.match(/members = \[([\s\S]*?)\]\n/)?.[1] ?? "";
const memberPaths = [...memberSection.matchAll(/"([^"]+)"/g)].map((match) => match[1]);
const workspaceDependencies = rootCargo.match(/\[workspace\.dependencies\]([\s\S]*?)(?=\n\[|$)/)?.[1] ?? "";
const workspacePackage = rootCargo.match(/\[workspace\.package\]([\s\S]*?)(?=\n\[|$)/)?.[1] ?? "";
const allowedCategories = new Set(["algorithms", "api-bindings", "asynchronous", "mathematics", "network-programming"]);

function manifestArray(section, key) {
  const escaped = key.replace(/[.*+?^$()|[\]\\{}]/g, "\\$&");
  const value = section.match(new RegExp("^" + escaped + "\\s*=\\s*\\[([^\\]]*)\\]$", "m"))?.[1];
  return value === undefined ? undefined : [...value.matchAll(/"([^"]+)"/g)].map((match) => match[1]);
}

if (releaseCrates.length !== 5) {
  throw new Error("release inventory must contain exactly five crates");
}
if (!/^license = "MIT OR Apache-2\.0"$/m.test(workspacePackage)) {
  throw new Error("Cargo workspace must declare the public dual-license expression");
}
if (!/^repository = "https:\/\/github\.com\/Lunarbase-Lab\/lunarbase-sdk"$/m.test(workspacePackage)) {
  throw new Error("Cargo workspace must declare the public repository URL");
}

const manifests = new Map();
for (const memberPath of memberPaths) {
  const manifest = await readFile(join(memberPath, "Cargo.toml"), "utf8");
  const packageSection = manifest.match(/\[package\]([\s\S]*?)(?=\n\[|$)/)?.[1];
  const name = packageSection?.match(/^name = "([^"]+)"$/m)?.[1];
  if (name) manifests.set(name, { manifest, memberPath, packageSection });
}

for (const { name: crate } of releaseCrates) {
  const entry = manifests.get(crate);
  if (!entry) throw new Error("missing release crate manifest for " + crate);
  if (/^publish = false$/m.test(entry.packageSection)) {
    throw new Error(crate + " unexpectedly disables publishing");
  }
  for (const inherited of ["version", "edition", "license", "rust-version", "repository"]) {
    const escaped = inherited.replace("-", "\\-");
    if (!new RegExp("^" + escaped + "\\.workspace = true$", "m").test(entry.packageSection)) {
      throw new Error(crate + " must inherit public " + inherited + " metadata from the workspace");
    }
  }
  const description = entry.packageSection.match(/^description = "([^"]+)"$/m)?.[1];
  if (!description?.trim()) {
    throw new Error(crate + " must declare a non-empty public description");
  }
  const keywords = manifestArray(entry.packageSection, "keywords");
  if (
    !keywords ||
    keywords.length === 0 ||
    keywords.length > 5 ||
    keywords.some((keyword) => !/^[A-Za-z0-9][A-Za-z0-9_+-]{0,19}$/.test(keyword))
  ) {
    throw new Error(crate + " must declare valid crates.io keywords");
  }
  const categories = manifestArray(entry.packageSection, "categories");
  if (
    !categories ||
    categories.length === 0 ||
    categories.length > 5 ||
    categories.some((category) => !allowedCategories.has(category))
  ) {
    throw new Error(crate + " must declare approved crates.io categories");
  }
  if (/\bgit\s*=/.test(entry.manifest)) {
    throw new Error(crate + " contains a git dependency that crates.io would strip");
  }

  const [readme, license] = await Promise.all([
    readFile(join(entry.memberPath, "README.md"), "utf8"),
    readFile(join(entry.memberPath, "LICENSE"), "utf8"),
  ]);
  if (!readme.trim() || !license.trim()) {
    throw new Error(crate + " must package non-empty README.md and LICENSE files");
  }
  if (license !== rootLicense) {
    throw new Error(crate + " LICENSE must exactly match the repository LICENSE");
  }

  const packageFiles = await readdir(entry.memberPath, { recursive: true });
  const privateBinary = packageFiles.find(
    (file) => /^src\/bin\/.*quote[-_]?oracle/i.test(file) || /^src\/bin\/.*(?:private|internal)/i.test(file),
  );
  if (privateBinary) {
    throw new Error(crate + " contains private binary target " + privateBinary);
  }

  const workspaceReferences = [
    ...entry.manifest.matchAll(
      /^([A-Za-z0-9_-]+)(?:\.workspace\s*=\s*true|\s*=\s*\{[^}]*\bworkspace\s*=\s*true[^}]*\})$/gm,
    ),
  ]
    .map((match) => match[1])
    .filter((name) => !["version", "edition", "license", "rust-version", "repository"].includes(name));

  for (const dependency of workspaceReferences) {
    const escaped = dependency.replace(/[.*+?^$()|[\]\\{}]/g, "\\$&");
    const declaration = workspaceDependencies.match(new RegExp("^" + escaped + "\\s*=\\s*(.+)$", "m"))?.[1];
    const hasRegistryVersion =
      declaration && (/^"[^"]+"/.test(declaration) || /\bversion\s*=\s*"[^"]+"/.test(declaration));
    if (!hasRegistryVersion) {
      throw new Error(crate + " workspace dependency " + dependency + " has no registry version fallback");
    }
  }

  console.log(crate + ": publishable manifest validated");
}
