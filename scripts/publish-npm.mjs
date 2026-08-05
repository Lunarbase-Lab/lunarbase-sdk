import { execFileSync } from "node:child_process";
import { readFile, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import { assertNpmRegistryArtifact, npmTarballIntegrity } from "./release-integrity.mjs";
import { releaseNpmPackages } from "./release-packages.mjs";

const releaseTag = process.env.RELEASE_TAG;
if (!releaseTag?.startsWith("v")) {
  throw new Error("RELEASE_TAG must be a v-prefixed SemVer tag");
}
const version = releaseTag.slice(1);
const distTag = version.includes("-") ? "next" : "latest";
const dist = resolve(process.cwd(), "dist");

const npmVersion = execFileSync("npm", ["--version"], { encoding: "utf8" }).trim();
const [npmMajor, npmMinor, npmPatch] = npmVersion.split(".").map(Number);
if (
  !Number.isInteger(npmMajor) ||
  npmMajor < 11 ||
  (npmMajor === 11 && npmMinor < 5) ||
  (npmMajor === 11 && npmMinor === 5 && npmPatch < 1)
) {
  throw new Error("npm >= 11.5.1 is required, found " + npmVersion);
}

const registryVersion = async (name) => {
  const url = "https://registry.npmjs.org/" + encodeURIComponent(name) + "/" + encodeURIComponent(version);
  for (let attempt = 0; attempt < 6; attempt += 1) {
    let response;
    try {
      response = await fetch(url, {
        headers: { accept: "application/json", "user-agent": "lunarbase-release" },
      });
    } catch (error) {
      if (attempt === 5) throw error;
      await delay((attempt + 1) * 2000);
      continue;
    }
    if (response.status === 200) return await response.json();
    if (response.status === 404) return undefined;
    if (response.status !== 429 && response.status < 500) {
      throw new Error("npm registry returned HTTP " + response.status + " for " + name + "@" + version);
    }
    await delay((attempt + 1) * 2000);
  }
  throw new Error("npm registry remained unavailable for " + name + "@" + version);
};

const ensureDistTag = (name) => {
  execFileSync("npm", ["dist-tag", "add", name + "@" + version, distTag], {
    env: process.env,
    stdio: "inherit",
  });
};

const archives = (await readdir(dist)).filter((entry) => entry.endsWith(".tgz")).map((entry) => join(dist, entry));
if (archives.length !== releaseNpmPackages.length) {
  throw new Error("expected exactly " + releaseNpmPackages.length + " packed npm archives, found " + archives.length);
}

const archiveByPackage = new Map();
for (const archive of archives) {
  const manifest = JSON.parse(execFileSync("tar", ["-xOf", archive, "package/package.json"], { encoding: "utf8" }));
  if (manifest.version !== version || !releaseNpmPackages.some(({ name }) => name === manifest.name)) {
    throw new Error("unexpected packed archive " + archive + " for " + manifest.name + "@" + manifest.version);
  }
  if (archiveByPackage.has(manifest.name)) {
    throw new Error("duplicate packed archive for " + manifest.name + "@" + version);
  }
  archiveByPackage.set(manifest.name, {
    archive,
    integrity: npmTarballIntegrity(await readFile(archive)),
  });
}

for (const { name } of releaseNpmPackages) {
  const local = archiveByPackage.get(name);
  if (!local) throw new Error("packed archive not found for " + name + "@" + version);

  const existing = await registryVersion(name);
  if (existing) {
    assertNpmRegistryArtifact(existing, { name, version, integrity: local.integrity });
    console.log(name + "@" + version + " already exists; restoring dist-tag");
    ensureDistTag(name);
    continue;
  }

  try {
    execFileSync("npm", ["publish", local.archive, "--access", "public", "--tag", distTag], {
      env: process.env,
      stdio: "inherit",
    });
  } catch (error) {
    const appeared = await registryVersion(name);
    if (appeared) {
      assertNpmRegistryArtifact(appeared, { name, version, integrity: local.integrity });
      console.log(name + "@" + version + " appeared after an ambiguous publish error");
      ensureDistTag(name);
      continue;
    }
    throw error;
  }

  let visible;
  for (let attempt = 0; attempt < 12; attempt += 1) {
    visible = await registryVersion(name);
    if (visible) {
      assertNpmRegistryArtifact(visible, { name, version, integrity: local.integrity });
      break;
    }
    await delay(5000);
  }
  if (!visible) throw new Error(name + "@" + version + " was published but is not visible");
  ensureDistTag(name);
}
