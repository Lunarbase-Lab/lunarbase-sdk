import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  assertCratesIoArtifact,
  assertNpmRegistryArtifact,
  assertTopologicalOrder,
  crateArchiveChecksum,
  npmTarballIntegrity,
} from "./release-integrity.mjs";

test("npm integrity accepts only the packed tarball", () => {
  const contents = Buffer.from("packed npm fixture");
  const integrity = "sha512-" + createHash("sha512").update(contents).digest("base64");
  assert.equal(npmTarballIntegrity(contents), integrity);
  assert.doesNotThrow(() =>
    assertNpmRegistryArtifact(
      { name: "@scope/pkg", version: "1.2.3", dist: { integrity } },
      { name: "@scope/pkg", version: "1.2.3", integrity },
    ),
  );
  assert.throws(
    () =>
      assertNpmRegistryArtifact(
        { name: "@scope/pkg", version: "1.2.3", dist: { integrity: "sha512-wrong" } },
        { name: "@scope/pkg", version: "1.2.3", integrity },
      ),
    /integrity mismatch/,
  );
});

test("crates.io integrity rejects checksum mismatches and yanked versions", () => {
  const contents = Buffer.from("packed crate fixture");
  const checksum = createHash("sha256").update(contents).digest("hex");
  assert.equal(crateArchiveChecksum(contents), checksum);
  const expected = { name: "crate-name", version: "1.2.3", checksum };
  assert.doesNotThrow(() =>
    assertCratesIoArtifact({ version: { crate: "crate-name", num: "1.2.3", checksum, yanked: false } }, expected),
  );
  assert.throws(
    () =>
      assertCratesIoArtifact(
        { version: { crate: "crate-name", num: "1.2.3", checksum: "0".repeat(64), yanked: false } },
        expected,
      ),
    /checksum mismatch/,
  );
  assert.throws(
    () => assertCratesIoArtifact({ version: { crate: "crate-name", num: "1.2.3", checksum, yanked: true } }, expected),
    /is yanked/,
  );
});

test("publish inventory must be topological", () => {
  const dependencies = new Map([
    ["math", new Set()],
    ["client", new Set(["math"])],
    ["source", new Set(["client", "math"])],
  ]);
  assert.doesNotThrow(() => assertTopologicalOrder(["math", "client", "source"], dependencies, "fixture"));
  assert.throws(() => assertTopologicalOrder(["client", "math", "source"], dependencies, "fixture"), /not topological/);
  assert.throws(() => assertTopologicalOrder(["math", "math"], dependencies, "fixture"), /duplicate/);
});
