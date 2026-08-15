import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";

const checker = resolve("scripts/check-performance-regression.mjs");

test("release publication depends on a dedicated immutable-baseline performance gate", () => {
  const workflow = readFileSync(resolve(".github/workflows/release.yml"), "utf8");
  const performanceStart = workflow.indexOf("\n  performance:");
  const publishStart = workflow.indexOf("\n  publish:");
  assert.ok(performanceStart > 0 && publishStart > performanceStart);
  const performance = workflow.slice(performanceStart, publishStart);
  const publish = workflow.slice(publishStart);

  assert.match(performance, /runs-on: \[self-hosted, linux, x64, lunarbase-performance\]/);
  assert.match(performance, /LUNARBASE_PERFORMANCE_BASELINE_REF/);
  assert.match(performance, /\^\[0-9a-fA-F\]\{40\}\$/);
  assert.match(performance, /make performance-capture/g);
  assert.match(performance, /make performance-gate/);
  assert.match(publish, /needs: \[binaries, gate, performance\]/);
});

test("accepts stable timing, mixed-load, RSS, and allocation reports", () => {
  withReports(({ baseline, current }) => {
    writeTimingSeries(baseline, "timing", 1_000_000, 10_000, 64_000_000);
    writeTimingSeries(current, "timing", 980_000, 10_400, 66_000_000);
    writeTimingSeries(baseline, "mixed", 900_000, 12_000, 70_000_000);
    writeTimingSeries(current, "mixed", 880_000, 12_300, 72_000_000);
    writeAllocation(baseline, allocationReport());
    writeAllocation(current, allocationReport());

    const result = runChecker(baseline, current);

    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /Performance gate passed for 3 scenarios/);
  });
});

test("rejects a reproducible throughput and p99 regression", () => {
  withReports(({ baseline, current }) => {
    writeTimingSeries(baseline, "timing", 1_000_000, 10_000, 64_000_000);
    writeTimingSeries(current, "timing", 900_000, 11_000, 64_000_000);

    const result = runChecker(baseline, current);

    assert.equal(result.status, 1);
    assert.match(result.stderr, /throughput regressed 10\.00%/);
    assert.match(result.stderr, /p99 regressed 9\.99%/);
  });
});

test("rejects allocation growth and an idle mixed-load publisher", () => {
  withReports(({ baseline, current }) => {
    writeAllocation(baseline, allocationReport());
    writeAllocation(current, allocationReport({ countPerQuote: 1.01 }));
    writeTimingSeries(baseline, "mixed", 900_000, 12_000, 70_000_000);
    writeTimingSeries(current, "mixed", 900_000, 12_000, 70_000_000, 0);

    const result = runChecker(baseline, current);

    assert.equal(result.status, 1);
    assert.match(result.stderr, /allocation count\/quote regressed 1\.00%/);
    assert.match(result.stderr, /mixed run did not apply every published update/);
  });
});

function withReports(callback) {
  const root = mkdtempSync(resolve(tmpdir(), "lunarbase-performance-test-"));
  const baseline = resolve(root, "baseline");
  const current = resolve(root, "current");
  mkdirSync(baseline);
  mkdirSync(current);
  try {
    callback({ baseline, current });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

function writeTimingSeries(directory, mode, throughput, p99, rss, publishedUpdates = 100) {
  for (let run = 1; run <= 10; run += 1) {
    writeReport(
      directory,
      `${mode}-${run}.json`,
      timingReport(mode, throughput + run, p99 + run, rss + run, publishedUpdates),
    );
  }
}

function timingReport(mode, throughput, p99, rss, publishedUpdates) {
  return {
    ...identity(mode, 128),
    timing: {
      quotesPerSecond: throughput,
      latencyNs: { p99 },
    },
    allocations: null,
    eventLoad:
      mode === "mixed"
        ? {
            configuredEventsPerSecond: 1_000,
            publishedUpdates,
            appliedUpdates: publishedUpdates,
            durationNs: 1_000_000,
          }
        : null,
    rssBytes: { peak: rss },
  };
}

function writeAllocation(directory, report) {
  writeReport(directory, "allocations.json", report);
}

function allocationReport(overrides = {}) {
  return {
    ...identity("allocations", 1),
    timing: null,
    allocations: {
      countPerQuote: 1,
      bytesPerQuote: 64,
      countMax: 16,
      bytesMax: 1024,
      countCurrent: 0,
      bytesCurrent: 0,
      ...overrides,
    },
    eventLoad: null,
    rssBytes: { peak: 64_000_000 },
  };
}

function identity(mode, concurrency) {
  return {
    schemaVersion: 1,
    scenarioId: `${mode}-lanes15-pairs100-batch16-c${concurrency}`,
    mode,
    buildProfile: "release",
    target: "x86_64-linux",
    allocatorInstrumented: mode === "allocations",
    lanes: 15,
    pairs: 100,
    batchSize: 16,
    concurrency,
    warmupCalls: 1024,
    measuredQuotes: 262_144,
  };
}

function writeReport(directory, name, report) {
  writeFileSync(resolve(directory, name), `${JSON.stringify(report)}\n`);
}

function runChecker(baseline, current) {
  return spawnSync(process.execPath, [checker, "--baseline", baseline, "--current", current], {
    encoding: "utf8",
  });
}
