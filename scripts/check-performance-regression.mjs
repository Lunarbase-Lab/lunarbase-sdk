import { readFileSync, readdirSync, statSync } from "node:fs";
import { resolve } from "node:path";

const settings = parseArguments(process.argv.slice(2));
const baseline = loadReports(settings.baseline);
const current = loadReports(settings.current);
const failures = [];
const summaries = [];

assertSameScenarios(baseline, current);
for (const scenario of [...baseline.keys()].sort()) {
  const before = baseline.get(scenario);
  const after = current.get(scenario);
  const mode = before[0].mode;
  const minimum = mode === "allocations" ? 1 : settings.minimumSamples;
  if (before.length < minimum || after.length < minimum) {
    failures.push(`${scenario}: requires at least ${minimum} samples in both sets`);
    continue;
  }
  validateIdentity(scenario, before, after);
  if (mode === "allocations") compareAllocations(scenario, before, after);
  else compareTiming(scenario, before, after);
}

for (const summary of summaries) console.log(summary);
if (failures.length > 0) {
  for (const failure of failures) console.error(`performance regression: ${failure}`);
  process.exit(1);
}
console.log(`Performance gate passed for ${baseline.size} scenarios.`);

function compareTiming(scenario, before, after) {
  const throughputBefore = median(
    before.map((report) => required(report.timing?.quotesPerSecond, scenario, "throughput")),
  );
  const throughputAfter = median(
    after.map((report) => required(report.timing?.quotesPerSecond, scenario, "throughput")),
  );
  const p99Before = median(before.map((report) => required(report.timing?.latencyNs?.p99, scenario, "p99")));
  const p99After = median(after.map((report) => required(report.timing?.latencyNs?.p99, scenario, "p99")));
  const rssBefore = median(before.map((report) => required(report.rssBytes?.peak, scenario, "peak RSS")));
  const rssAfter = median(after.map((report) => required(report.rssBytes?.peak, scenario, "peak RSS")));
  const throughputRegression = lowerRegression(throughputBefore, throughputAfter);
  const p99Regression = higherRegression(p99Before, p99After);
  const rssRegression = higherRegression(rssBefore, rssAfter);
  summaries.push(
    `${scenario}: throughput ${percent(throughputRegression)}, p99 ${percent(p99Regression)}, RSS ${percent(rssRegression)}`,
  );
  checkLimit(scenario, "throughput", throughputRegression, settings.throughputRegressionPercent);
  checkLimit(scenario, "p99", p99Regression, settings.p99RegressionPercent);
  checkLimit(scenario, "peak RSS", rssRegression, settings.rssRegressionPercent);
}

function compareAllocations(scenario, before, after) {
  for (const [label, field] of [
    ["allocation count/quote", "countPerQuote"],
    ["allocation bytes/quote", "bytesPerQuote"],
    ["peak allocation count", "countMax"],
    ["peak allocation bytes", "bytesMax"],
    ["live allocation count", "countCurrent"],
    ["live allocation bytes", "bytesCurrent"],
  ]) {
    const baselineValue = median(before.map((report) => required(report.allocations?.[field], scenario, label)));
    const currentValue = median(after.map((report) => required(report.allocations?.[field], scenario, label)));
    const regression = higherRegression(baselineValue, currentValue);
    summaries.push(`${scenario}: ${label} ${percent(regression)}`);
    checkLimit(scenario, label, regression, settings.allocationRegressionPercent);
  }
}

function validateIdentity(scenario, before, after) {
  const reference = identity(before[0]);
  for (const report of [...before, ...after]) {
    if (identity(report) !== reference) failures.push(`${scenario}: benchmark configuration or target changed`);
    if (report.buildProfile !== "release") failures.push(`${scenario}: report is not from a release build`);
    if (
      report.mode === "mixed" &&
      (!(report.eventLoad?.publishedUpdates > 0) ||
        report.eventLoad.appliedUpdates !== report.eventLoad.publishedUpdates)
    )
      failures.push(`${scenario}: mixed run did not apply every published update`);
  }
}

function identity(report) {
  return JSON.stringify({
    schemaVersion: report.schemaVersion,
    scenarioId: report.scenarioId,
    mode: report.mode,
    target: report.target,
    allocatorInstrumented: report.allocatorInstrumented,
    lanes: report.lanes,
    pairs: report.pairs,
    batchSize: report.batchSize,
    concurrency: report.concurrency,
    warmupCalls: report.warmupCalls,
    measuredQuotes: report.measuredQuotes,
    mixedEventsPerSecond: report.eventLoad?.configuredEventsPerSecond,
  });
}

function loadReports(path) {
  const grouped = new Map();
  for (const file of jsonFiles(resolve(path))) {
    const report = JSON.parse(readFileSync(file, "utf8"));
    if (report.schemaVersion !== 1 || typeof report.scenarioId !== "string") fail(`invalid benchmark report: ${file}`);
    const reports = grouped.get(report.scenarioId) || [];
    reports.push(report);
    grouped.set(report.scenarioId, reports);
  }
  if (grouped.size === 0) fail(`no JSON reports found under ${path}`);
  return grouped;
}

function jsonFiles(path) {
  if (statSync(path).isFile()) return path.endsWith(".json") ? [path] : [];
  return readdirSync(path, { withFileTypes: true }).flatMap((entry) => jsonFiles(resolve(path, entry.name)));
}

function assertSameScenarios(before, after) {
  const left = [...before.keys()].sort();
  const right = [...after.keys()].sort();
  if (JSON.stringify(left) !== JSON.stringify(right))
    fail(`scenario sets differ: baseline=${left.join(",")} current=${right.join(",")}`);
}

function checkLimit(scenario, metric, regression, limit) {
  if (regression > limit)
    failures.push(`${scenario}: ${metric} regressed ${percent(regression)} (limit ${limit.toFixed(2)}%)`);
}

function lowerRegression(before, after) {
  if (before === 0) return after < before ? Number.POSITIVE_INFINITY : 0;
  return ((before - after) / before) * 100;
}

function higherRegression(before, after) {
  if (before === 0) return after > 0 ? Number.POSITIVE_INFINITY : 0;
  return ((after - before) / Math.abs(before)) * 100;
}

function median(values) {
  const sorted = values.toSorted((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
}

function required(value, scenario, label) {
  if (typeof value !== "number" || !Number.isFinite(value)) fail(`${scenario}: missing finite ${label}`);
  return value;
}

function percent(value) {
  return Number.isFinite(value) ? `${value.toFixed(2)}%` : "infinite";
}

function parseArguments(arguments_) {
  const values = new Map();
  for (let index = 0; index < arguments_.length; index += 2) {
    const key = arguments_[index];
    const value = arguments_[index + 1];
    if (!key?.startsWith("--") || value === undefined) fail("arguments must be --name value pairs");
    values.set(key.slice(2), value);
  }
  const baselineValue = values.get("baseline");
  const currentValue = values.get("current");
  if (!baselineValue || !currentValue) fail("--baseline and --current are required");
  return {
    baseline: baselineValue,
    current: currentValue,
    minimumSamples: positiveNumber(values.get("minimum-samples") || "10", "minimum-samples", true),
    throughputRegressionPercent: positiveNumber(
      values.get("throughput-regression-percent") || "3",
      "throughput-regression-percent",
    ),
    p99RegressionPercent: positiveNumber(values.get("p99-regression-percent") || "5", "p99-regression-percent"),
    rssRegressionPercent: positiveNumber(values.get("rss-regression-percent") || "5", "rss-regression-percent"),
    allocationRegressionPercent: positiveNumber(
      values.get("allocation-regression-percent") || "0",
      "allocation-regression-percent",
    ),
  };
}

function positiveNumber(value, name, integer = false) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0 || (integer && !Number.isSafeInteger(parsed)))
    fail(`${name} must be a non-negative ${integer ? "safe integer" : "number"}`);
  return parsed;
}

function fail(message) {
  console.error(`check-performance-regression: ${message}`);
  process.exit(1);
}
