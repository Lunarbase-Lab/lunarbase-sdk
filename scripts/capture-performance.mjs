import { mkdirSync, readdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";

const settings = parseArguments(process.argv.slice(2));
const output = resolve(settings.output);
mkdirSync(output, { recursive: true });
if (readdirSync(output).length !== 0) fail(`output directory must be empty: ${output}`);

const cargo = process.env.CARGO || "cargo";
build([]);
for (const mode of ["timing", "mixed"]) {
  for (const lanes of [15, 64]) {
    for (const batch of [1, 16, 256]) {
      for (let run = 1; run <= settings.repetitions; run += 1) {
        capture(mode, lanes, batch, 128, run, settings.repetitions);
      }
    }
  }
}

build(["--features", "allocation-stats"]);
for (const lanes of [15, 64]) {
  for (const batch of [1, 16, 256]) {
    for (let run = 1; run <= settings.allocationRepetitions; run += 1) {
      capture("allocations", lanes, batch, 1, run, settings.allocationRepetitions);
    }
  }
}

function build(extra) {
  run(cargo, ["build", "--locked", "--release", "-p", "lunarbase-tools", "--bin", "lunarbase-quote-bench", ...extra]);
}

function capture(mode, lanes, batch, concurrency, runNumber, totalRuns) {
  const binary = resolve(
    process.env.CARGO_TARGET_DIR || "target",
    "release",
    process.platform === "win32" ? "lunarbase-quote-bench.exe" : "lunarbase-quote-bench",
  );
  const benchmarkArguments = [
    "--mode",
    mode,
    "--lanes",
    String(lanes),
    "--pairs",
    "100",
    "--batch-size",
    String(batch),
    "--concurrency",
    String(concurrency),
    "--warmup-calls",
    String(settings.warmupCalls),
  ];
  if (mode === "allocations") {
    benchmarkArguments.push("--allocation-calls", String(settings.allocationCalls));
  } else {
    benchmarkArguments.push(
      "--measured-quotes",
      String(settings.measuredQuotes),
      "--mixed-events-per-second",
      String(settings.mixedEventsPerSecond),
    );
  }
  const result = run(binary, benchmarkArguments, true);
  let report;
  try {
    report = JSON.parse(result.stdout);
  } catch (error) {
    fail(`benchmark returned invalid JSON: ${error.message}\n${result.stdout}`);
  }
  if (
    report.schemaVersion !== 1 ||
    report.mode !== mode ||
    report.buildProfile !== "release" ||
    report.scenarioId !== `${mode}-lanes${lanes}-pairs100-batch${batch}-c${concurrency}`
  ) {
    fail(`benchmark report identity mismatch: ${JSON.stringify(report)}`);
  }
  if (
    mode === "mixed" &&
    (!(report.eventLoad?.publishedUpdates > 0) || report.eventLoad.appliedUpdates !== report.eventLoad.publishedUpdates)
  ) {
    fail(`mixed scenario ${report.scenarioId} did not apply every published update`);
  }
  const suffix = String(runNumber).padStart(String(totalRuns).length, "0");
  writeFileSync(resolve(output, `${report.scenarioId}-run${suffix}.json`), `${JSON.stringify(report, null, 2)}\n`, {
    flag: "wx",
  });
}

function run(command, commandArguments, captureOutput = false) {
  const result = spawnSync(command, commandArguments, {
    cwd: process.cwd(),
    encoding: "utf8",
    stdio: captureOutput ? ["ignore", "pipe", "pipe"] : "inherit",
    maxBuffer: 8 * 1024 * 1024,
  });
  if (result.error) fail(`${command} failed to start: ${result.error.message}`);
  if (result.status !== 0) {
    fail(`${command} exited with ${result.status}: ${result.stderr || ""}`);
  }
  return result;
}

function parseArguments(arguments_) {
  const values = new Map();
  for (let index = 0; index < arguments_.length; index += 2) {
    const key = arguments_[index];
    const value = arguments_[index + 1];
    if (!key?.startsWith("--") || value === undefined) fail("arguments must be --name value pairs");
    values.set(key.slice(2), value);
  }
  const outputValue = values.get("output");
  if (!outputValue) fail("--output is required");
  return {
    output: outputValue,
    repetitions: positiveInteger(values.get("repetitions") || process.env.PERF_REPETITIONS || "10", "repetitions"),
    allocationRepetitions: positiveInteger(
      values.get("allocation-repetitions") || process.env.PERF_ALLOCATION_REPETITIONS || "1",
      "allocation-repetitions",
    ),
    measuredQuotes: positiveInteger(process.env.MEASURED_QUOTES || "262144", "MEASURED_QUOTES"),
    allocationCalls: positiveInteger(process.env.ALLOCATION_CALLS || "4096", "ALLOCATION_CALLS"),
    warmupCalls: positiveInteger(process.env.WARMUP_CALLS || "1024", "WARMUP_CALLS"),
    mixedEventsPerSecond: positiveInteger(process.env.MIXED_EVENTS_PER_SECOND || "1000", "MIXED_EVENTS_PER_SECOND"),
  };
}

function positiveInteger(value, name) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) fail(`${name} must be a positive safe integer`);
  return parsed;
}

function fail(message) {
  console.error(`capture-performance: ${message}`);
  process.exit(1);
}
